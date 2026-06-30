// Package coverer is the boot logic of the sbw-coverer — the SHARDED sensor/actuator
// half of the server↔coverer split (DESIGN-server-coverer-split §8 step4). It fuses the
// GoBGP RIB-tap + RIB-survival guard with the agent-facing AgentService transport, and
// bridges them to the sbw-server over rpc.ServerCoverer: it WATCHES the server for its
// COVERAGE assignment (drives the tap to (de)tap) and per-edge desired-state (relays to
// agents), and REPORTS up what only its tap can see (hard-death votes, member→edge
// locality, relayed agent reports). It owns NO store — no etcd, no YugabyteDB — by design.
//
// This package is the adapted COVERER-HALF of the monolith controller pkg: the split
// tap.go (taphandler.go), the Watch consumer turned gRPC client (watchclient.go), and the
// Report client-stream (reportclient.go).
package coverer

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/netip"
	"slices"
	"sync"

	"github.com/fivetime/sbw-contract/metrics"
	"github.com/fivetime/sbw-contract/model"
	"github.com/fivetime/sbw-contract/rpc"

	"github.com/fivetime/sbw-coverer/internal/grpcsrv"
	"github.com/fivetime/sbw-coverer/internal/guard"
	"github.com/fivetime/sbw-coverer/internal/ribtap"
)

// Coverer holds the coverer-half state: the guard + tap (the sensor), the agent-facing
// grpcsrv (the actuator to L/R agents), the ServerCoverer client (Watch/Report/Register),
// and the canary memory (coverer-local so an attribute-less canary withdrawal is still
// recognised).
type Coverer struct {
	self   string                  // = WatchRequest.CovererId; stamped on every Report
	client rpc.ServerCovererClient // the sbw-server (Watch downlink / Register relay)
	guard  *guard.Guard            // RIB-survival /32 mirror (built from the lossy tap)
	srv    *ribtap.Server          // the embedded GoBGP tap
	sink   *ribtap.CoverageSink    // applyCoverage drives this to (de)tap covered edges
	rc     *reportClient           // the long-lived Report client-stream (stamps CovererId)
	agents *grpcsrv.Server         // the agent-facing AgentService transport
	met    *metrics.Metrics
	log    *slog.Logger

	// canary memory (coverer-local). A BGP withdrawal carries no attributes, so the
	// canary's attribute-less withdrawal is recognised by the prefix remembered when it
	// was advertised, not by its large community.
	canaryLC   model.LargeCommunity
	hasCanary  bool
	canaryMu   sync.Mutex
	canarySeen map[canaryKey]struct{}
}

// New builds the Coverer and its agent-facing grpcsrv. The grpcsrv routes the WHOLE
// registration up via SetRegisterFull(client.Register) and relays each agent EdgeReport
// up as an AGENT_REPORT CovererReport through the single reportClient.Send chokepoint.
func New(
	self string,
	client rpc.ServerCovererClient,
	g *guard.Guard,
	srv *ribtap.Server,
	sink *ribtap.CoverageSink,
	rc *reportClient,
	met *metrics.Metrics,
	canaryLC model.LargeCommunity,
	log *slog.Logger,
) *Coverer {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	c := &Coverer{
		self:       self,
		client:     client,
		guard:      g,
		srv:        srv,
		sink:       sink,
		rc:         rc,
		met:        met,
		log:        log,
		canaryLC:   canaryLC,
		hasCanary:  canaryLC != (model.LargeCommunity{}),
		canarySeen: make(map[canaryKey]struct{}),
	}

	// The agent-facing AgentService. WithReport turns the in-proc relayReport into an
	// AGENT_REPORT send; SetRegisterFull relays the agent's RegisterRequest straight to
	// the server (unary). onSubscribe/onResync are best-effort (see their godoc).
	c.agents = grpcsrv.New(
		grpcsrv.WithLogger(log),
		grpcsrv.WithReport(func(ctx context.Context, er model.EdgeReport) error {
			payload, err := json.Marshal(er)
			if err != nil {
				return err
			}
			return c.rc.Send(&rpc.CovererReport{
				Kind:       rpc.CovererReport_AGENT_REPORT,
				EdgeId:     string(er.EdgeID),
				Generation: er.Generation,
				Payload:    payload,
			})
		}),
		grpcsrv.WithOnSubscribe(c.onSubscribe),
		grpcsrv.WithOnResync(c.onResync),
	)
	c.agents.SetRegisterFull(func(ctx context.Context, req *rpc.RegisterRequest) (*rpc.RegisterResponse, error) {
		// Verbatim unary relay to the server — it does the authoritative register +
		// coverer assignment and returns the reply the agent gets back.
		return c.client.Register(ctx, req)
	})
	return c
}

// Agents exposes the agent-facing AgentService server so cmd can register it on a gRPC
// listener (rpc.RegisterAgentServiceServer).
func (c *Coverer) Agents() *grpcsrv.Server { return c.agents }

// onSubscribe is the agent on-connect hook. In the monolith this rendered+pushed the
// agent's current desired state; the split coverer cannot render (no store, no
// orchestrator) and the ServerCoverer contract has no per-edge "resync this edge now"
// RPC. So it is BEST-EFFORT: the reconnecting agent recovers its desired-state via the
// server's periodic Watch re-render / drift sweep + the agent's own generation-gap
// detection. (Convergence is slower than the monolith — a known gap, see DESIGN risks.)
func (c *Coverer) onSubscribe(edge model.EdgeID) {
	c.log.Debug("agent subscribed; relying on server periodic re-render for initial sync", "edge", edge)
}

// onResync is the dropped-delta resync hook. Same constraint as onSubscribe: the coverer
// cannot render, so it cannot satisfy the resync locally. Best-effort log; the server's
// periodic re-render + the agent's generation-gap detection recover the lost deltas.
func (c *Coverer) onResync(edge model.EdgeID) {
	c.log.Debug("delta dropped under overflow; relying on server re-render for resync", "edge", edge)
}

// reportDeathVote routes a tap death/revive signal up as a DEATH_VOTE CovererReport.
// down=true is a hard death (PeerDown), down=false clears it (PeerUp / canary-up).
//
// NOTE(#6, soft canary): the CovererReport proto carries only `down bool` — no soft/hard
// discriminator — and the server-half Report handler maps DEATH_VOTE strictly to
// HardDown/HardUp. So the SOFT canary signal cannot be distinguished from a hard PeerDown
// over the current contract. Until a `soft` bit (or a CANARY kind) is added to the proto
// AND a server-side CanaryDown/CanaryUp path exists, the canary path is reported through
// this same DEATH_VOTE (see taphandler.go) — a known regression flagged in the DESIGN
// risks, NOT a silent drop.
// reportDeathVote sends a DEATH_VOTE. soft=false is a HARD session vote (PeerDown/Up →
// the server's FailoverQuorum); soft=true is a SOFT canary signal (canary route
// withdrawn/restored → the server's CanaryDown/CanaryUp, which only fails over together
// with an agent data-plane-death report). CovererId is stamped centrally by rc.Send.
func (c *Coverer) reportDeathVote(edge model.EdgeID, down, soft bool) {
	if err := c.rc.Send(&rpc.CovererReport{
		Kind:   rpc.CovererReport_DEATH_VOTE,
		EdgeId: string(edge),
		Down:   down,
		Soft:   soft,
	}); err != nil {
		c.log.Warn("report death vote failed", "edge", edge, "down", down, "soft", soft, "err", err)
	}
}

// memberEdgeChunk bounds the member prefixes per MEMBER_EDGE report so a multi-million-
// member edge's EOR/drift full-snapshot does not blow the gRPC message cap. Mirrors
// grpcsrv's desired-state chunking granularity.
const memberEdgeChunk = 50_000

// reportMemberEdge routes a member→edge observation up as a MEMBER_EDGE CovererReport
// (the locality + member-liveness uplink). down carries the guard's verdict (the
// route-withdrawal veto the server consumes for render suppression). A large member set
// is split into bounded batches to stay under the gRPC cap.
func (c *Coverer) reportMemberEdge(edge model.EdgeID, members []netip.Prefix, down bool) {
	if len(members) == 0 {
		if err := c.rc.Send(&rpc.CovererReport{
			Kind:   rpc.CovererReport_MEMBER_EDGE,
			EdgeId: string(edge),
			Down:   down,
		}); err != nil {
			c.log.Warn("report member-edge failed", "edge", edge, "err", err)
		}
		return
	}
	for start := 0; start < len(members); start += memberEdgeChunk {
		end := min(start+memberEdgeChunk, len(members))
		if err := c.rc.Send(&rpc.CovererReport{
			Kind:    rpc.CovererReport_MEMBER_EDGE,
			EdgeId:  string(edge),
			Members: cidrs(members[start:end]),
			Down:    down,
		}); err != nil {
			c.log.Warn("report member-edge failed", "edge", edge, "members", end-start, "err", err)
		}
	}
}

// cidrs renders prefixes as CIDR strings (the MEMBER_EDGE wire form).
func cidrs(ps []netip.Prefix) []string {
	out := make([]string, len(ps))
	for i, p := range ps {
		out[i] = p.String()
	}
	return out
}

// viewReplayed reports whether the tap's initial replay for (edge, family) has completed
// — the guard's view is VALID (peerUp ∧ canaryUp ∧ EOR seen). Until then the per-host
// member-edge fusion is suppressed so a reconnect's adj-in replay does not flood the
// uplink; the post-EOR reconcile applies the rebuilt view once. No guard wired → always
// true (legacy).
func (c *Coverer) viewReplayed(edge model.EdgeID, family model.Family) bool {
	return c.guard == nil || c.guard.ViewValid(edge, family)
}

// isCanary reports whether the event carries the configured canary large community.
func (c *Coverer) isCanary(lcs []model.LargeCommunity) bool {
	if !c.hasCanary {
		return false
	}
	return slices.Contains(lcs, c.canaryLC)
}

// canaryKey identifies a canary by the edge that advertised it and its prefix.
type canaryKey struct {
	edge   model.EdgeID
	prefix netip.Prefix
}

// rememberCanary records that (edge, prefix) was advertised as a canary, so its later
// attribute-less withdrawal is still recognised as a canary signal.
func (c *Coverer) rememberCanary(edge model.EdgeID, prefix netip.Prefix) {
	c.canaryMu.Lock()
	c.canarySeen[canaryKey{edge, prefix}] = struct{}{}
	c.canaryMu.Unlock()
}

// forgetCanary reports whether (edge, prefix) was a remembered canary, removing it. True
// means this withdrawal is a canary withdrawal.
func (c *Coverer) forgetCanary(edge model.EdgeID, prefix netip.Prefix) bool {
	c.canaryMu.Lock()
	defer c.canaryMu.Unlock()
	k := canaryKey{edge, prefix}
	if _, ok := c.canarySeen[k]; ok {
		delete(c.canarySeen, k)
		return true
	}
	return false
}
