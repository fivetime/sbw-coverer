package ribtap

import (
	"context"
	"fmt"
	"log/slog"
	"net/netip"
	"sync"

	api "github.com/osrg/gobgp/v4/api"
	"github.com/osrg/gobgp/v4/pkg/server"

	"github.com/fivetime/sbw-contract/model"
)

// api.Global.Families uses GoBGP's AfiSafiType index encoding (pkg/config/oc
// IntToAfiSafiTypeMap), NOT the bgp.Family afi<<16|safi value: 0 = IPv4 unicast,
// 1 = IPv6 unicast. We pin the global RIB to exactly these two so it doesn't
// allocate tables for families we never tap.
const (
	familyIPv4Unicast uint32 = 0
	familyIPv6Unicast uint32 = 1
)

// Config configures the embedded GoBGP server (DESIGN.md §6.2).
type Config struct {
	ASN        uint32   // controller AS — 65010; the tap is iBGP (peers share it)
	RouterID   string   // must be a valid IP literal (GoBGP requires it)
	ListenPort int32    // 1790 in prod; the port BIRDs connect in to
	ListenAddr []string // optional bind addresses (default: all)

	// BFD on every tap session (T-903): when enabled, each peer runs BFD so an
	// edge death is detected sub-second — PeerDown fires fast (→ HardDown →
	// immediate failover) instead of waiting the BGP hold timer (9s in the lab,
	// up to 180s on GoBGP defaults). Detection time ≈ max(tx,rx)×multiplier.
	// The edge BIRD must mirror this with `bfd on` on its tap protocol. Zero
	// intervals/multiplier fall back to 300ms × 3 (~0.9s).
	BFDEnabled    bool
	BFDTxMs       uint32 // desired min TX interval (ms); 0 → 300
	BFDRxMs       uint32 // required min RX interval (ms); 0 → 300
	BFDMultiplier uint32 // detection multiplier; 0 → 3

	// BFDMultihop runs the tap's BFD as MULTIHOP (RFC 5883, UDP 4784) instead of
	// single-hop (RFC 5881, UDP 3784), so the controller can sit any number of
	// hops from the edges (TODO-liveness L-02): each peer's BFD targets port 4784
	// and the BGP session is configured eBGP-multihop. Pairs with the edge BIRD's
	// `to_tap` carrying `multihop`. Requires the multihop-capable GoBGP (L-01,
	// dual-listen 3784+4784). No effect unless BFDEnabled.
	BFDMultihop bool

	// BindInterface pins the tap session's BGP + BFD sockets to a named interface via
	// SO_BINDTODEVICE (GoBGP api.Transport.BindInterface → oc.Transport.Config.BindInterface
	// → bfdServer.AddPeer). REQUIRED for MULTIHOP BFD on a MULTI-HOMED coverer: the coverer
	// has flannel eth0 (default route) + the ctrl-tap interface (the BGP source), and without
	// binding, GoBGP's BFD UDP(4784) egresses via the default route (eth0), never reaching the
	// edge over ctrl-tap → BFD stuck Down → BFD-triggered BGP reset → tap flap → false
	// hard-quorum death. Set to the ctrl-tap interface name (lab: "ctap"). "" = no bind
	// (single-homed / legacy). No effect unless the peer runs BFD.
	BindInterface string

	// ActiveDial makes the controller INITIATE the tap session instead of waiting
	// for the edge BIRD to connect (TODO-liveness L-05). Under sharding, a
	// controller peers with exactly the edges it currently covers and drops them
	// (RemovePeer) when coverage moves — driving the session from the controller
	// side makes that a clean AddPeer/RemovePeer, and the edge BIRD's `to_tap`
	// becomes passive. Without it (the classic single-controller mode) the edge
	// dials in and the tap stays passive.
	ActiveDial bool
}

// bfdConfig builds the GoBGP BFD peer config from Config, applying the ~0.9s
// defaults for any zero field. Returns nil when BFD is disabled.
//
// UNITS: despite the proto doc-comment calling these fields "milliseconds",
// GoBGP v4.6.0 reads BfdPeerConfig.{Desired,Required}*Interval as MICROSECONDS
// (pkg/server/bfd_peer.go: `time.Duration(v) * time.Microsecond`). Our config is
// in ms (BFDTxMs/BFDRxMs), so we convert ×1000. Passing the ms value raw put
// ~300µs on the wire, which BIRD cannot negotiate — it stops transmitting (a
// peer advertising a sub-ms / zero RX interval tells the other side to cease,
// RFC 5880 §6.8.6), and the session deadlocks at Init. ×1000 puts a real 300ms
// on the wire so the session establishes.
func (c Config) bfdConfig() *api.BfdPeerConfig {
	if !c.BFDEnabled {
		return nil
	}
	tx, rx, mult := c.BFDTxMs, c.BFDRxMs, c.BFDMultiplier
	if tx == 0 {
		tx = 300
	}
	if rx == 0 {
		rx = 300
	}
	if mult == 0 {
		mult = 3
	}
	const msToUs = 1000
	cfg := &api.BfdPeerConfig{
		Enabled:                  true,
		DesiredMinimumTxInterval: tx * msToUs,
		RequiredMinimumReceive:   rx * msToUs,
		DetectionMultiplier:      mult,
	}
	if c.BFDMultihop {
		// Target the RFC 5883 multihop control port; GoBGP (L-01) listens on it too.
		cfg.Port = bfdMultihopPort
	}
	return cfg
}

// bfdMultihopPort is the RFC 5883 multihop BFD control port (vs 3784 single-hop).
const bfdMultihopPort uint32 = 4784

func (c Config) validate() error {
	if c.ASN == 0 {
		return fmt.Errorf("ribtap: config: asn required")
	}
	if _, err := netip.ParseAddr(c.RouterID); err != nil {
		return fmt.Errorf("ribtap: config: router id %q must be an IP literal: %w", c.RouterID, err)
	}
	if c.ListenPort <= 0 {
		// GoBGP treats -1 as "don't listen"; the controller must accept inbound
		// from the edges, so a real positive port is mandatory.
		return fmt.Errorf("ribtap: config: listen_port must be > 0, got %d", c.ListenPort)
	}
	return nil
}

// Peer is one edge's BIRD tap session.
type Peer struct {
	Edge            model.EdgeID // logical edge id (the T-611 adapter maps peer → edge)
	NeighborAddress string       // the edge BIRD's source address
	PeerASN         uint32       // defaults to the controller ASN (iBGP) when 0
}

// Server is the embedded GoBGP RIB tap. It only ever RECEIVES routes from the
// edges — every peer is export-default-REJECT (§1.1-6) and passive — so it can
// never re-inject into the fabric. T-611 builds the RouteEvent producer over
// BGP().
type Server struct {
	bgp *server.BgpServer
	log *slog.Logger
	cfg Config

	mu    sync.RWMutex
	peers map[netip.Addr]model.EdgeID // neighbor address → edge, for the producer
}

// NewServer builds the embedded server. It does not start it; call Start.
func NewServer(cfg Config, log *slog.Logger) (*Server, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	bgp := server.NewBgpServer(server.LoggerOption(log, new(slog.LevelVar)))
	return &Server{bgp: bgp, log: log, cfg: cfg, peers: map[netip.Addr]model.EdgeID{}}, nil
}

// registerEdge records the neighbor address → edge mapping the producer uses to
// stamp RouteEvents (T-611). Called by AddPeer; exposed unexported for tests.
func (s *Server) registerEdge(addr netip.Addr, edge model.EdgeID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.peers[addr] = edge
}

// EdgeFor resolves the edge a peer address belongs to. The producer uses it to
// label every RouteEvent with its originating edge.
func (s *Server) EdgeFor(addr netip.Addr) (model.EdgeID, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.peers[addr]
	return e, ok
}

// Start runs the server loop and brings BGP up (no gRPC — embedded only, §6.2).
func (s *Server) Start(ctx context.Context) error {
	go s.bgp.Serve()
	req := &api.StartBgpRequest{Global: &api.Global{
		Asn:             s.cfg.ASN,
		RouterId:        s.cfg.RouterID,
		ListenPort:      s.cfg.ListenPort,
		ListenAddresses: s.cfg.ListenAddr,
		Families:        []uint32{familyIPv4Unicast, familyIPv6Unicast},
	}}
	if err := s.bgp.StartBgp(ctx, req); err != nil {
		return fmt.Errorf("ribtap: start bgp: %w", err)
	}
	s.log.Info("RIB tap started", "asn", s.cfg.ASN, "router_id", s.cfg.RouterID, "listen_port", s.cfg.ListenPort)
	return nil
}

// AddPeer configures one edge tap session. The peer is:
//   - passive by default — the edge BIRD initiates; the controller never dials
//     out — unless Config.ActiveDial (sharding, L-05) flips it to controller-initiated;
//   - export default REJECT — it must never advertise anything back (§1.1-6),
//     belt-and-suspenders on top of never injecting into the RIB;
//   - AddPath-receive on v4 and v6 — so multiple paths for the same prefix
//     reach the guard (the §6.3-6 uniqueness check needs them), paired with the
//     BIRD tap's `add paths tx`.
func (s *Server) AddPeer(ctx context.Context, p Peer) error {
	if p.NeighborAddress == "" {
		return fmt.Errorf("ribtap: add peer: neighbor address required")
	}
	asn := p.PeerASN
	if asn == 0 {
		asn = s.cfg.ASN // iBGP tap
	}
	transport := &api.Transport{PassiveMode: !s.cfg.ActiveDial, BindInterface: s.cfg.BindInterface}
	// Active-dial (sharding, L-05): source the outbound session from this replica's
	// RouterID so co-located replicas (K=2 on one test host sharing a segment) each
	// dial the edge from a distinct address instead of both from the interface's
	// primary — otherwise the edge sees same-source sessions that collide. In
	// production replicas live on different hosts, but binding the source is correct
	// regardless (the tap's identity is the replica).
	if s.cfg.ActiveDial && s.cfg.RouterID != "" {
		transport.LocalAddress = s.cfg.RouterID
	}
	peer := &api.Peer{
		Conf: &api.PeerConf{
			NeighborAddress: p.NeighborAddress,
			PeerAsn:         asn,
			LocalAsn:        s.cfg.ASN,
		},
		Transport: transport,
		// Graceful restart (+ long-lived, §4.3): pairs with the BIRD tap's
		// `long lived graceful restart on` so a session flap keeps stale routes
		// instead of a withdrawal storm, and — load-bearing for the guard — the
		// edge sends an explicit End-of-RIB after its initial dump, which the
		// guard's EOR gating (§6.3-4 / T-604) needs to trust "absence".
		//
		// RestartTime is the HELPER retention window — but it does NOT delay HARD
		// death. A real NODE death is detected by BFD, and GoBGP's BFD-down takes the
		// hard-reset path (bfd_peer resetPeer → ResetPeer Soft=false → CEASE/HARD_RESET
		// notification → fsm.go: the notification case returns BGP_FSM_IDLE BEFORE the
		// reasonCh GR trigger) which BYPASSES GR entirely and drops the routes at once.
		// GR-helper is only entered on notification-recv / hold-timer-expired / a TCP
		// read-write failure (e.g. a clean RST from a graceful bird restart). So keep
		// RestartTime LONG (120s): a legitimate graceful bird restart keeps its routes
		// (guard fail-static) for the full window, while a true node death still fails
		// over fast via BFD. (An earlier 25s here was based on a wrong "GR masks BFD"
		// assumption — reverted after reading the GoBGP FSM; RFC 5882/8538 compliant.)
		GracefulRestart: &api.GracefulRestart{
			Enabled:          true,
			RestartTime:      120,
			LonglivedEnabled: true,
		},
		AfiSafis: []*api.AfiSafi{
			recvAddPath(api.Family_AFI_IP),
			recvAddPath(api.Family_AFI_IP6),
		},
		ApplyPolicy: &api.ApplyPolicy{
			ExportPolicy: &api.PolicyAssignment{
				Direction:     api.PolicyDirection_POLICY_DIRECTION_EXPORT,
				DefaultAction: api.RouteAction_ROUTE_ACTION_REJECT,
			},
		},
		// BFD (T-903): sub-second liveness on the tap so a dead edge's PeerDown
		// fires fast. nil when disabled — then detection falls back to the BGP
		// hold timer. The edge BIRD must run `bfd on` on its tap protocol to match.
		Bfd: s.cfg.bfdConfig(),
	}
	if s.cfg.BFDMultihop {
		// A multihop controller's eBGP tap session must allow >1 hop (TTL), to
		// match the edge BIRD's `multihop`; pairs with multihop BFD on UDP 4784.
		peer.EbgpMultihop = &api.EbgpMultihop{Enabled: true, MultihopTtl: 64}
	}
	addr, err := netip.ParseAddr(p.NeighborAddress)
	if err != nil {
		return fmt.Errorf("ribtap: add peer: neighbor address %q invalid: %w", p.NeighborAddress, err)
	}
	if err := s.bgp.AddPeer(ctx, &api.AddPeerRequest{Peer: peer}); err != nil {
		return fmt.Errorf("ribtap: add peer %s (edge %s): %w", p.NeighborAddress, p.Edge, err)
	}
	s.registerEdge(addr, p.Edge)
	s.log.Info("RIB tap peer configured", "edge", p.Edge, "neighbor", p.NeighborAddress, "peer_asn", asn)
	return nil
}

// RemovePeer tears down one edge tap session and forgets its address→edge
// mapping. Used when sharding moves an edge off this controller (coverage
// change, L-05): the session drops and the producer stops seeing the edge's
// routes. Idempotent — removing an unknown address deletes the GoBGP peer (if
// any) and returns its result; a missing map entry is not an error.
func (s *Server) RemovePeer(ctx context.Context, neighborAddress string) error {
	addr, err := netip.ParseAddr(neighborAddress)
	if err != nil {
		return fmt.Errorf("ribtap: remove peer: neighbor address %q invalid: %w", neighborAddress, err)
	}
	if err := s.bgp.DeletePeer(ctx, &api.DeletePeerRequest{Address: neighborAddress}); err != nil {
		return fmt.Errorf("ribtap: remove peer %s: %w", neighborAddress, err)
	}
	s.mu.Lock()
	delete(s.peers, addr)
	s.mu.Unlock()
	s.log.Info("RIB tap peer removed", "neighbor", neighborAddress)
	return nil
}

// Peers returns a snapshot of the currently-configured tap sessions as
// edge→neighbor-address. The active-dial TapSink adapter (L-05) diffs the
// desired covered-edge set against this to compute AddPeer/RemovePeer.
func (s *Server) Peers() map[model.EdgeID]netip.Addr {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[model.EdgeID]netip.Addr, len(s.peers))
	for addr, edge := range s.peers {
		out[edge] = addr
	}
	return out
}

// recvAddPath builds an afi-safi that receives (and never advertises) with
// AddPath receive enabled, so the guard sees every path for a prefix.
func recvAddPath(afi api.Family_Afi) *api.AfiSafi {
	return &api.AfiSafi{
		Config: &api.AfiSafiConfig{
			Family:  &api.Family{Afi: afi, Safi: api.Family_SAFI_UNICAST},
			Enabled: true,
		},
		AddPaths:          &api.AddPaths{Config: &api.AddPathsConfig{Receive: true}},
		MpGracefulRestart: &api.MpGracefulRestart{Config: &api.MpGracefulRestartConfig{Enabled: true}},
	}
}

// PeerEstablished reports whether the session to addr is in ESTABLISHED state.
func (s *Server) PeerEstablished(ctx context.Context, addr string) (bool, error) {
	established := false
	err := s.bgp.ListPeer(ctx, &api.ListPeerRequest{Address: addr}, func(p *api.Peer) {
		if p.GetState().GetSessionState() == api.PeerState_SESSION_STATE_ESTABLISHED {
			established = true
		}
	})
	if err != nil {
		return false, fmt.Errorf("ribtap: list peer %s: %w", addr, err)
	}
	return established, nil
}

// BGP exposes the embedded server for the T-611 producer adapter (WatchEvent /
// ListPath). Other code must use Server's methods.
func (s *Server) BGP() *server.BgpServer { return s.bgp }

// Stop shuts BGP down.
func (s *Server) Stop(ctx context.Context) error {
	if err := s.bgp.StopBgp(ctx, &api.StopBgpRequest{}); err != nil {
		return fmt.Errorf("ribtap: stop bgp: %w", err)
	}
	return nil
}
