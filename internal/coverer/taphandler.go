package coverer

import (
	"context"
	"net/netip"
	"time"

	"github.com/fivetime/sbw-contract/model"
	"github.com/fivetime/sbw-coverer/internal/ribevent"
)

// TapHandler returns the ribevent.Handler that fuses the RIB tap into the coverer: every
// event feeds the RIB-survival guard (STAYS coverer-local), and every cross-boundary
// signal that the monolith handled IN-PROCESS (HardDown/Up, canary liveness, host /32
// changes, member-up/down, anchor re-render) is ROUTED UP as a CovererReport — none
// dropped. The server re-implements the in-process halves off those reports.
//
// The post-split fate of each monolith tap.go call (DESIGN tapSplit):
//   - guard.OnEvent                         → STAYS (the lossy /32 mirror is coverer-side)
//   - PeerDown → HardDown                   → DEATH_VOTE{down:true}
//   - canary Withdrawal → CanaryDown        → DEATH_VOTE{down:true}  (#6: the proto has no
//     soft/hard bit, so the SOFT canary cannot be distinguished from a hard PeerDown
//     over the current contract — a known regression flagged in DESIGN risks, routed
//     through a Report rather than dropped)
//   - canary PathUpdate → CanaryUp/HardUp   → DEATH_VOTE{down:false}
//   - host /32 add/remove → onHostChange    → MEMBER_EDGE, GATED by the guard verdict
//     (HasHost for add, ShouldWithdraw for remove); the resulting Down bit IS the
//     verdict the server consumes for render suppression + member-up/down + locality.
//   - EOR → markOrRerender                   → MEMBER_EDGE full-snapshot (HostsByFamily)
//
// memberHome / Agents.IsSubscribed K-dedup / emitMemberUp/Down / markOrRerender all
// collapse into the MEMBER_EDGE report — NONE survive on the coverer (it has no YB, no
// orchestrator). The server re-implements them off the report stream.
//
// The handler is synchronous and in-order per edge (the producer contract), so guard +
// canary state stay consistent with the event stream.
func (c *Coverer) TapHandler() ribevent.Handler {
	return func(e ribevent.Event) {
		c.guard.OnEvent(e)

		switch e.Kind {
		case ribevent.PeerDown:
			// Session gone → HARD death vote.
			c.reportDeathVote(e.Edge, true, false)
		case ribevent.Withdrawal:
			// A BGP withdrawal carries no attributes, so the canary is recognised by the
			// remembered prefix, not its large community.
			if c.isCanary(e.LargeCommunities) || c.forgetCanary(e.Edge, e.Prefix) {
				// Canary gone while the session is up → SOFT signal (DESIGN-liveness §4.7):
				// the server's CanaryDown only fails over together with an agent data-plane-
				// death report, never on its own.
				c.log.Warn("canary withdrawn → soft CanaryDown", "edge", e.Edge, "prefix", e.Prefix)
				c.reportDeathVote(e.Edge, true, true)
			} else if model.IsHost(e.Prefix) && c.viewReplayed(e.Edge, e.Family) {
				// Host /32 gone (post-EOR only). GATE on the guard's trustworthy-absence
				// verdict (route-withdrawal veto stays coverer-side; the verdict is what the
				// server consumes). A frozen/mid-replay view → ShouldWithdraw false → no report.
				if c.guard == nil || c.guard.ShouldWithdraw(e.Edge, e.Prefix) {
					c.reportMemberEdge(e.Edge, []netip.Prefix{e.Prefix}, true)
				}
			}
		case ribevent.PathUpdate:
			if c.isCanary(e.LargeCommunities) {
				c.log.Info("canary up (remembered)", "edge", e.Edge, "prefix", e.Prefix)
				c.rememberCanary(e.Edge, e.Prefix)      // so its attribute-less withdrawal is recognised
				c.reportDeathVote(e.Edge, false, true)  // soft CanaryUp (clears the soft signal)
				c.reportDeathVote(e.Edge, false, false) // canary back ⇒ session up → clear the hard vote
			} else if model.IsHost(e.Prefix) && c.viewReplayed(e.Edge, e.Family) {
				// Gate the per-host fusion on a VALID view (post-EOR). A tap (re)connect
				// replays the WHOLE adj-in as PathUpdates; firing per replayed host would
				// flood the uplink. OnEvent already rebuilt the guard view; the EOR case
				// below applies it ONCE. Only LIVE host changes (post-EOR) take this path.
				if c.guard == nil || c.guard.HasHost(e.Edge, e.Prefix) {
					c.reportMemberEdge(e.Edge, []netip.Prefix{e.Prefix}, false)
				}
			}
		case ribevent.EOR:
			// Replay done for this (edge, family): emit the rebuilt guard view ONCE as a
			// MEMBER_EDGE full-snapshot (replacing BOTH the per-host floods suppressed above
			// AND the monolith's server-side batch re-render). The server reconciles its
			// member→edge map for (coverer, edge, family) to this authoritative set in one
			// shot. Async so the tap event stream is never blocked.
			go c.emitEORSnapshot(e.Edge, e.Family)
		}
	}
}

// emitEORSnapshot reports the guard's full host set for (edge, family) as a MEMBER_EDGE
// snapshot — the post-EOR reconcile + the drift-repair re-emit share it.
func (c *Coverer) emitEORSnapshot(edge model.EdgeID, family model.Family) {
	if c.guard == nil {
		return
	}
	hosts := c.guard.HostsByFamily(edge, family)
	c.reportMemberEdge(edge, hosts, false)
}

// RunTap drives the GoBGP RIB-tap producer into the coverer's guard + Report fusion via
// TapHandler, blocking until ctx is cancelled or the producer errors.
func (c *Coverer) RunTap(ctx context.Context, producer ribevent.Producer) error {
	c.log.Info("RIB tap starting", "canary", c.hasCanary)
	return producer.Run(ctx, c.TapHandler())
}

// tapSnapshotter pulls the authoritative point-in-time adj-in for one family (the
// concrete *ribtap.Producer satisfies it via ListPath).
type tapSnapshotter interface {
	Snapshot(model.Family) ([]ribevent.Event, error)
}

// ReconcileTapView is the adj-in reconciliation safety net (T-609): the guard's host
// mirror is built from the LOSSY WatchEvent stream, so a silently dropped
// PathUpdate/Withdrawal drifts it. This pulls the authoritative adj-in via Snapshot and
// reconciles each CURRENTLY-COVERED edge's SETTLED mirror against it, repairing drift.
// After a repair it re-emits that (edge, family)'s MEMBER_EDGE full snapshot (same shape
// as the EOR case) so the server stays consistent.
//
// It iterates srv.Peers() (the tap's currently-covered edges) — the coverer's analog of
// the monolith's cp.Registry.List (which was server-side). guard.ReconcileHosts already
// self-skips unsettled edges, so the audit never fights the live replay.
func (c *Coverer) ReconcileTapView(ctx context.Context, snap tapSnapshotter) error {
	if c.guard == nil {
		return nil
	}
	covered := c.srv.Peers() // edge → neighbor addr (the currently-tapped edges)
	for _, fam := range []model.Family{model.FamilyIPv4, model.FamilyIPv6} {
		evs, err := snap.Snapshot(fam)
		if err != nil {
			return err
		}
		// Authoritative host set per edge (skip the canary and non-host aggregates).
		byEdge := map[model.EdgeID][]netip.Prefix{}
		for _, e := range evs {
			if e.Kind != ribevent.PathUpdate || c.isCanary(e.LargeCommunities) || !model.IsHost(e.Prefix) {
				continue
			}
			byEdge[e.Edge] = append(byEdge[e.Edge], e.Prefix)
		}
		// Reconcile EVERY covered edge — including those with zero snapshot paths, so an
		// edge whose hosts all went stale (missed withdrawals) is still corrected. The
		// guard self-skips unsettled edges.
		for edge := range covered {
			missed, stale, settled := c.guard.ReconcileHosts(edge, fam, byEdge[edge])
			if !settled || (len(missed) == 0 && len(stale) == 0) {
				continue
			}
			if c.met != nil {
				c.met.RIBReconcileDrift(edge, len(missed), len(stale))
			}
			c.log.Warn("adj-in mirror drift repaired (T-609)", "edge", edge, "family", fam,
				"missed", len(missed), "stale", len(stale))
			// Re-emit the repaired authoritative set so the server reconciles to it.
			c.emitEORSnapshot(edge, fam)
		}
	}
	return nil
}

// RunReconcileTapView runs ReconcileTapView every interval until ctx is cancelled.
// Blocks; run in a goroutine. A nil snapshotter (no tap) is a no-op.
func (c *Coverer) RunReconcileTapView(ctx context.Context, snap tapSnapshotter, interval time.Duration) {
	if snap == nil {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := c.ReconcileTapView(ctx, snap); err != nil {
				c.log.Warn("adj-in reconciliation failed", "err", err)
			}
		}
	}
}
