// Package guard is the RIB存活守卫 (controller §6): a state machine over the
// normalized RouteEvent stream (ribevent) that mirrors each edge's tap RIB and
// answers the only question the controller must get right before driving an
// anchor advertisement — "is member X actually present in the fabric, on a view
// I can trust?" Getting this wrong means盲写黑洞: advertising a /32 for an IP
// that has left the fabric, drawing traffic into a black hole (§6.1).
//
// The guard consumes ONLY ribevent.Event (§6.2): the GoBGP/BMP producer is a
// replaceable adapter, and the guard is unit-tested by feeding synthetic events.
//
// It implements the read side of the six guard rules (§6.4):
//   - Rule 4 (EOR gating): absence is untrustworthy until the edge/AFI EOR — a
//     view mid-replay has incomplete state, so a "missing" host route is "still
//     loading", not "gone".
//   - Rule 2 (canary): each edge exports a canary route (loopback + a designated
//     large community); its withdrawal/absence means the view is invalid. Fused
//     with the peer FSM (PeerUp/PeerDown) by AND — both must be up for a valid
//     view.
//   - Rule 3 (freeze): an invalid view neither withdraws existing anchors nor
//     admits new ones; a tap flap is never amplified into bulk withdrawals.
//   - Rule 1 (host-route check): the read primitives ShouldAdvertise /
//     ShouldWithdraw gate anchor decisions on host-route presence in a trusted
//     view. Home-assignment policy (which edge should hold X) lives above, in
//     the controller; the guard supplies the per-edge facts.
package guard

import (
	"net/netip"
	"sync"

	"github.com/fivetime/sbw-contract/model"
	"github.com/fivetime/sbw-coverer/internal/ribevent"
)

// edgeView is the mirrored tap state for one edge.
type edgeView struct {
	peerUp   bool                      // peer FSM up (PeerUp seen, no PeerDown since)
	canaryUp bool                      // canary route present (end-to-end export proven)
	eor      map[model.Family]bool     // EOR seen per AFI (absence trustworthy gate)
	hosts    map[netip.Prefix]struct{} // member host routes (/32, /128) present
	// canaryPfx is the prefix last seen advertised AS the canary (carrying the
	// canary large community). A BGP withdrawal carries NO attributes, so a canary
	// withdrawal cannot be matched by its large community — it is recognised by
	// this remembered prefix instead (mirrors the liveness rememberCanary, §6.4-2).
	canaryPfx netip.Prefix
}

func newEdgeView() *edgeView {
	return &edgeView{eor: map[model.Family]bool{}, hosts: map[netip.Prefix]struct{}{}}
}

// reset clears a view to "nothing known" — used on PeerDown (§6.2: a session
// loss invalidates the edge's entire view) and on PeerUp (a fresh replay
// follows, so prior state is stale).
func (v *edgeView) reset() {
	v.peerUp = false
	v.canaryUp = false
	v.eor = map[model.Family]bool{}
	v.hosts = map[netip.Prefix]struct{}{}
	v.canaryPfx = netip.Prefix{}
}

// valid reports whether the edge's view can be trusted for absence checks in the
// given family: peer up AND canary up (rule 2 AND) AND that family's EOR seen
// (rule 4). A false here freezes withdrawals (rule 3).
func (v *edgeView) valid(f model.Family) bool {
	return v.peerUp && v.canaryUp && v.eor[f]
}

// Guard mirrors all edges' tap RIBs and answers anchor-gating questions. Safe
// for concurrent use: events fold under a lock, queries read under it.
type Guard struct {
	mu       sync.Mutex
	canaryLC model.LargeCommunity
	edges    map[model.EdgeID]*edgeView

	// onConflict fires when a host prefix transitions into / out of being
	// advertised by more than one edge (rule 6, T-612). Alarm-only.
	onConflict func(Conflict, bool)

	// onViewChange fires when an edge's per-family view validity flips (T-1004):
	// valid→false is a FREEZE (the view can no longer be trusted for absence, so
	// withdrawals are held — a session/canary loss the controller should surface),
	// false→valid is a THAW. Alarm-only.
	onViewChange func(edge model.EdgeID, family model.Family, valid bool)
}

// Option configures a Guard.
type Option func(*Guard)

// WithConflictHandler registers a callback fired when a member prefix becomes
// multi-sourced (resolved=false) or drops back to a single source
// (resolved=true) — the unique-advertisement alarm (rule 6, §6.4-6). It is
// invoked OUTSIDE the guard lock; the handler must not block. Alarm-only: the
// guard never auto-withdraws a multi-sourced prefix.
func WithConflictHandler(fn func(Conflict, bool)) Option {
	return func(g *Guard) { g.onConflict = fn }
}

// WithViewChangeHandler registers a callback fired when an edge's per-family view
// validity flips (T-1004): valid=false is a freeze (view untrustworthy → absence
// checks held), valid=true a thaw. Invoked OUTSIDE the guard lock; must not block.
func WithViewChangeHandler(fn func(edge model.EdgeID, family model.Family, valid bool)) Option {
	return func(g *Guard) { g.onViewChange = fn }
}

// New builds a guard. canaryLC identifies canary routes: any PathUpdate carrying
// it is treated as its edge's canary (rule 2), regardless of prefix.
func New(canaryLC model.LargeCommunity, opts ...Option) *Guard {
	g := &Guard{canaryLC: canaryLC, edges: map[model.EdgeID]*edgeView{}}
	for _, o := range opts {
		o(g)
	}
	return g
}

func (g *Guard) view(edge model.EdgeID) *edgeView {
	v, ok := g.edges[edge]
	if !ok {
		v = newEdgeView()
		g.edges[edge] = v
	}
	return v
}

// OnEvent folds one normalized event into the mirror. Register it as the
// ribevent.Handler driving the producer. Multi-source alarms (rule 6) are fired
// outside the lock after the fold.
func (g *Guard) OnEvent(e ribevent.Event) {
	g.mu.Lock()
	affected := g.affectedHosts(e)                     // prefixes whose source-set this event may change
	before := g.multiSourced(affected)                 // which were multi-sourced before
	v4Before, v6Before := g.validitySnapshot(e.Edge)   // view validity before the fold
	g.fold(e)                                          // mutate state
	notices := g.conflictTransitions(affected, before) // detect crossings
	views := g.viewTransitions(e.Edge, v4Before, v6Before)
	g.mu.Unlock()

	if g.onConflict != nil {
		for _, n := range notices {
			g.onConflict(n.conflict, n.resolved)
		}
	}
	for _, v := range views {
		g.onViewChange(v.edge, v.family, v.valid)
	}
}

// validitySnapshot reads an edge's per-family view validity. Caller holds the lock.
func (g *Guard) validitySnapshot(edge model.EdgeID) (v4, v6 bool) {
	v, ok := g.edges[edge]
	if !ok {
		return false, false
	}
	return v.valid(model.FamilyIPv4), v.valid(model.FamilyIPv6)
}

type viewNotice struct {
	edge   model.EdgeID
	family model.Family
	valid  bool
}

// viewTransitions emits a notice for each family whose validity changed vs the
// pre-fold snapshot. Caller holds the lock; notices fire after it is released.
func (g *Guard) viewTransitions(edge model.EdgeID, v4Before, v6Before bool) []viewNotice {
	if g.onViewChange == nil {
		return nil
	}
	v, ok := g.edges[edge]
	if !ok {
		return nil
	}
	var out []viewNotice
	if now := v.valid(model.FamilyIPv4); now != v4Before {
		out = append(out, viewNotice{edge, model.FamilyIPv4, now})
	}
	if now := v.valid(model.FamilyIPv6); now != v6Before {
		out = append(out, viewNotice{edge, model.FamilyIPv6, now})
	}
	return out
}

// fold applies one event to the mirror (caller holds the lock).
func (g *Guard) fold(e ribevent.Event) {
	v := g.view(e.Edge)

	switch e.Kind {
	case ribevent.PeerUp:
		// Fresh session: discard stale state; the replay (PathUpdates + EOR)
		// rebuilds it, EOR-gated until complete.
		v.reset()
		v.peerUp = true
	case ribevent.PeerDown:
		// Session lost → the edge's entire view is invalid (§6.2).
		v.reset()
	case ribevent.EOR:
		v.eor[e.Family] = true
	case ribevent.PathUpdate:
		if g.isCanary(e) {
			v.canaryUp = true
			v.canaryPfx = e.Prefix // remember so its attribute-less withdrawal is recognised
			return
		}
		if model.IsHost(e.Prefix) {
			v.hosts[e.Prefix] = struct{}{}
		}
	case ribevent.Withdrawal:
		// A withdrawal carries no large community, so match the canary by its
		// remembered prefix (rule 2: canary loss → view untrustworthy). isCanary is
		// also checked for the rare case attributes survive.
		if g.isCanary(e) || (v.canaryPfx.IsValid() && e.Prefix == v.canaryPfx) {
			v.canaryUp = false
			return
		}
		delete(v.hosts, e.Prefix)
	}
}

func (g *Guard) isCanary(e ribevent.Event) bool {
	for _, lc := range e.LargeCommunities {
		if lc == g.canaryLC {
			return true
		}
	}
	return false
}

// ViewValid reports whether edge's view is trustworthy for absence checks in
// family (rule 2 + rule 4). Exposed for the controller's freeze decisions.
func (g *Guard) ViewValid(edge model.EdgeID, family model.Family) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	v, ok := g.edges[edge]
	return ok && v.valid(family)
}

// HasHost reports whether edge currently mirrors host route prefix.
func (g *Guard) HasHost(edge model.EdgeID, prefix netip.Prefix) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	v, ok := g.edges[edge]
	if !ok {
		return false
	}
	_, present := v.hosts[prefix]
	return present
}

// PresentAnywhere reports whether ANY edge mirrors host route prefix. Used for
// the rule-6 multi-source / migration views.
func (g *Guard) PresentAnywhere(prefix netip.Prefix) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, v := range g.edges {
		if _, ok := v.hosts[prefix]; ok {
			return true
		}
	}
	return false
}

// ShouldAdvertise reports whether member's anchor may be advertised on edge:
// the host route must be present in edge's mirror (rule 1, §6.4-1). Presence is
// a positive fact that does not require EOR (a route we can see is real); the
// EOR/freeze gate guards ABSENCE, not presence.
func (g *Guard) ShouldAdvertise(edge model.EdgeID, member netip.Prefix) bool {
	return g.HasHost(edge, member)
}

// ShouldWithdraw reports whether member's anchor on edge must be withdrawn: the
// host route is ABSENT and the view is trustworthy (valid + EOR seen for the
// family). When the view is invalid/frozen this returns false — a tap flap or
// mid-replay gap never triggers a withdrawal (rules 3 + 4).
func (g *Guard) ShouldWithdraw(edge model.EdgeID, member netip.Prefix) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	v, ok := g.edges[edge]
	if !ok {
		return false
	}
	if !v.valid(model.FamilyOf(member)) {
		return false // frozen: cannot trust absence
	}
	_, present := v.hosts[member]
	return !present
}

// HostsByFamily returns the host prefixes of `family` edge currently mirrors —
// the adj-in the guard believes the edge advertises. For the periodic adj-in
// reconciliation audit (T-609) and inspection.
func (g *Guard) HostsByFamily(edge model.EdgeID, family model.Family) []netip.Prefix {
	g.mu.Lock()
	defer g.mu.Unlock()
	v, ok := g.edges[edge]
	if !ok {
		return nil
	}
	var out []netip.Prefix
	for p := range v.hosts {
		if model.FamilyOf(p) == family {
			out = append(out, p)
		}
	}
	return out
}

// ReconcileHosts corrects edge's mirrored host set for `family` to the
// AUTHORITATIVE set pulled from the tap (a Snapshot/ListPath, = ground truth),
// returning the drift it repaired: `missed` (in authoritative, absent from the
// mirror — a dropped PathUpdate, re-added) and `stale` (in the mirror, absent
// from authoritative — a dropped Withdrawal, removed). This is the §6 adj-in
// reconciliation safety net (T-609): the live WatchEvent stream can silently drop
// an event, drifting the mirror and so the suppress decision; a periodic Snapshot
// re-pull catches and repairs it.
//
// It acts ONLY on a SETTLED view (peerUp ∧ eor[family]); a mid-replay or down
// edge returns settled=false and is left untouched, so the audit never fights the
// live replay (whose snapshot would be a moving target). The repair fires the
// same rule-6 conflict transitions OnEvent does for the net source-set change.
func (g *Guard) ReconcileHosts(edge model.EdgeID, family model.Family, authoritative []netip.Prefix) (missed, stale []netip.Prefix, settled bool) {
	g.mu.Lock()
	v, ok := g.edges[edge]
	if !ok || !v.peerUp || !v.eor[family] {
		g.mu.Unlock()
		return nil, nil, false // unsettled: do not fight the live replay
	}

	auth := make(map[netip.Prefix]struct{}, len(authoritative))
	for _, p := range authoritative {
		if model.IsHost(p) && model.FamilyOf(p) == family {
			auth[p] = struct{}{}
		}
	}
	for p := range v.hosts {
		if model.FamilyOf(p) != family {
			continue
		}
		if _, ok := auth[p]; !ok {
			stale = append(stale, p) // mirror has it, ground truth doesn't → dropped withdrawal
		}
	}
	for p := range auth {
		if _, ok := v.hosts[p]; !ok {
			missed = append(missed, p) // ground truth has it, mirror doesn't → dropped update
		}
	}
	if len(missed) == 0 && len(stale) == 0 {
		g.mu.Unlock()
		return nil, nil, true // in sync
	}

	affected := make([]netip.Prefix, 0, len(missed)+len(stale))
	affected = append(affected, missed...)
	affected = append(affected, stale...)
	before := g.multiSourced(affected)
	for _, p := range missed {
		v.hosts[p] = struct{}{}
	}
	for _, p := range stale {
		delete(v.hosts, p)
	}
	notices := g.conflictTransitions(affected, before)
	g.mu.Unlock()

	if g.onConflict != nil {
		for _, n := range notices {
			g.onConflict(n.conflict, n.resolved)
		}
	}
	return missed, stale, true
}
