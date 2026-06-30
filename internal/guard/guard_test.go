package guard

import (
	"net/netip"
	"testing"

	"github.com/fivetime/sbw-contract/model"

	"github.com/fivetime/sbw-coverer/internal/ribevent"
)

var canaryLC = model.LargeCommunity{GlobalAdmin: 65010, LocalData1: 999, LocalData2: 1}

const edge = model.EdgeID("edge-2")

func mustPfx(s string) netip.Prefix { return netip.MustParsePrefix(s) }

func peerUp(e model.EdgeID) ribevent.Event   { return ribevent.Event{Kind: ribevent.PeerUp, Edge: e} }
func peerDown(e model.EdgeID) ribevent.Event { return ribevent.Event{Kind: ribevent.PeerDown, Edge: e} }
func eor(e model.EdgeID, f model.Family) ribevent.Event {
	return ribevent.Event{Kind: ribevent.EOR, Edge: e, Family: f}
}

func canaryUp(e model.EdgeID) ribevent.Event {
	return ribevent.Event{
		Kind: ribevent.PathUpdate, Edge: e, Family: model.FamilyIPv4,
		Prefix: mustPfx("10.255.0.2/32"), LargeCommunities: []model.LargeCommunity{canaryLC},
	}
}

func canaryGone(e model.EdgeID) ribevent.Event {
	return ribevent.Event{
		Kind: ribevent.Withdrawal, Edge: e,
		Prefix: mustPfx("10.255.0.2/32"), LargeCommunities: []model.LargeCommunity{canaryLC},
	}
}

func host(e model.EdgeID, p string) ribevent.Event {
	pfx := mustPfx(p)
	f := model.FamilyIPv4
	if pfx.Addr().Is6() {
		f = model.FamilyIPv6
	}
	return ribevent.Event{Kind: ribevent.PathUpdate, Edge: e, Family: f, Prefix: pfx}
}

func hostGone(e model.EdgeID, p string) ribevent.Event {
	return ribevent.Event{Kind: ribevent.Withdrawal, Edge: e, Prefix: mustPfx(p)}
}

// bringValid drives an edge to a valid, EOR-complete IPv4 view.
func bringValid(g *Guard, e model.EdgeID) {
	g.OnEvent(peerUp(e))
	g.OnEvent(canaryUp(e))
	g.OnEvent(eor(e, model.FamilyIPv4))
}

func TestViewValidityRequiresAllThreeSignals(t *testing.T) {
	member := mustPfx("203.0.113.5/32")
	g := New(canaryLC)

	g.OnEvent(peerUp(edge))
	if g.ViewValid(edge, model.FamilyIPv4) {
		t.Error("peer up alone must not be valid (no canary, no EOR)")
	}
	g.OnEvent(canaryUp(edge))
	if g.ViewValid(edge, model.FamilyIPv4) {
		t.Error("peer+canary without EOR must not be valid (rule 4)")
	}
	g.OnEvent(eor(edge, model.FamilyIPv4))
	if !g.ViewValid(edge, model.FamilyIPv4) {
		t.Error("peer+canary+EOR must be valid")
	}
	// v6 EOR not seen → v6 view still invalid (per-AFI gating).
	if g.ViewValid(edge, model.FamilyIPv6) {
		t.Error("v6 EOR not seen → v6 view must be invalid")
	}
	_ = member
}

func TestEORGatesAbsenceWithdrawal(t *testing.T) {
	member := mustPfx("203.0.113.5/32")
	g := New(canaryLC)

	// Mid-replay (peer up, canary up, but NO EOR yet): host absent, but absence
	// is "still loading" — must NOT withdraw (rule 4).
	g.OnEvent(peerUp(edge))
	g.OnEvent(canaryUp(edge))
	if g.ShouldWithdraw(edge, member) {
		t.Error("absence before EOR must not trigger withdraw (rule 4)")
	}
	// EOR arrives, host still absent and view valid → trustworthy absence.
	g.OnEvent(eor(edge, model.FamilyIPv4))
	if !g.ShouldWithdraw(edge, member) {
		t.Error("absence after EOR on a valid view must allow withdraw")
	}
}

func TestHostPresenceGatesAdvertiseAndSuppressesWithdraw(t *testing.T) {
	member := mustPfx("203.0.113.5/32")
	g := New(canaryLC)
	bringValid(g, edge)

	if g.ShouldAdvertise(edge, member) {
		t.Error("absent host must not be advertisable")
	}
	g.OnEvent(host(edge, "203.0.113.5/32"))
	if !g.ShouldAdvertise(edge, member) || !g.HasHost(edge, member) {
		t.Error("present host must be advertisable")
	}
	if g.ShouldWithdraw(edge, member) {
		t.Error("present host must not be withdrawn")
	}
	// Host leaves the fabric on a valid view → withdraw.
	g.OnEvent(hostGone(edge, "203.0.113.5/32"))
	if !g.ShouldWithdraw(edge, member) {
		t.Error("host gone on valid view must withdraw (rule 1)")
	}
}

func TestCanaryLossFreezesWithdrawals(t *testing.T) {
	member := mustPfx("203.0.113.5/32")
	g := New(canaryLC)
	bringValid(g, edge)
	g.OnEvent(host(edge, "203.0.113.5/32"))

	// Canary withdrawn = view invalid (rule 2). A subsequent host withdrawal is
	// untrustworthy (tap/export problem, not a real departure) → freeze (rule 3).
	g.OnEvent(canaryGone(edge))
	if g.ViewValid(edge, model.FamilyIPv4) {
		t.Error("canary gone must invalidate the view")
	}
	g.OnEvent(hostGone(edge, "203.0.113.5/32"))
	if g.ShouldWithdraw(edge, member) {
		t.Error("host gone under invalid view must be frozen, not withdrawn (rule 3)")
	}
	// Canary returns, EOR re-seen → view valid again, absence now trustworthy.
	g.OnEvent(canaryUp(edge))
	g.OnEvent(eor(edge, model.FamilyIPv4))
	if !g.ShouldWithdraw(edge, member) {
		t.Error("after recovery, the real absence should withdraw")
	}
}

func TestPeerDownInvalidatesEntireView(t *testing.T) {
	member := mustPfx("203.0.113.5/32")
	g := New(canaryLC)
	bringValid(g, edge)
	g.OnEvent(host(edge, "203.0.113.5/32"))

	g.OnEvent(peerDown(edge))
	if g.ViewValid(edge, model.FamilyIPv4) {
		t.Error("PeerDown must invalidate the view (§6.2)")
	}
	// Hosts cleared, but the absence is frozen (view invalid) — no withdraw storm.
	if g.HasHost(edge, member) {
		t.Error("PeerDown must clear mirrored hosts")
	}
	if g.ShouldWithdraw(edge, member) {
		t.Error("absence after PeerDown must be frozen, not a withdraw storm (rule 3)")
	}
}

func TestPeerUpDiscardsStaleStateThenRebuilds(t *testing.T) {
	stale := mustPfx("203.0.113.9/32")
	fresh := mustPfx("203.0.113.5/32")
	g := New(canaryLC)
	bringValid(g, edge)
	g.OnEvent(host(edge, "203.0.113.9/32"))
	if !g.HasHost(edge, stale) {
		t.Fatal("setup: stale host should be present")
	}

	// Re-established session: stale state discarded, fresh replay.
	g.OnEvent(peerUp(edge))
	if g.HasHost(edge, stale) || g.ViewValid(edge, model.FamilyIPv4) {
		t.Error("PeerUp must discard stale hosts and re-gate the view")
	}
	g.OnEvent(canaryUp(edge))
	g.OnEvent(host(edge, "203.0.113.5/32"))
	g.OnEvent(eor(edge, model.FamilyIPv4))
	if g.HasHost(edge, stale) {
		t.Error("stale host must not reappear after fresh replay")
	}
	if !g.HasHost(edge, fresh) || !g.ViewValid(edge, model.FamilyIPv4) {
		t.Error("fresh replay must rebuild the view")
	}
}

func TestCanaryIdentifiedByCommunityNotPrefix(t *testing.T) {
	g := New(canaryLC)
	g.OnEvent(peerUp(edge))
	// A canary on an unexpected prefix still counts (identified by LC).
	g.OnEvent(ribevent.Event{
		Kind: ribevent.PathUpdate, Edge: edge, Family: model.FamilyIPv4,
		Prefix: mustPfx("192.0.2.123/32"), LargeCommunities: []model.LargeCommunity{canaryLC},
	})
	g.OnEvent(eor(edge, model.FamilyIPv4))
	if !g.ViewValid(edge, model.FamilyIPv4) {
		t.Error("canary identified by community must validate the view")
	}
	// And it is NOT tracked as a member host route.
	if g.HasHost(edge, mustPfx("192.0.2.123/32")) {
		t.Error("canary route must not be mirrored as a member host route")
	}
}

func TestPresentAnywhereAcrossEdges(t *testing.T) {
	member := mustPfx("203.0.113.5/32")
	g := New(canaryLC)
	bringValid(g, "edge-2")
	bringValid(g, "edge-5")
	g.OnEvent(host("edge-5", "203.0.113.5/32"))

	if g.HasHost("edge-2", member) {
		t.Error("edge-2 should not have the host")
	}
	if !g.PresentAnywhere(member) {
		t.Error("PresentAnywhere must see the host on edge-5")
	}
}

func TestIPv6HostAndEORGating(t *testing.T) {
	member := mustPfx("2001:db8::5/128")
	g := New(canaryLC)
	g.OnEvent(peerUp(edge))
	g.OnEvent(canaryUp(edge)) // canary is v4 loopback; gates the edge end-to-end
	// v6 EOR not yet seen → v6 absence frozen.
	if g.ShouldWithdraw(edge, member) {
		t.Error("v6 absence before v6 EOR must be frozen")
	}
	g.OnEvent(eor(edge, model.FamilyIPv6))
	if !g.ShouldWithdraw(edge, member) {
		t.Error("v6 absence after v6 EOR on a valid view must withdraw")
	}
	g.OnEvent(host(edge, "2001:db8::5/128"))
	if g.ShouldWithdraw(edge, member) || !g.HasHost(edge, member) {
		t.Error("present v6 host must not withdraw")
	}
}
