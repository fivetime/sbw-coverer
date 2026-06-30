package guard

import (
	"net/netip"
	"testing"

	"github.com/fivetime/sbw-contract/model"
)

// TestReconcileHostsRepairsDrift: a settled mirror that drifted from the
// authoritative adj-in (one dropped withdrawal + one dropped update) is repaired
// — stale removed, missed re-added — and a second pass is clean (idempotent).
func TestReconcileHostsRepairsDrift(t *testing.T) {
	g := New(canaryLC)
	bringValid(g, edge) // settled v4 view (peerUp + canary + EOR)
	g.OnEvent(host(edge, "203.0.113.10/32"))
	g.OnEvent(host(edge, "203.0.113.11/32"))
	// Mirror = {.10, .11}. Ground truth (Snapshot) = {.11, .12}:
	// .10 stale (dropped withdrawal), .12 missed (dropped update), .11 in sync.
	auth := []netip.Prefix{mustPfx("203.0.113.11/32"), mustPfx("203.0.113.12/32")}

	missed, stale, settled := g.ReconcileHosts(edge, model.FamilyIPv4, auth)
	if !settled {
		t.Fatal("a settled view must reconcile")
	}
	if len(missed) != 1 || missed[0] != mustPfx("203.0.113.12/32") {
		t.Errorf("missed = %v, want [.12]", missed)
	}
	if len(stale) != 1 || stale[0] != mustPfx("203.0.113.10/32") {
		t.Errorf("stale = %v, want [.10]", stale)
	}
	if !g.HasHost(edge, mustPfx("203.0.113.12/32")) {
		t.Error("missed host must be added to the mirror")
	}
	if g.HasHost(edge, mustPfx("203.0.113.10/32")) {
		t.Error("stale host must be removed from the mirror")
	}
	if !g.HasHost(edge, mustPfx("203.0.113.11/32")) {
		t.Error("in-sync host must be kept")
	}
	// Idempotent: now in sync, a second pass repairs nothing.
	m2, s2, _ := g.ReconcileHosts(edge, model.FamilyIPv4, auth)
	if len(m2) != 0 || len(s2) != 0 {
		t.Errorf("second pass must be clean, got missed=%v stale=%v", m2, s2)
	}
}

// TestReconcileHostsSkipsUnsettled: a view without EOR (mid-replay) is left
// untouched — the audit must never fight the live replay or mass-withdraw.
func TestReconcileHostsSkipsUnsettled(t *testing.T) {
	g := New(canaryLC)
	g.OnEvent(peerUp(edge)) // peer up, but no EOR → unsettled
	g.OnEvent(host(edge, "203.0.113.10/32"))

	missed, stale, settled := g.ReconcileHosts(edge, model.FamilyIPv4, nil)
	if settled {
		t.Error("a view without EOR must not reconcile (rule 4 / replay safety)")
	}
	if missed != nil || stale != nil {
		t.Errorf("unsettled must report no drift, got missed=%v stale=%v", missed, stale)
	}
	if !g.HasHost(edge, mustPfx("203.0.113.10/32")) {
		t.Error("unsettled mirror must be left intact")
	}
}

// TestReconcileHostsFamilyScoped: reconciling one family must not touch the
// other's mirrored hosts.
func TestReconcileHostsFamilyScoped(t *testing.T) {
	g := New(canaryLC)
	g.OnEvent(peerUp(edge))
	g.OnEvent(canaryUp(edge))
	g.OnEvent(eor(edge, model.FamilyIPv4))
	g.OnEvent(eor(edge, model.FamilyIPv6))
	g.OnEvent(host(edge, "203.0.113.10/32")) // v4
	g.OnEvent(host(edge, "2001:db8::1/128")) // v6

	// Reconcile v4 against empty truth → v4 .10 is stale; v6 host untouched.
	missed, stale, settled := g.ReconcileHosts(edge, model.FamilyIPv4, nil)
	if !settled {
		t.Fatal("settled")
	}
	if len(missed) != 0 || len(stale) != 1 || stale[0] != mustPfx("203.0.113.10/32") {
		t.Errorf("want 1 v4 stale, got missed=%v stale=%v", missed, stale)
	}
	if g.HasHost(edge, mustPfx("203.0.113.10/32")) {
		t.Error("v4 stale host must be removed")
	}
	if !g.HasHost(edge, mustPfx("2001:db8::1/128")) {
		t.Error("v6 host must be untouched by a v4 reconcile")
	}
}

// TestReconcileHostsFiresConflict: a repair that adds a SECOND source for a
// prefix fires the rule-6 multi-source alarm, exactly as a live event would.
func TestReconcileHostsFiresConflict(t *testing.T) {
	var raised []Conflict
	g := New(canaryLC, WithConflictHandler(func(c Conflict, resolved bool) {
		if !resolved {
			raised = append(raised, c)
		}
	}))
	a := model.EdgeID("edge-a")
	b := model.EdgeID("edge-b")
	bringValid(g, a)
	bringValid(g, b)
	g.OnEvent(host(a, "203.0.113.5/32")) // A advertises .5

	// B's mirror missed .5 (dropped update); ground truth says B holds it too →
	// reconcile re-adds it on B, making .5 multi-sourced → conflict.
	g.ReconcileHosts(b, model.FamilyIPv4, []netip.Prefix{mustPfx("203.0.113.5/32")})
	if len(raised) != 1 || raised[0].Prefix != mustPfx("203.0.113.5/32") {
		t.Fatalf("repair creating a 2nd source must raise a conflict, got %v", raised)
	}
	if len(raised[0].Edges) != 2 {
		t.Errorf("conflict must list both source edges, got %v", raised[0].Edges)
	}
}
