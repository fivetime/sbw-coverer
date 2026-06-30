package guard

import (
	"net/netip"
	"testing"

	"github.com/fivetime/sbw-contract/model"
)

type alarm struct {
	prefix   netip.Prefix
	edges    []model.EdgeID
	resolved bool
}

// guardWithAlarms builds a guard recording every conflict transition.
func guardWithAlarms() (*Guard, *[]alarm) {
	var got []alarm
	g := New(canaryLC, WithConflictHandler(func(c Conflict, resolved bool) {
		got = append(got, alarm{prefix: c.Prefix, edges: c.Edges, resolved: resolved})
	}))
	return g, &got
}

func TestSingleSourceNoConflict(t *testing.T) {
	g, got := guardWithAlarms()
	bringValid(g, "edge-2")
	g.OnEvent(host("edge-2", "203.0.113.5/32"))

	if len(*got) != 0 {
		t.Errorf("single source must not alarm, got %v", *got)
	}
	if len(g.Conflicts()) != 0 {
		t.Errorf("no conflicts expected, got %v", g.Conflicts())
	}
	if srcs := g.Sources(netip.MustParsePrefix("203.0.113.5/32")); len(srcs) != 1 {
		t.Errorf("want 1 source, got %v", srcs)
	}
}

func TestSecondSourceAlarms(t *testing.T) {
	g, got := guardWithAlarms()
	member := netip.MustParsePrefix("203.0.113.5/32")
	bringValid(g, "edge-2")
	bringValid(g, "edge-5")

	g.OnEvent(host("edge-2", "203.0.113.5/32")) // 1 source: no alarm
	g.OnEvent(host("edge-5", "203.0.113.5/32")) // 2 sources: alarm

	if len(*got) != 1 {
		t.Fatalf("want 1 alarm, got %d: %v", len(*got), *got)
	}
	a := (*got)[0]
	if a.resolved || a.prefix != member {
		t.Errorf("want unresolved alarm for %s, got %+v", member, a)
	}
	if len(a.edges) != 2 || a.edges[0] != "edge-2" || a.edges[1] != "edge-5" {
		t.Errorf("alarm should carry both sorted sources, got %v", a.edges)
	}
	cs := g.Conflicts()
	if len(cs) != 1 || cs[0].Prefix != member || len(cs[0].Edges) != 2 {
		t.Errorf("Conflicts() should list the multi-sourced prefix, got %v", cs)
	}
}

func TestConflictResolvesOnWithdrawal(t *testing.T) {
	g, got := guardWithAlarms()
	bringValid(g, "edge-2")
	bringValid(g, "edge-5")
	g.OnEvent(host("edge-2", "203.0.113.5/32"))
	g.OnEvent(host("edge-5", "203.0.113.5/32")) // alarm

	g.OnEvent(hostGone("edge-5", "203.0.113.5/32")) // back to 1 source: resolved

	if len(*got) != 2 {
		t.Fatalf("want enter+resolve = 2 alarms, got %d: %v", len(*got), *got)
	}
	if !(*got)[1].resolved {
		t.Errorf("second alarm should be resolved, got %+v", (*got)[1])
	}
	if len(g.Conflicts()) != 0 {
		t.Errorf("conflict should be cleared, got %v", g.Conflicts())
	}
}

func TestConflictResolvesOnPeerDown(t *testing.T) {
	g, got := guardWithAlarms()
	bringValid(g, "edge-2")
	bringValid(g, "edge-5")
	g.OnEvent(host("edge-2", "203.0.113.5/32"))
	g.OnEvent(host("edge-5", "203.0.113.5/32")) // alarm

	// One source's session drops: its host clears → conflict resolves (the
	// remaining single source is legitimate). PeerDown must not be silent here.
	g.OnEvent(peerDown("edge-5"))

	if len(*got) != 2 || !(*got)[1].resolved {
		t.Fatalf("PeerDown should resolve the conflict, got %v", *got)
	}
	if len(g.Conflicts()) != 0 {
		t.Errorf("conflict should clear after PeerDown, got %v", g.Conflicts())
	}
}

func TestThirdSourceDoesNotReAlarmButAuditSeesAll(t *testing.T) {
	g, got := guardWithAlarms()
	member := netip.MustParsePrefix("203.0.113.5/32")
	for _, e := range []model.EdgeID{"edge-2", "edge-5", "edge-9"} {
		bringValid(g, e)
		g.OnEvent(host(e, "203.0.113.5/32"))
	}
	// Entering multi (2nd source) alarms once; the 3rd stays multi → no new
	// enter-alarm. The audit query still reports all three sources.
	if len(*got) != 1 {
		t.Errorf("want exactly 1 enter-alarm across 3 sources, got %d: %v", len(*got), *got)
	}
	if srcs := g.Sources(member); len(srcs) != 3 {
		t.Errorf("audit should see all 3 sources, got %v", srcs)
	}
}

func TestNoHandlerStillTracksConflicts(t *testing.T) {
	g := New(canaryLC) // no conflict handler
	bringValid(g, "edge-2")
	bringValid(g, "edge-5")
	g.OnEvent(host("edge-2", "203.0.113.5/32"))
	g.OnEvent(host("edge-5", "203.0.113.5/32"))

	if len(g.Conflicts()) != 1 {
		t.Errorf("Conflicts() must work without a handler, got %v", g.Conflicts())
	}
}
