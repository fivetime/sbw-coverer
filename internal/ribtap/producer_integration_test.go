//go:build integration

package ribtap

import (
	"context"
	"sync"
	"testing"
	"time"

	api "github.com/osrg/gobgp/v4/api"
	"github.com/osrg/gobgp/v4/pkg/apiutil"

	"github.com/fivetime/sbw-contract/model"

	"github.com/fivetime/sbw-coverer/internal/ribevent"
)

const producerPort = 11792

// collector is a thread-safe sink for the events the producer delivers from its
// watch goroutine.
type collector struct {
	mu  sync.Mutex
	evs []ribevent.Event
}

func (c *collector) handle(e ribevent.Event) {
	c.mu.Lock()
	c.evs = append(c.evs, e)
	c.mu.Unlock()
}

func (c *collector) snapshot() []ribevent.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]ribevent.Event(nil), c.evs...)
}

// T-611 acceptance with real GoBGP: the producer normalizes a live edge's
// lifecycle — establish, advertise, EOR, withdraw, disconnect — into the
// ribevent stream the guard consumes, and a session loss yields exactly ONE
// PeerDown (not a withdrawal storm).
func TestRealProducerNormalizesEventStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctrl, err := NewServer(Config{ASN: 65010, RouterID: "10.0.0.254", ListenPort: producerPort}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := ctrl.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ctrl.Stop(ctx) })
	if err := ctrl.AddPeer(ctx, Peer{Edge: "edge-1", NeighborAddress: "127.0.0.2"}); err != nil {
		t.Fatal(err)
	}

	col := &collector{}
	go func() { _ = NewProducer(ctrl).Run(ctx, col.handle) }()

	// Bring up an edge that establishes the session (no routes yet).
	edge := startEdge(t, ctx, "127.0.0.2", "10.0.0.1", producerPort, nil)

	// Establishing → exactly one PeerUp for edge-1.
	waitForEvent(t, col, "PeerUp edge-1", func(e ribevent.Event) bool {
		return e.Kind == ribevent.PeerUp && e.Edge == "edge-1"
	})

	// Advertise → PathUpdate, then the peer's EOR for v4.
	const prefix = "203.0.113.50/32"
	path := injectRoute(t, edge, prefix, "127.0.0.2")
	waitForEvent(t, col, "PathUpdate "+prefix, func(e ribevent.Event) bool {
		return e.Kind == ribevent.PathUpdate && e.Edge == "edge-1" && e.Prefix.String() == prefix
	})
	waitForEvent(t, col, "EOR edge-1", func(e ribevent.Event) bool {
		return e.Kind == ribevent.EOR && e.Edge == "edge-1"
	})

	// ListPath-based reconciliation snapshot (T-609 mechanism): the advertised
	// route is in the controller's RIB view, normalized and edge-stamped.
	snap, err := NewProducer(ctrl).Snapshot(model.FamilyIPv4)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	found := false
	for _, e := range snap {
		if e.Kind == ribevent.PathUpdate && e.Edge == "edge-1" && e.Prefix.String() == prefix {
			found = true
		}
	}
	if !found {
		t.Errorf("snapshot missing %s from edge-1: %v", prefix, snap)
	}

	// Withdraw → Withdrawal.
	if err := edge.DeletePath(apiutil.DeletePathRequest{Paths: []*apiutil.Path{path}}); err != nil {
		t.Fatalf("withdraw: %v", err)
	}
	waitForEvent(t, col, "Withdrawal "+prefix, func(e ribevent.Event) bool {
		return e.Kind == ribevent.Withdrawal && e.Prefix.String() == prefix
	})

	// Disconnect the edge → exactly one PeerDown.
	if err := edge.StopBgp(ctx, &api.StopBgpRequest{}); err != nil {
		t.Fatalf("stop edge: %v", err)
	}
	waitForEvent(t, col, "PeerDown edge-1", func(e ribevent.Event) bool {
		return e.Kind == ribevent.PeerDown && e.Edge == "edge-1"
	})
	// Let any FSM reconnect churn settle, then assert there was exactly one.
	time.Sleep(time.Second)
	if n := countKind(col, ribevent.PeerDown); n != 1 {
		t.Errorf("got %d PeerDown events, want exactly 1 (no withdrawal storm)", n)
	}
}

func waitForEvent(t *testing.T, col *collector, what string, pred func(ribevent.Event) bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, e := range col.snapshot() {
			if pred(e) {
				return
			}
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("never observed %s; events so far: %v", what, col.snapshot())
}

func countKind(col *collector, k ribevent.Kind) int {
	n := 0
	for _, e := range col.snapshot() {
		if e.Kind == k {
			n++
		}
	}
	return n
}
