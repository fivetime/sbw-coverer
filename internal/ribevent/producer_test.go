package ribevent

import (
	"context"
	"net/netip"
	"testing"

	"github.com/fivetime/sbw-contract/model"
)

// scriptedProducer is a mock Producer that replays a fixed Event sequence to the
// handler. It demonstrates the §6.1 DoD: the guard (here, a stand-in collector)
// can be unit-tested by feeding it normalized Events with no real BGP. A real
// guard's HandleEvent would be passed as the Handler in exactly this way.
type scriptedProducer struct{ events []Event }

func (p *scriptedProducer) Run(ctx context.Context, handler Handler) error {
	for _, e := range p.events {
		if err := ctx.Err(); err != nil {
			return err
		}
		handler(e)
	}
	return nil
}

func TestProducerContractDrivesAHandler(t *testing.T) {
	// A realistic startup: replay two paths, EOR (absence now trustworthy),
	// then a live withdrawal, then a session loss as a single PeerDown.
	prefix := netip.MustParsePrefix("203.0.113.10/32")
	prod := &scriptedProducer{events: []Event{
		NewPathUpdate("edge-1", prefix, netip.MustParseAddr("10.0.0.1"), nil, ts),
		NewPathUpdate("edge-1", netip.MustParsePrefix("203.0.113.20/32"), netip.MustParseAddr("10.0.0.1"), nil, ts),
		NewEOR("edge-1", model.FamilyIPv4, ts),
		NewWithdrawal("edge-1", prefix, netip.MustParseAddr("10.0.0.1"), nil, ts),
		NewPeerDown("edge-1", ts),
	}}

	var got []Kind
	for _, e := range collect(t, prod) {
		if err := e.Validate(); err != nil {
			t.Fatalf("producer emitted invalid event %s: %v", e, err)
		}
		got = append(got, e.Kind)
	}

	want := []Kind{PathUpdate, PathUpdate, EOR, Withdrawal, PeerDown}
	if len(got) != len(want) {
		t.Fatalf("got %d events, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("event %d kind = %s, want %s", i, got[i], want[i])
		}
	}
}

func TestProducerStopsOnContextCancel(t *testing.T) {
	prod := &scriptedProducer{events: []Event{NewPeerUp("e", ts), NewPeerUp("e", ts)}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled
	var n int
	err := prod.Run(ctx, func(Event) { n++ })
	if err == nil {
		t.Fatal("Run should return ctx error when cancelled")
	}
	if n != 0 {
		t.Errorf("delivered %d events after cancel, want 0", n)
	}
}

// collect runs a producer to completion and returns every event it delivered.
func collect(t *testing.T, p Producer) []Event {
	t.Helper()
	var out []Event
	if err := p.Run(context.Background(), func(e Event) { out = append(out, e) }); err != nil {
		t.Fatalf("producer Run: %v", err)
	}
	return out
}
