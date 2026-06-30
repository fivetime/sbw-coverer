package ribtap

import (
	"context"
	"fmt"
	"net/netip"
	"sort"
	"testing"

	"github.com/fivetime/sbw-contract/model"
)

// fakeTap records AddPeer/RemovePeer and serves a mutable peer set so the
// CoverageSink diff is tested without an embedded GoBGP.
type fakeTap struct {
	peers   map[model.EdgeID]netip.Addr
	added   []model.EdgeID
	removed []string
	failAdd map[model.EdgeID]bool
}

func newFakeTap(initial ...model.EdgeID) *fakeTap {
	f := &fakeTap{peers: map[model.EdgeID]netip.Addr{}}
	for _, e := range initial {
		f.peers[e] = addrOf(e)
	}
	return f
}

func addrOf(e model.EdgeID) netip.Addr {
	// deterministic 10.0.0.x from the edge's first rune
	return netip.AddrFrom4([4]byte{10, 0, 0, byte(string(e)[0])})
}

func (f *fakeTap) Peers() map[model.EdgeID]netip.Addr {
	out := make(map[model.EdgeID]netip.Addr, len(f.peers))
	for k, v := range f.peers {
		out[k] = v
	}
	return out
}

func (f *fakeTap) AddPeer(_ context.Context, p Peer) error {
	if f.failAdd[p.Edge] {
		return fmt.Errorf("boom %s", p.Edge)
	}
	f.peers[p.Edge] = netip.MustParseAddr(p.NeighborAddress)
	f.added = append(f.added, p.Edge)
	return nil
}

func (f *fakeTap) RemovePeer(_ context.Context, addr string) error {
	for e, a := range f.peers {
		if a.String() == addr {
			delete(f.peers, e)
			f.removed = append(f.removed, addr)
			return nil
		}
	}
	f.removed = append(f.removed, addr)
	return nil
}

func resolver() EdgePeer {
	return func(e model.EdgeID) (Peer, bool) {
		return Peer{NeighborAddress: addrOf(e).String(), PeerASN: 65010}, true
	}
}

func sink(tap tapPeers, r EdgePeer) *CoverageSink { return newCoverageSink(tap, r, nil) }

func keys(m map[model.EdgeID]netip.Addr) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, string(k))
	}
	sort.Strings(out)
	return out
}

// From empty, Ensure adds exactly the wanted edges.
func TestEnsureAddsMissing(t *testing.T) {
	tap := newFakeTap()
	s := sink(tap, resolver())
	if err := s.Ensure(context.Background(), []model.EdgeID{"a-edge", "b-edge"}); err != nil {
		t.Fatal(err)
	}
	if got := keys(tap.peers); len(got) != 2 || got[0] != "a-edge" || got[1] != "b-edge" {
		t.Fatalf("peers = %v, want [a-edge b-edge]", got)
	}
	if len(tap.added) != 2 || len(tap.removed) != 0 {
		t.Errorf("added=%v removed=%v", tap.added, tap.removed)
	}
}

// Ensure removes peers no longer covered and adds new ones in one pass.
func TestEnsureConvergesDiff(t *testing.T) {
	tap := newFakeTap("a-edge", "b-edge", "c-edge")
	s := sink(tap, resolver())
	// want: keep b, drop a & c, add d.
	if err := s.Ensure(context.Background(), []model.EdgeID{"b-edge", "d-edge"}); err != nil {
		t.Fatal(err)
	}
	got := keys(tap.peers)
	if len(got) != 2 || got[0] != "b-edge" || got[1] != "d-edge" {
		t.Fatalf("converged peers = %v, want [b-edge d-edge]", got)
	}
	if len(tap.added) != 1 || tap.added[0] != "d-edge" {
		t.Errorf("added = %v, want [d-edge]", tap.added)
	}
	if len(tap.removed) != 2 {
		t.Errorf("removed = %v, want 2 (a,c)", tap.removed)
	}
}

// Re-running Ensure with an unchanged set is a no-op (idempotent).
func TestEnsureIdempotent(t *testing.T) {
	tap := newFakeTap("a-edge", "b-edge")
	s := sink(tap, resolver())
	want := []model.EdgeID{"a-edge", "b-edge"}
	_ = s.Ensure(context.Background(), want)
	if len(tap.added) != 0 || len(tap.removed) != 0 {
		t.Fatalf("second-state Ensure should be no-op: added=%v removed=%v", tap.added, tap.removed)
	}
}

// An unresolvable edge is skipped, not fatal; the rest still converge.
func TestEnsureSkipsUnresolvable(t *testing.T) {
	tap := newFakeTap()
	r := func(e model.EdgeID) (Peer, bool) {
		if e == "ghost" {
			return Peer{}, false
		}
		return Peer{NeighborAddress: addrOf(e).String()}, true
	}
	s := sink(tap, r)
	if err := s.Ensure(context.Background(), []model.EdgeID{"a-edge", "ghost"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := tap.peers["a-edge"]; !ok {
		t.Error("resolvable edge a-edge should be peered")
	}
	if _, ok := tap.peers["ghost"]; ok {
		t.Error("unresolvable edge ghost must be skipped")
	}
}

// A failing AddPeer is reported but does not abort the other peers.
func TestEnsureAccumulatesErrors(t *testing.T) {
	tap := newFakeTap()
	tap.failAdd = map[model.EdgeID]bool{"bad-edge": true}
	s := sink(tap, resolver())
	err := s.Ensure(context.Background(), []model.EdgeID{"good-edge", "bad-edge"})
	if err == nil {
		t.Fatal("expected an error for the failing peer")
	}
	if _, ok := tap.peers["good-edge"]; !ok {
		t.Error("good-edge should still be added despite bad-edge failing")
	}
}
