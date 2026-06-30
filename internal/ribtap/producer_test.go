package ribtap

import (
	"net/netip"
	"testing"
	"time"

	"github.com/osrg/gobgp/v4/pkg/apiutil"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"

	"github.com/fivetime/sbw-contract/model"
	"github.com/fivetime/sbw-coverer/internal/ribevent"
)

var pts = time.Unix(1_700_000_000, 0)

func testProducer(t *testing.T) (*Producer, netip.Addr) {
	t.Helper()
	srv, err := NewServer(Config{ASN: 65010, RouterID: "10.0.0.254", ListenPort: 1790}, nil)
	if err != nil {
		t.Fatal(err)
	}
	addr := netip.MustParseAddr("127.0.0.2")
	srv.registerEdge(addr, "edge-1")
	return NewProducer(srv), addr
}

func mustNLRI(t *testing.T, prefix string) bgp.NLRI {
	t.Helper()
	n, err := bgp.NewIPAddrPrefix(netip.MustParsePrefix(prefix))
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestPathEventNormalizesAdvertisement(t *testing.T) {
	p, addr := testProducer(t)
	nh, _ := bgp.NewPathAttributeNextHop(netip.MustParseAddr("127.0.0.2"))
	path := &apiutil.Path{
		Family:      bgp.RF_IPv4_UC,
		Nlri:        mustNLRI(t, "203.0.113.50/32"),
		PeerAddress: addr,
		Attrs: []bgp.PathAttributeInterface{
			nh,
			&bgp.PathAttributeLargeCommunities{Values: []*bgp.LargeCommunity{{ASN: 65010, LocalData1: 101, LocalData2: 5}}},
		},
	}
	ev, ok := p.pathEvent(path, pts)
	if !ok {
		t.Fatal("path should normalize")
	}
	if ev.Kind != ribevent.PathUpdate || ev.Edge != "edge-1" || ev.Family != model.FamilyIPv4 {
		t.Fatalf("event = %+v", ev)
	}
	if ev.Prefix.String() != "203.0.113.50/32" {
		t.Errorf("prefix = %s", ev.Prefix)
	}
	if ev.NextHop != netip.MustParseAddr("127.0.0.2") {
		t.Errorf("next hop = %s", ev.NextHop)
	}
	if len(ev.LargeCommunities) != 1 || ev.LargeCommunities[0] != (model.LargeCommunity{GlobalAdmin: 65010, LocalData1: 101, LocalData2: 5}) {
		t.Errorf("large communities = %+v", ev.LargeCommunities)
	}
	if err := ev.Validate(); err != nil {
		t.Errorf("normalized event invalid: %v", err)
	}
}

func TestPathEventWithdrawal(t *testing.T) {
	p, addr := testProducer(t)
	path := &apiutil.Path{
		Family: bgp.RF_IPv4_UC, Nlri: mustNLRI(t, "203.0.113.50/32"),
		PeerAddress: addr, Withdrawal: true,
	}
	ev, ok := p.pathEvent(path, pts)
	if !ok || ev.Kind != ribevent.Withdrawal {
		t.Fatalf("withdrawal not normalized: %+v ok=%v", ev, ok)
	}
}

func TestPathEventUnknownPeerDropped(t *testing.T) {
	p, _ := testProducer(t)
	path := &apiutil.Path{
		Family: bgp.RF_IPv4_UC, Nlri: mustNLRI(t, "203.0.113.50/32"),
		PeerAddress: netip.MustParseAddr("10.9.9.9"), // not a registered edge
	}
	if _, ok := p.pathEvent(path, pts); ok {
		t.Fatal("path from unknown peer must be dropped")
	}
}

func TestEorEvent(t *testing.T) {
	p, addr := testProducer(t)
	ev, ok := p.eorEvent(&apiutil.Path{Family: bgp.RF_IPv6_UC, PeerAddress: addr}, pts)
	if !ok || ev.Kind != ribevent.EOR || ev.Family != model.FamilyIPv6 || ev.Edge != "edge-1" {
		t.Fatalf("eor = %+v ok=%v", ev, ok)
	}
}

func peerStateEvent(addr netip.Addr, state bgp.FSMState) *apiutil.WatchEventMessage_PeerEvent {
	return &apiutil.WatchEventMessage_PeerEvent{
		Type: apiutil.PEER_EVENT_STATE,
		Peer: apiutil.Peer{State: apiutil.PeerState{NeighborAddress: addr, SessionState: state}},
	}
}

// DoD: a session loss yields exactly ONE PeerDown, and pre-establishment FSM
// churn yields nothing.
func TestPeerEventSynthesizesOnePeerDown(t *testing.T) {
	p, addr := testProducer(t)
	established := map[netip.Addr]bool{}

	// Pre-establishment churn: no events.
	for _, st := range []bgp.FSMState{bgp.BGP_FSM_ACTIVE, bgp.BGP_FSM_OPENSENT, bgp.BGP_FSM_OPENCONFIRM} {
		if _, ok := p.peerEvent(established, peerStateEvent(addr, st), pts); ok {
			t.Fatalf("pre-established state %v should emit nothing", st)
		}
	}

	// Establish → exactly one PeerUp.
	ev, ok := p.peerEvent(established, peerStateEvent(addr, bgp.BGP_FSM_ESTABLISHED), pts)
	if !ok || ev.Kind != ribevent.PeerUp || ev.Edge != "edge-1" {
		t.Fatalf("expected PeerUp, got %+v ok=%v", ev, ok)
	}
	// A redundant established notification emits nothing (no transition).
	if _, ok := p.peerEvent(established, peerStateEvent(addr, bgp.BGP_FSM_ESTABLISHED), pts); ok {
		t.Fatal("repeated established should not re-emit")
	}

	// Session loss → exactly one PeerDown.
	ev, ok = p.peerEvent(established, peerStateEvent(addr, bgp.BGP_FSM_IDLE), pts)
	if !ok || ev.Kind != ribevent.PeerDown {
		t.Fatalf("expected PeerDown, got %+v ok=%v", ev, ok)
	}
	// Further down states (reconnect churn) emit no more PeerDowns.
	for _, st := range []bgp.FSMState{bgp.BGP_FSM_ACTIVE, bgp.BGP_FSM_CONNECT} {
		if _, ok := p.peerEvent(established, peerStateEvent(addr, st), pts); ok {
			t.Fatalf("post-down churn state %v must not emit another PeerDown", st)
		}
	}
}

func TestPeerEventIgnoresNonStateAndUnknown(t *testing.T) {
	p, addr := testProducer(t)
	established := map[netip.Addr]bool{}
	// Non-STATE peer event type ignored.
	if _, ok := p.peerEvent(established, &apiutil.WatchEventMessage_PeerEvent{Type: apiutil.PEER_EVENT_INIT}, pts); ok {
		t.Error("non-state peer event should be ignored")
	}
	// Unknown peer ignored.
	unk := peerStateEvent(netip.MustParseAddr("10.9.9.9"), bgp.BGP_FSM_ESTABLISHED)
	if _, ok := p.peerEvent(established, unk, pts); ok {
		t.Error("unknown peer should be ignored")
	}
	_ = addr
}
