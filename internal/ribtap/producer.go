package ribtap

import (
	"context"
	"fmt"
	"net/netip"
	"time"

	api "github.com/osrg/gobgp/v4/api"
	"github.com/osrg/gobgp/v4/pkg/apiutil"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
	"github.com/osrg/gobgp/v4/pkg/server"

	"github.com/fivetime/sbw-contract/model"
	"github.com/fivetime/sbw-coverer/internal/ribevent"
)

// Producer is the GoBGP implementation of ribevent.Producer (T-611): it watches
// the embedded server's events and normalizes them into ribevent.Event, the only
// thing the guard sees. It is the §6.1 adapter — all the GoBGP/apiutil parsing
// lives here so the guard never touches a wire type.
//
// Two normalizations matter:
//   - session loss → a single PeerDown. GoBGP reports peer FSM transitions; the
//     producer tracks per-peer up/down and emits exactly one PeerDown when a
//     peer LEAVES established, so the guard never has to infer a session loss
//     from a withdrawal storm (DESIGN.md §6.1).
//   - replay → on Run the watch is opened with current=true, so the existing
//     adj-in is replayed as PathUpdate events then closed with an EOR per
//     (edge, family) — exactly the full-replay entry the guard needs after a
//     stateless controller restart (§6.3-8 / T-608).
type Producer struct {
	srv *Server
}

// NewProducer builds the GoBGP producer over a (started) tap server.
func NewProducer(srv *Server) *Producer { return &Producer{srv: srv} }

var _ ribevent.Producer = (*Producer)(nil)

// Run watches the tap and delivers normalized events to handler until ctx is
// cancelled. WatchEvent registers the callbacks and runs them on a single
// goroutine, so handler calls are serialized and ordered (the guard can stay a
// lock-free state machine). The peer up/down map is touched only from that
// goroutine.
func (p *Producer) Run(ctx context.Context, handler ribevent.Handler) error {
	established := map[netip.Addr]bool{}

	cb := server.WatchEventMessageCallbacks{
		OnPathUpdate: func(paths []*apiutil.Path, t time.Time) {
			for _, path := range paths {
				if ev, ok := p.pathEvent(path, t); ok {
					handler(ev)
				}
			}
		},
		OnPathEor: func(path *apiutil.Path, t time.Time) {
			if ev, ok := p.eorEvent(path, t); ok {
				handler(ev)
			}
		},
		OnPeerUpdate: func(pe *apiutil.WatchEventMessage_PeerEvent, t time.Time) {
			if ev, ok := p.peerEvent(established, pe, t); ok {
				handler(ev)
			}
		},
	}
	if err := p.srv.BGP().WatchEvent(ctx, cb,
		server.WatchUpdate(true, "", ""), // pre-policy adj-in, current=true → replay then live
		server.WatchEor(true),
		server.WatchPeer()); err != nil {
		return fmt.Errorf("ribtap: watch event: %w", err)
	}
	<-ctx.Done()
	return ctx.Err()
}

// Snapshot pulls the controller's current view of a family's RIB via ListPath
// and normalizes each path into a PathUpdate event — the point-in-time
// reconciliation/replay query (T-609). Run's current=true stream is the live
// replay; Snapshot is the on-demand pull the guard can reconcile against
// ground truth. With AddPath receive, multiple paths for one prefix are all
// returned (the §6.3-6 multipath the uniqueness check needs).
func (p *Producer) Snapshot(family model.Family) ([]ribevent.Event, error) {
	rf := bgp.RF_IPv4_UC
	if family == model.FamilyIPv6 {
		rf = bgp.RF_IPv6_UC
	}
	var out []ribevent.Event
	err := p.srv.BGP().ListPath(
		apiutil.ListPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Family: rf},
		func(_ bgp.NLRI, paths []*apiutil.Path) {
			for _, path := range paths {
				if ev, ok := p.pathEvent(path, time.Now()); ok {
					out = append(out, ev)
				}
			}
		})
	if err != nil {
		return nil, fmt.Errorf("ribtap: snapshot list path: %w", err)
	}
	return out, nil
}

// pathEvent normalizes one received path into a PathUpdate or Withdrawal. A path
// from an unrecognized peer, or with an unparseable NLRI, is dropped (the guard
// only deals in known edges).
func (p *Producer) pathEvent(path *apiutil.Path, t time.Time) (ribevent.Event, bool) {
	if path == nil || path.Nlri == nil {
		return ribevent.Event{}, false
	}
	edge, ok := p.srv.EdgeFor(path.PeerAddress)
	if !ok {
		return ribevent.Event{}, false
	}
	prefix, err := netip.ParsePrefix(path.Nlri.String())
	if err != nil {
		return ribevent.Event{}, false
	}
	nh := nextHopOf(path.Attrs)
	lcs := largeCommunitiesOf(path.Attrs)
	if path.Withdrawal {
		return ribevent.NewWithdrawal(edge, prefix, nh, lcs, t), true
	}
	return ribevent.NewPathUpdate(edge, prefix, nh, lcs, t), true
}

// eorEvent normalizes an end-of-RIB marker for one (edge, family).
func (p *Producer) eorEvent(path *apiutil.Path, t time.Time) (ribevent.Event, bool) {
	if path == nil {
		return ribevent.Event{}, false
	}
	edge, ok := p.srv.EdgeFor(path.PeerAddress)
	if !ok {
		return ribevent.Event{}, false
	}
	return ribevent.NewEOR(edge, modelFamily(path.Family), t), true
}

// peerEvent turns a peer FSM transition into a single PeerUp (on reaching
// established) or PeerDown (on leaving it). Intermediate FSM churn before the
// first establishment emits nothing.
func (p *Producer) peerEvent(established map[netip.Addr]bool, pe *apiutil.WatchEventMessage_PeerEvent, t time.Time) (ribevent.Event, bool) {
	if pe == nil || pe.Type != apiutil.PEER_EVENT_STATE {
		return ribevent.Event{}, false
	}
	addr := pe.Peer.State.NeighborAddress
	edge, ok := p.srv.EdgeFor(addr)
	if !ok {
		return ribevent.Event{}, false
	}
	up := pe.Peer.State.SessionState == bgp.BGP_FSM_ESTABLISHED
	was := established[addr]
	switch {
	case up && !was:
		established[addr] = true
		return ribevent.NewPeerUp(edge, t), true
	case !up && was:
		established[addr] = false
		return ribevent.NewPeerDown(edge, t), true
	default:
		return ribevent.Event{}, false
	}
}

// nextHopOf extracts the next hop from a path's attributes (the v4 NEXT_HOP
// attribute or the v6 MP_REACH next hop), used as the multipath discriminator.
func nextHopOf(attrs []bgp.PathAttributeInterface) netip.Addr {
	for _, a := range attrs {
		switch v := a.(type) {
		case *bgp.PathAttributeNextHop:
			return v.Value
		case *bgp.PathAttributeMpReachNLRI:
			return v.Nexthop
		}
	}
	return netip.Addr{}
}

// largeCommunitiesOf extracts large communities (e.g. the anchor/canary tags,
// §4.3) into the model type.
func largeCommunitiesOf(attrs []bgp.PathAttributeInterface) []model.LargeCommunity {
	for _, a := range attrs {
		lc, ok := a.(*bgp.PathAttributeLargeCommunities)
		if !ok {
			continue
		}
		out := make([]model.LargeCommunity, 0, len(lc.Values))
		for _, c := range lc.Values {
			out = append(out, model.LargeCommunity{GlobalAdmin: c.ASN, LocalData1: c.LocalData1, LocalData2: c.LocalData2})
		}
		return out
	}
	return nil
}

func modelFamily(f bgp.Family) model.Family {
	if f.Afi() == bgp.AFI_IP6 {
		return model.FamilyIPv6
	}
	return model.FamilyIPv4
}
