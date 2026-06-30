//go:build integration

package ribtap

import (
	"context"
	"net/netip"
	"testing"
	"time"

	api "github.com/osrg/gobgp/v4/api"
	"github.com/osrg/gobgp/v4/pkg/apiutil"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
	"github.com/osrg/gobgp/v4/pkg/server"
)

// T-601 acceptance with real GoBGP (DESIGN.md §6.2): two synthetic edges
// (standing in for the edge BIRDs) actively peer into the controller's embedded
// server, and we confirm the three contract properties:
//
//  1. established — the controller's passive peers accept the inbound sessions;
//  2. no re-injection — the controller (export default REJECT) relays nothing,
//     so neither edge ever learns the other's routes (§1.1-6, 绝不回注);
//  3. multipath reaches the controller — both edges advertise the SAME prefix
//     with different next-hops, and the controller's RIB holds BOTH paths, the
//     signal the §6.3-6 uniqueness check (T-612) depends on.
//
// Self-contained: real GoBGP speaking real BGP over loopback, no external
// process. The edges source from distinct 127.0.0.x addresses so the controller
// can key them as separate peers.
const controllerPort = 11790

func TestRealTapPeeringReceiveNoReinjectMultipath(t *testing.T) {
	ctx := context.Background()

	ctrl, err := NewServer(Config{ASN: 65010, RouterID: "10.0.0.254", ListenPort: controllerPort}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := ctrl.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ctrl.Stop(ctx) })

	// Configure the two edge taps on the controller (passive, export-reject,
	// add-path receive).
	if err := ctrl.AddPeer(ctx, Peer{Edge: "edge-1", NeighborAddress: "127.0.0.2"}); err != nil {
		t.Fatal(err)
	}
	if err := ctrl.AddPeer(ctx, Peer{Edge: "edge-2", NeighborAddress: "127.0.0.3"}); err != nil {
		t.Fatal(err)
	}

	// The shared prefix (multipath) plus a unique prefix per edge (to detect
	// any re-injection: if the controller relayed, an edge would learn the
	// other's unique prefix).
	const shared = "203.0.113.50/32"
	edge1 := startEdge(t, ctx, "127.0.0.2", "10.0.0.1", controllerPort, map[string]string{
		shared:        "127.0.0.2",
		"10.1.1.1/32": "127.0.0.2",
	})
	edge2 := startEdge(t, ctx, "127.0.0.3", "10.0.0.2", controllerPort, map[string]string{
		shared:        "127.0.0.3",
		"10.2.2.2/32": "127.0.0.3",
	})

	// 1. Both sessions reach ESTABLISHED.
	waitEstablished(t, ctrl, "127.0.0.2")
	waitEstablished(t, ctrl, "127.0.0.3")

	// Give the post-establishment UPDATE exchange a moment to settle.
	waitForPaths(t, ctrl.BGP(), api.TableType_TABLE_TYPE_GLOBAL, "", shared, 2, 5*time.Second)

	// 3. Multipath: the controller holds BOTH paths for the shared prefix.
	if n := countPaths(t, ctrl.BGP(), api.TableType_TABLE_TYPE_GLOBAL, "", shared); n != 2 {
		t.Errorf("controller has %d paths for %s, want 2 (multipath not visible)", n, shared)
	}
	// And it received each edge's unique prefix.
	if n := countPaths(t, ctrl.BGP(), api.TableType_TABLE_TYPE_GLOBAL, "", "10.1.1.1/32"); n != 1 {
		t.Errorf("controller missing edge-1 unique route: %d paths", n)
	}

	// 2. No re-injection: neither edge learned anything from the controller.
	if n := countPaths(t, edge1, api.TableType_TABLE_TYPE_ADJ_IN, "127.0.0.254", ""); n != 0 {
		t.Errorf("edge-1 received %d routes from the controller, want 0 (re-injection!)", n)
	}
	if n := countPaths(t, edge2, api.TableType_TABLE_TYPE_ADJ_IN, "127.0.0.254", ""); n != 0 {
		t.Errorf("edge-2 received %d routes from the controller, want 0 (re-injection!)", n)
	}
	// Belt-and-suspenders: edge-2 must not know edge-1's unique route.
	if n := countPaths(t, edge2, api.TableType_TABLE_TYPE_GLOBAL, "", "10.1.1.1/32"); n != 0 {
		t.Errorf("edge-2 learned edge-1's route via the controller (re-injection!)")
	}
}

// startEdge spins a synthetic edge BGP speaker that actively dials the
// controller from localAddr and advertises the given prefix→next-hop routes.
func startEdge(t *testing.T, ctx context.Context, localAddr, routerID string, port uint32, routes map[string]string) *server.BgpServer {
	t.Helper()
	s := server.NewBgpServer()
	go s.Serve()
	if err := s.StartBgp(ctx, &api.StartBgpRequest{Global: &api.Global{
		Asn: 65010, RouterId: routerID, ListenPort: -1, // edges don't listen; they dial out
	}}); err != nil {
		t.Fatalf("edge %s StartBgp: %v", localAddr, err)
	}
	t.Cleanup(func() { _ = s.StopBgp(ctx, &api.StopBgpRequest{}) })

	peer := &api.Peer{
		Conf:            &api.PeerConf{NeighborAddress: "127.0.0.254", PeerAsn: 65010, LocalAsn: 65010},
		Transport:       &api.Transport{LocalAddress: localAddr, RemotePort: port},
		Timers:          &api.Timers{Config: &api.TimersConfig{ConnectRetry: 1, IdleHoldTimeAfterReset: 1}},
		GracefulRestart: &api.GracefulRestart{Enabled: true, RestartTime: 120, LonglivedEnabled: true},
		AfiSafis: []*api.AfiSafi{{
			Config:            &api.AfiSafiConfig{Family: &api.Family{Afi: api.Family_AFI_IP, Safi: api.Family_SAFI_UNICAST}, Enabled: true},
			AddPaths:          &api.AddPaths{Config: &api.AddPathsConfig{SendMax: 4}},
			MpGracefulRestart: &api.MpGracefulRestart{Config: &api.MpGracefulRestartConfig{Enabled: true}},
		}},
	}
	if err := s.AddPeer(ctx, &api.AddPeerRequest{Peer: peer}); err != nil {
		t.Fatalf("edge %s AddPeer: %v", localAddr, err)
	}

	// The controller's RouterID is 10.0.0.254; the edge dials it at 127.0.0.254.
	for prefix, nh := range routes {
		injectRoute(t, s, prefix, nh)
	}
	return s
}

func injectRoute(t *testing.T, s *server.BgpServer, prefix, nextHop string) *apiutil.Path {
	t.Helper()
	nlri, err := bgp.NewIPAddrPrefix(netip.MustParsePrefix(prefix))
	if err != nil {
		t.Fatalf("nlri %s: %v", prefix, err)
	}
	nh, err := bgp.NewPathAttributeNextHop(netip.MustParseAddr(nextHop))
	if err != nil {
		t.Fatalf("nexthop %s: %v", nextHop, err)
	}
	attrs := []bgp.PathAttributeInterface{
		bgp.NewPathAttributeOrigin(0), // IGP
		nh,
		bgp.NewPathAttributeLocalPref(100), // iBGP
	}
	p := &apiutil.Path{Family: bgp.RF_IPv4_UC, Nlri: nlri, Attrs: attrs, Age: time.Now().Unix()}
	if _, err := s.AddPath(apiutil.AddPathRequest{Paths: []*apiutil.Path{p}}); err != nil {
		t.Fatalf("inject %s: %v", prefix, err)
	}
	return p
}

func waitEstablished(t *testing.T, s *Server, addr string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if ok, _ := s.PeerEstablished(context.Background(), addr); ok {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("peer %s did not reach ESTABLISHED", addr)
}

func waitForPaths(t *testing.T, s *server.BgpServer, tt api.TableType, name, prefix string, want int, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if countPaths(t, s, tt, name, prefix) >= want {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// countPaths counts paths for prefix (or all prefixes if prefix=="") in the
// given table. name selects a peer for ADJ_IN tables.
func countPaths(t *testing.T, s *server.BgpServer, tt api.TableType, name, prefix string) int {
	t.Helper()
	n := 0
	req := apiutil.ListPathRequest{TableType: tt, Name: name, Family: bgp.RF_IPv4_UC}
	err := s.ListPath(req, func(nlri bgp.NLRI, paths []*apiutil.Path) {
		if prefix == "" || nlri.String() == prefix {
			n += len(paths)
		}
	})
	if err != nil {
		t.Fatalf("ListPath: %v", err)
	}
	return n
}
