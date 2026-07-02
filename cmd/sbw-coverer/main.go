// Command sbw-coverer is the SBW control plane's SHARDED sensor + actuator
// (DESIGN-server-coverer-split §8): over its K-covered edges it runs the GoBGP RIB-tap +
// RIB-survival guard and serves desired-state to its L/R agents (rpc.AgentService) — but
// it ONLY watches sbw-server (rpc.ServerCoverer client) and NEVER touches YugabyteDB/etcd,
// so coverers scale with the edge count without fanning store connections out.
//
// It DIALS the server (Watch coverage + per-edge Directive; Report votes/member-edge/agent
// reports up; relay Register), drives the tap to (de)tap exactly the edges the server's
// COVERAGE assignment names, and stamps CovererId on every report (the server keys the
// FailoverQuorum on it). It runs NONE of the server-only machinery (no etcd/YB client, no
// orchestrator/liveness/coverage assigner/ctrlreg/deathvote/edgever).
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fivetime/sbw-contract/buildinfo"
	"github.com/fivetime/sbw-contract/logx"
	"github.com/fivetime/sbw-contract/metrics"
	"github.com/fivetime/sbw-contract/model"
	"github.com/fivetime/sbw-contract/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/fivetime/sbw-coverer/internal/coverer"
	"github.com/fivetime/sbw-coverer/internal/guard"
	"github.com/fivetime/sbw-coverer/internal/ribtap"
)

func main() {
	cfgPath := flag.String("config", "", "path to JSON config file (optional; env overrides apply)")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println(buildinfo.String())
		return
	}

	cfg, cfgErr := coverer.LoadConfig(*cfgPath)

	log, err := logx.New(cfg.Log, os.Stderr)
	if err != nil {
		log = logx.Default()
		log.Warn("invalid log config; falling back to defaults", "err", err)
	}
	if cfgErr != nil {
		log.Error("configuration error", "err", cfgErr)
		os.Exit(1)
	}

	// self = WatchRequest.CovererId; the server keys the FailoverQuorum on it. RouterID is
	// validated non-empty in Config.Validate, so this is never empty.
	self := cfg.Sharding.ResolveReplicaID(cfg.BGP.RouterID)
	if self == "" {
		log.Error("coverer id empty (no shard key) — failover quorum would collapse; set bgp.router_id / sharding.replica_id")
		os.Exit(1)
	}

	log.Info("sbw-coverer starting",
		"version", buildinfo.Version,
		"component", "coverer",
		"coverer_id", self,
		"server_addr", cfg.ServerAddr,
		"agent_grpc_listen", cfg.AgentGRPCListenAddr,
		"bgp_listen_port", cfg.BGP.ListenPort,
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	met := metrics.New()

	// Dial the sbw-server (rpc.ServerCoverer). NO etcd / YugabyteDB client is ever opened
	// — that is the whole point of the split.
	//
	// ConnectParams CAP the reconnect backoff at 5s (gRPC default MaxDelay is 120s): after a
	// total control-plane restart the server pod is replaced, and a bare dial's ClientConn can
	// back off up to ~2min before retrying — so the coverer's Watch/Report stay wedged (the
	// app-level 5s Watch backoff is moot when the underlying ClientConn is in a 120s backoff)
	// until a manual pod restart resets it. Capping it lets the coverer re-establish within ~5s
	// of the server becoming reachable.
	// DialServer wraps the ClientConn in a self-healing client (serverConn): the watch loop
	// Recreate()s it on persistent failure to force a FRESH DNS resolution — the real fix for
	// the total-restart wedge (a stuck ClientConn caching a stale/dead server pod IP). The
	// backoff cap + keepalive below help the common cases; Recreate is the backstop.
	client, err := coverer.DialServer(cfg.ServerAddr, log,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  250 * time.Millisecond,
				Multiplier: 1.6,
				Jitter:     0.2,
				MaxDelay:   5 * time.Second,
			},
			MinConnectTimeout: 5 * time.Second,
		}),
		// Keepalive is what UN-WEDGES a total-restart: the Watch server-stream idles between
		// pushes, and a half-open ClientConn (Watch created but never truly connected — server
		// never got it) leaves the coverer blocked in stream.Recv() forever with no error to
		// break the loop and retry. Pinging every 20s (Timeout 10s) detects the dead transport,
		// closes it → Recv errors → the watch loop reconnects. PermitWithoutStream so the ping
		// runs even while the conn is (from gRPC's view) streamless. 20s > server MinTime 10s,
		// so no GOAWAY.
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                20 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		log.Error("dial sbw-server failed", "addr", cfg.ServerAddr, "err", err)
		os.Exit(1)
	}
	defer func() { _ = client.Close() }()

	// The long-lived Report client-stream (stamps CovererId=self on every send).
	rc := coverer.NewReportClient(ctx, self, client, log)

	// RIB-survival guard (§6.4/§7): the lossy /32 mirror, with the same conflict / view-
	// freeze alarms the monolith wired.
	canaryLC := model.LargeCommunity{
		GlobalAdmin: cfg.BGP.Canary.GlobalAdmin,
		LocalData1:  cfg.BGP.Canary.LocalData1,
		LocalData2:  cfg.BGP.Canary.LocalData2,
	}
	g := guard.New(canaryLC,
		guard.WithConflictHandler(func(c guard.Conflict, resolved bool) {
			met.GuardConflict(resolved)
			if resolved {
				log.Info("advertisement conflict resolved", "conflict", c)
			} else {
				log.Warn("multi-sourced member prefix (unique-advert conflict)", "conflict", c)
			}
		}),
		guard.WithViewChangeHandler(func(edge model.EdgeID, family model.Family, valid bool) {
			met.ViewFrozen(edge, family, valid)
			if valid {
				log.Info("RIB-survival view thawed (trustworthy again)", "edge", edge, "family", family)
			} else {
				log.Warn("RIB-survival view FROZEN (absence untrustworthy; withdrawals held)", "edge", edge, "family", family)
			}
		}),
	)

	// The embedded GoBGP RIB tap. ActiveDial ALWAYS true — the coverer INITIATES sessions
	// for the edges the server assigns it via COVERAGE.
	srv, err := ribtap.NewServer(ribtap.Config{
		ASN:           cfg.BGP.ASN,
		RouterID:      cfg.BGP.RouterID,
		ListenPort:    int32(cfg.BGP.ListenPort),
		ListenAddr:    cfg.BGP.ListenAddresses,
		BFDEnabled:    cfg.BGP.BFDEnabled,
		BFDTxMs:       cfg.BGP.BFDTxMs,
		BFDRxMs:       cfg.BGP.BFDRxMs,
		BFDMultiplier: cfg.BGP.BFDMultiplier,
		BFDMultihop:   cfg.BGP.BFDMultihop,
		BindInterface: cfg.BGP.BindInterface,
		ActiveDial:    true,
	}, log)
	if err != nil {
		log.Error("RIB tap build failed", "err", err)
		os.Exit(1)
	}
	if err := srv.Start(ctx); err != nil {
		log.Error("RIB tap start failed", "err", err)
		os.Exit(1)
	}

	// Edge → dial-target resolver from the static peer list; applyCoverage drives the sink
	// to peer with exactly the server's covered set.
	peerByEdge := make(map[model.EdgeID]ribtap.Peer, len(cfg.BGP.Peers))
	for _, p := range cfg.BGP.Peers {
		peerByEdge[model.EdgeID(p.Edge)] = ribtap.Peer{NeighborAddress: p.Address, PeerASN: p.ASN}
	}
	resolve := func(e model.EdgeID) (ribtap.Peer, bool) { p, ok := peerByEdge[e]; return p, ok }
	sink := ribtap.NewCoverageSink(srv, resolve)

	cov := coverer.New(self, cfg.AgentAdvertiseAddr, client, g, srv, sink, rc, met, canaryLC, log)

	// Drive the tap producer into the guard + Report fusion + the periodic adj-in
	// reconciliation (T-609; re-points off srv.Peers() not a server-side registry).
	producer := ribtap.NewProducer(srv)
	go func() {
		if err := cov.RunTap(ctx, producer); err != nil && ctx.Err() == nil {
			// The tap feeds liveness/PeerDown detection for the edges THIS coverer covers —
			// losing it silently breaks failover for them. FATAL so an orchestrator restarts.
			log.Error("RIB tap producer stopped (fatal)", "err", err)
			os.Exit(1)
		}
	}()
	go cov.RunReconcileTapView(ctx, producer, 60*time.Second)

	// Serve the agent-facing AgentService (L/R agents Register/Subscribe/Report here).
	gs := grpc.NewServer()
	rpc.RegisterAgentServiceServer(gs, cov.Agents())
	lis, err := net.Listen("tcp", cfg.AgentGRPCListenAddr)
	if err != nil {
		log.Error("agent grpc listen failed", "addr", cfg.AgentGRPCListenAddr, "err", err)
		os.Exit(1)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- gs.Serve(lis) }()

	// The load-bearing Watch client: applyCoverage drives the tap, the directive demux
	// relays desired-state to agents byte-identically.
	go cov.RunWatchClient(ctx)

	// Prometheus /metrics.
	if cfg.MetricsListenAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", met.Handler())
		msrv := &http.Server{Addr: cfg.MetricsListenAddr, Handler: mux}
		go func() {
			if err := msrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Error("metrics server stopped", "err", err)
			}
		}()
		defer func() { _ = msrv.Close() }()
		log.Info("metrics serving", "addr", cfg.MetricsListenAddr, "path", "/metrics")
	}

	log.Info("sbw-coverer running; agents may register/subscribe. Send SIGTERM/SIGINT to stop.")
	select {
	case <-ctx.Done():
		log.Info("sbw-coverer received shutdown signal; stopping")
	case err := <-serveErr:
		if err != nil {
			log.Error("agent grpc server stopped", "err", err)
			os.Exit(1)
		}
	}
	gs.GracefulStop()
	rc.Close()
	_ = srv.Stop(context.Background())
}
