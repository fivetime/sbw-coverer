package coverer

import (
	"context"
	"log/slog"
	"sync"

	"github.com/fivetime/sbw-contract/rpc"
	"google.golang.org/grpc"
)

// serverConn is a SELF-HEALING rpc.ServerCovererClient. It delegates every call to the CURRENT
// underlying *grpc.ClientConn's client, and Recreate() tears the conn down and re-dials — which
// forces a FRESH DNS resolution.
//
// This is the fix for the total-restart wedge: after a mass-restart (server pod replaced), the
// coverer's gRPC ClientConn can cache a server pod IP it resolved transiently DURING the churn
// (the old pod still terminating, still a ready endpoint for a moment) and then NEVER re-resolve
// to the replacement pod — so it dials a dead address forever ("no route to host" / conntrack
// to a gone pod → connection refused). Retrying the Watch RPC on that stuck conn never helps
// (backoff + keepalive just reconnect to the SAME stale address). Recreate() makes a brand-new
// grpc.NewClient which re-resolves the name and reaches the current server. Wrapping the client
// behind the SAME interface keeps every watch/report/register call site unchanged.
type serverConn struct {
	addr string
	opts []grpc.DialOption
	log  *slog.Logger

	mu    sync.RWMutex
	conn  *grpc.ClientConn
	inner rpc.ServerCovererClient
}

var _ rpc.ServerCovererClient = (*serverConn)(nil)

// DialServer builds a self-healing ServerCoverer client with an initial connection. opts are
// reused on every Recreate, so pass the transport creds / ConnectParams / keepalive here.
func DialServer(addr string, log *slog.Logger, opts ...grpc.DialOption) (*serverConn, error) {
	sc := &serverConn{addr: addr, opts: opts, log: log}
	if err := sc.redial(); err != nil {
		return nil, err
	}
	return sc, nil
}

func (sc *serverConn) redial() error {
	conn, err := grpc.NewClient(sc.addr, sc.opts...)
	if err != nil {
		return err
	}
	sc.mu.Lock()
	old := sc.conn
	sc.conn = conn
	sc.inner = rpc.NewServerCovererClient(conn)
	sc.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	return nil
}

// Recreate closes the current conn and re-dials (fresh DNS resolution). The watch loop calls it
// after persistent Watch-open failure, when the ClientConn is stuck on a stale/dead server
// address it won't re-resolve. Closing the conn also drops the Report stream, which the
// reportClient re-opens on its next Send through this same wrapper (now on the new conn).
func (sc *serverConn) Recreate() {
	sc.log.Warn("recreating server connection to force DNS re-resolution", "addr", sc.addr)
	if err := sc.redial(); err != nil {
		sc.log.Error("server connection recreate failed", "addr", sc.addr, "err", err)
	}
}

// Close closes the underlying conn (process shutdown).
func (sc *serverConn) Close() error {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.conn != nil {
		return sc.conn.Close()
	}
	return nil
}

func (sc *serverConn) client() rpc.ServerCovererClient {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.inner
}

func (sc *serverConn) Watch(ctx context.Context, in *rpc.WatchRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[rpc.Assignment], error) {
	return sc.client().Watch(ctx, in, opts...)
}

func (sc *serverConn) Report(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[rpc.CovererReport, rpc.ReportAck], error) {
	return sc.client().Report(ctx, opts...)
}

func (sc *serverConn) Register(ctx context.Context, in *rpc.RegisterRequest, opts ...grpc.CallOption) (*rpc.RegisterResponse, error) {
	return sc.client().Register(ctx, in, opts...)
}
