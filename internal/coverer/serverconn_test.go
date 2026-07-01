package coverer

import (
	"log/slog"
	"testing"

	"github.com/fivetime/sbw-contract/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestServerConnRecreate proves Recreate() swaps the underlying ClientConn + client (forcing a
// fresh DNS resolution) while still satisfying the ServerCovererClient interface. Uses the
// passthrough scheme + lazy grpc.NewClient so no real server is needed.
func TestServerConnRecreate(t *testing.T) {
	sc, err := DialServer("passthrough:///dummy:1792", slog.New(slog.DiscardHandler),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("DialServer: %v", err)
	}
	defer func() { _ = sc.Close() }()

	var _ rpc.ServerCovererClient = sc // implements the interface (call sites unchanged)
	var _ recreatable = sc             // and the watch loop's self-heal hook

	conn1, cli1 := sc.conn, sc.client()
	if conn1 == nil || cli1 == nil {
		t.Fatal("nil conn/client after dial")
	}
	sc.Recreate()
	if sc.conn == conn1 {
		t.Fatal("Recreate did not swap the ClientConn (no fresh resolution)")
	}
	if sc.client() == cli1 {
		t.Fatal("Recreate did not swap the client")
	}
}
