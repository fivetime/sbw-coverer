package grpcsrv

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/fivetime/sbw-contract/model"
	"github.com/fivetime/sbw-contract/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func dial(t *testing.T, s *Server) (rpc.AgentServiceClient, func()) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	rpc.RegisterAgentServiceServer(gs, s)
	go func() { _ = gs.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return rpc.NewAgentServiceClient(conn), func() { _ = conn.Close(); gs.Stop() }
}

func TestRegister(t *testing.T) {
	var gotEdge model.EdgeID
	var gotCap uint64
	s := New(WithRegister(func(_ context.Context, e model.EdgeID, c uint64) error {
		gotEdge, gotCap = e, c
		return nil
	}))
	cli, done := dial(t, s)
	defer done()

	resp, err := cli.Register(context.Background(), &rpc.RegisterRequest{
		EdgeId: "edge-2", CapacityBps: 100_000_000_000, SchemaVersion: model.SchemaVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Accepted || resp.SchemaVersion != model.SchemaVersion {
		t.Errorf("register response: %+v", resp)
	}
	if gotEdge != "edge-2" || gotCap != 100_000_000_000 {
		t.Errorf("onRegister got edge=%s cap=%d", gotEdge, gotCap)
	}
}

func TestRegisterSchemaMismatchRejected(t *testing.T) {
	s := New()
	cli, done := dial(t, s)
	defer done()
	resp, err := cli.Register(context.Background(), &rpc.RegisterRequest{EdgeId: "e", SchemaVersion: 999})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Accepted {
		t.Error("schema mismatch must be rejected")
	}
}

func TestSubscribePushDesired(t *testing.T) {
	s := New()
	cli, done := dial(t, s)
	defer done()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := cli.Subscribe(ctx, &rpc.SubscribeRequest{EdgeId: "edge-2"})
	if err != nil {
		t.Fatal(err)
	}

	// Wait for the server to register the subscription.
	waitFor(t, func() bool { return len(s.Subscribers()) == 1 })

	want := model.EdgeDesiredState{SchemaVersion: model.SchemaVersion, EdgeID: "edge-2", Generation: 9}
	if err := s.PushDesired("edge-2", want); err != nil {
		t.Fatalf("push: %v", err)
	}
	d, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	if d.Kind != rpc.Directive_DESIRED_STATE || d.Generation != 9 {
		t.Errorf("directive: kind=%v gen=%d", d.Kind, d.Generation)
	}
	var got model.EdgeDesiredState
	if err := json.Unmarshal(d.Payload, &got); err != nil {
		t.Fatal(err)
	}
	if got.EdgeID != "edge-2" || got.Generation != 9 {
		t.Errorf("payload = %+v", got)
	}
}

func TestRegisterReturnsCoverers(t *testing.T) {
	assign := model.CovererAssignment{
		EdgeID: "edge-2",
		Coverers: []model.Coverer{
			{ControllerID: "ctrl-a", GRPCEndpoint: "a:1791", Primary: true},
			{ControllerID: "ctrl-b", GRPCEndpoint: "b:1791"},
		},
	}
	s := New(WithCoverer(func(_ context.Context, e model.EdgeID) (model.CovererAssignment, bool, error) {
		if e != "edge-2" {
			return model.CovererAssignment{}, false, nil
		}
		return assign, true, nil
	}))
	cli, done := dial(t, s)
	defer done()

	resp, err := cli.Register(context.Background(), &rpc.RegisterRequest{EdgeId: "edge-2", SchemaVersion: model.SchemaVersion})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Coverers) == 0 {
		t.Fatal("expected coverers in register response")
	}
	var got model.CovererAssignment
	if err := json.Unmarshal(resp.Coverers, &got); err != nil {
		t.Fatal(err)
	}
	p, ok := got.Primary()
	if !ok || p.ControllerID != "ctrl-a" || p.GRPCEndpoint != "a:1791" {
		t.Errorf("primary = %+v ok=%v, want ctrl-a", p, ok)
	}
	if len(got.Fallbacks()) != 1 || got.Fallbacks()[0].ControllerID != "ctrl-b" {
		t.Errorf("fallbacks = %+v, want [ctrl-b]", got.Fallbacks())
	}
}

// A coverer-lookup error must NOT fail the registration (the agent stays put).
func TestRegisterCovererErrorIsNonFatal(t *testing.T) {
	s := New(WithCoverer(func(context.Context, model.EdgeID) (model.CovererAssignment, bool, error) {
		return model.CovererAssignment{}, false, errors.New("etcd down")
	}))
	cli, done := dial(t, s)
	defer done()
	resp, err := cli.Register(context.Background(), &rpc.RegisterRequest{EdgeId: "e", SchemaVersion: model.SchemaVersion})
	if err != nil {
		t.Fatalf("register must not fail on coverer error: %v", err)
	}
	if !resp.Accepted || len(resp.Coverers) != 0 {
		t.Errorf("want accepted with no coverers, got %+v", resp)
	}
}

func TestPushRehome(t *testing.T) {
	s := New()
	cli, done := dial(t, s)
	defer done()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := cli.Subscribe(ctx, &rpc.SubscribeRequest{EdgeId: "edge-2"})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(s.Subscribers()) == 1 })

	assign := model.CovererAssignment{EdgeID: "edge-2", Coverers: []model.Coverer{{ControllerID: "ctrl-z", GRPCEndpoint: "z:1791", Primary: true}}}
	if err := s.PushRehome("edge-2", assign); err != nil {
		t.Fatalf("push rehome: %v", err)
	}
	d, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if d.Kind != rpc.Directive_REHOME {
		t.Fatalf("kind = %v, want REHOME", d.Kind)
	}
	var got model.CovererAssignment
	if err := json.Unmarshal(d.Payload, &got); err != nil {
		t.Fatal(err)
	}
	if p, ok := got.Primary(); !ok || p.ControllerID != "ctrl-z" {
		t.Errorf("rehome primary = %+v, want ctrl-z", p)
	}
}

// TestPushCoalescesOnFullBufferNeverErrors asserts the scalability fix: when the
// agent's downlink buffer is full (slow consumer), PushDesired must NOT return
// ErrSlowConsumer. Instead it coalesces latest-wins, so a flood of pushes always
// succeeds and the agent eventually converges to the NEWEST desired state. This
// replaces the old "fail on slow consumer" behavior (which, under concurrent pool
// creates, collapsed the create rate via 409 → rollback → policer churn).
func TestPushCoalescesOnFullBufferNeverErrors(t *testing.T) {
	s := New()
	// Register a subscription WITHOUT a reader draining sb.ch, so the buffer fills.
	sb := &sub{ch: make(chan *rpc.Directive, pushBuffer), done: make(chan struct{}), wake: make(chan struct{}, 1), pending: map[rpc.Directive_Kind]*rpc.Directive{}}
	s.mu.Lock()
	s.subs["edge-x"] = sb
	s.mu.Unlock()

	// Push far more than the buffer depth. None may error (coalesce, never block).
	const n = pushBuffer + 500
	for i := 1; i <= n; i++ {
		st := model.EdgeDesiredState{SchemaVersion: model.SchemaVersion, EdgeID: "edge-x", Generation: uint64(i)}
		if err := s.PushDesired("edge-x", st); err != nil {
			t.Fatalf("push %d must not error under backpressure, got %v", i, err)
		}
	}

	// The newest snapshot (highest Generation) is what the slow send loop will take.
	d := sb.takePending()
	if d == nil {
		t.Fatal("expected a coalesced pending snapshot")
	}
	if d.Generation != uint64(n) {
		t.Errorf("coalesced pending Generation = %d, want newest %d (latest-wins)", d.Generation, n)
	}

	// And the classifier still recognizes the sentinel as non-fatal backpressure.
	if !s.IsBackpressure(ErrSlowConsumer) {
		t.Error("IsBackpressure(ErrSlowConsumer) must be true")
	}
	if s.IsBackpressure(errors.New("real failure")) {
		t.Error("IsBackpressure must be false for a genuine error")
	}
}

func TestPushToUnsubscribedEdge(t *testing.T) {
	s := New()
	if err := s.PushDesired("nobody", model.EdgeDesiredState{}); !errors.Is(err, ErrNotSubscribed) {
		t.Errorf("want ErrNotSubscribed, got %v", err)
	}
}

func TestReport(t *testing.T) {
	type reported struct {
		r   model.EdgeReport
		raw []byte
	}
	got := make(chan reported, 1)
	s := New(WithReport(func(_ context.Context, r model.EdgeReport, raw []byte) error {
		got <- reported{r, raw}
		return nil
	}))
	cli, done := dial(t, s)
	defer done()

	// A payload with a field this binary's model does NOT have (simulating a NEWER
	// agent/server contract than this relay's — e.g. a future fault_kind). The relay must
	// forward the bytes VERBATIM so the field is not stripped; the decoded struct drops it.
	payload := []byte(`{"schema_version":1,"edge_id":"edge-2","generation":3,` +
		`"health":{"edge_id":"edge-2","state":0},"unknown_future_field":"keepme"}`)
	if _, err := cli.Report(context.Background(), &rpc.ReportRequest{EdgeId: "edge-2", Generation: 3, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	select {
	case rr := <-got:
		if rr.r.EdgeID != "edge-2" || rr.r.Health.State != model.HealthHealthy {
			t.Errorf("onReport decoded %+v", rr.r)
		}
		// The decode necessarily lost the unknown field; the raw bytes MUST retain it so
		// the coverer relays it unchanged (the fault_kind-stripping bug fix).
		if !bytes.Contains(rr.raw, []byte("unknown_future_field")) {
			t.Fatalf("raw payload not passed verbatim (unknown field stripped): %s", rr.raw)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("onReport not called")
	}
}

// chunkTestState builds a snapshot whose member-bearing count exceeds n so
// PushDesired must chunk it.
func chunkTestState(n int) model.EdgeDesiredState {
	st := model.EdgeDesiredState{
		SchemaVersion: model.SchemaVersion, EdgeID: "edge-2", Generation: 77, DesiredVersion: 5,
	}
	for i := 0; i < n; i++ {
		st.Policers = append(st.Policers, model.PolicerSpec{
			Name: "p", PoolID: model.PoolID(i), Direction: model.DirectionIngress,
			Type: model.Policer1R2C, RateType: model.RateKbps, CIR: uint64(i),
			ConformAction: model.PolicerTransmit, ExceedAction: model.PolicerDrop,
		})
	}
	return st
}

// TestPushDesiredBelowThresholdSingleMessage asserts the non-chunked path is
// unchanged: a small state arrives as exactly ONE plain DESIRED_STATE.
func TestPushDesiredBelowThresholdSingleMessage(t *testing.T) {
	s := New(WithChunkMembers(50_000))
	cli, done := dial(t, s)
	defer done()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := cli.Subscribe(ctx, &rpc.SubscribeRequest{EdgeId: "edge-2"})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(s.Subscribers()) == 1 })

	want := chunkTestState(10) // well below threshold
	if err := s.PushDesired("edge-2", want); err != nil {
		t.Fatalf("push: %v", err)
	}
	d, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if d.Kind != rpc.Directive_DESIRED_STATE {
		t.Fatalf("expected single DESIRED_STATE, got %v", d.Kind)
	}
	if d.Generation != want.Generation {
		t.Fatalf("generation %d != %d", d.Generation, want.Generation)
	}
}

// TestPushDesiredChunksLargeState drives a large state through PushDesired and
// reassembles the DESIRED_STATE_CHUNK sequence on the client, asserting the result is
// byte-identical to a single-message marshal of the original and the echoed
// generation is preserved.
func TestPushDesiredChunksLargeState(t *testing.T) {
	const chunkSize = 32
	s := New(WithChunkMembers(chunkSize))
	cli, done := dial(t, s)
	defer done()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := cli.Subscribe(ctx, &rpc.SubscribeRequest{EdgeId: "edge-2"})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(s.Subscribers()) == 1 })

	want := chunkTestState(200) // 200 / 32 → 7 chunks
	if err := s.PushDesired("edge-2", want); err != nil {
		t.Fatalf("push: %v", err)
	}

	var buf []model.EdgeDesiredStateChunk
	for {
		d, err := stream.Recv()
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		if d.Kind != rpc.Directive_DESIRED_STATE_CHUNK {
			t.Fatalf("expected DESIRED_STATE_CHUNK, got %v", d.Kind)
		}
		if d.Generation != want.Generation {
			t.Fatalf("chunk generation %d != snapshot %d", d.Generation, want.Generation)
		}
		var ch model.EdgeDesiredStateChunk
		if err := json.Unmarshal(d.Payload, &ch); err != nil {
			t.Fatal(err)
		}
		if ch.Epoch != want.Generation {
			t.Fatalf("chunk Epoch %d != %d", ch.Epoch, want.Generation)
		}
		buf = append(buf, ch)
		if ch.Last {
			break
		}
	}
	if len(buf) <= 1 {
		t.Fatalf("expected multiple chunks, got %d", len(buf))
	}

	got := model.AssembleChunks(buf)
	gotJSON, _ := json.Marshal(got)
	wantJSON, _ := json.Marshal(want)
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("reassembled != original snapshot")
	}
	if got.Generation != want.Generation {
		t.Fatalf("reassembled generation %d != %d", got.Generation, want.Generation)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	for i := 0; i < 100; i++ {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}
