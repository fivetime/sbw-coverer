package coverer

import (
	"context"
	"log/slog"
	"sync"

	"github.com/fivetime/sbw-contract/rpc"
	"google.golang.org/grpc"
)

// reportClient is the ONE long-lived client-stream wrapping rpc.ServerCoverer.Report —
// the single chokepoint through which EVERY coverer→server uplink (DEATH_VOTE,
// MEMBER_EDGE, AGENT_REPORT) flows. It stamps CovererId on every message.
//
// ***CRITICAL — CovererId is stamped HERE, unconditionally, on EVERY send.*** The
// sbw-server keys the FailoverQuorum on CovererId: a report with an empty CovererId
// collapses all K coverers' votes into ONE identity, so the quorum can never reach
// FailoverQuorum distinct votes and a real edge death never fails over. Stamping in the
// single Send (not at each call site) makes it impossible to forget.
type reportClient struct {
	self   string
	client rpc.ServerCovererClient
	log    *slog.Logger

	mu     sync.Mutex
	stream grpc.ClientStreamingClient[rpc.CovererReport, rpc.ReportAck]
	ctx    context.Context // the long-lived stream context (the process lifetime ctx)
}

// NewReportClient builds the manager. ctx is the process-lifetime context the Report
// stream rides on; a transport drop re-opens a fresh stream under it.
func NewReportClient(ctx context.Context, self string, client rpc.ServerCovererClient, log *slog.Logger) *reportClient {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &reportClient{self: self, client: client, log: log, ctx: ctx}
}

// ensureStream lazily (re)opens the Report stream. Caller holds rc.mu.
func (rc *reportClient) ensureStream() error {
	if rc.stream != nil {
		return nil
	}
	s, err := rc.client.Report(rc.ctx)
	if err != nil {
		return err
	}
	rc.stream = s
	return nil
}

// Send stamps CovererId=self and Sends r on the long-lived Report stream, re-opening it
// on a transport error. Serialized under rc.mu so concurrent emitters (the tap handler
// goroutine, the EOR snapshot, the agent-report hook) never interleave on one stream.
func (rc *reportClient) Send(r *rpc.CovererReport) error {
	r.CovererId = rc.self // ***the central, unconditional stamp***
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if err := rc.ensureStream(); err != nil {
		rc.log.Warn("report: open stream failed", "kind", r.Kind, "err", err)
		return err
	}
	if err := rc.stream.Send(r); err != nil {
		// Transport error → drop the stream so the next Send re-opens it.
		rc.stream = nil
		rc.log.Warn("report: send failed (will reconnect)", "kind", r.Kind, "edge", r.EdgeId, "err", err)
		return err
	}
	return nil
}

// Close flushes the stream at shutdown (best-effort; the ack is empty).
func (rc *reportClient) Close() {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if rc.stream != nil {
		_, _ = rc.stream.CloseAndRecv()
		rc.stream = nil
	}
}
