package coverer

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/fivetime/sbw-contract/model"
	"github.com/fivetime/sbw-contract/rpc"
	"github.com/fivetime/sbw-coverer/internal/grpcsrv"
)

// watch backoff bounds: capped exponential, reset on a clean Recv.
const (
	watchBackoffMin = 250 * time.Millisecond
	watchBackoffMax = 5 * time.Second
)

// edgeWorkerIdle is how long a per-edge relay worker may sit idle before it is reaped, so
// edge churn cannot leak goroutines: ONE worker per ACTIVE edge, lazily created and
// idle-reaped — never one per event.
const edgeWorkerIdle = 30 * time.Second

// edgeWorker is one edge's ordered relay queue. dispatch APPENDS directives (never drops,
// never blocks the consumer loop); the worker goroutine drains them FIFO into
// grpcsrv.Push*. This preserves per-edge FIFO ordering AND cross-edge concurrency.
// enqueue COALESCES the desired-state backlog so a wedged large agent cannot pile
// unbounded snapshots over grpcsrv's cap.
type edgeWorker struct {
	edge  model.EdgeID
	mu    sync.Mutex
	queue []*rpc.Directive
	wake  chan struct{}
}

// enqueue appends d under w.mu (held by the caller). A full DESIRED_STATE supersedes every
// queued desired-state directive before it (older snapshots + the deltas chaining onto
// them) — the agent converges identically by applying d alone. REHOME/FAILOVER/URGENT are
// independent of desired-state and preserved in order.
func (w *edgeWorker) enqueue(d *rpc.Directive) {
	if d.Kind == rpc.Directive_DESIRED_STATE && len(w.queue) > 0 {
		kept := make([]*rpc.Directive, 0, len(w.queue)+1)
		for _, q := range w.queue {
			if q.Kind != rpc.Directive_DESIRED_STATE && q.Kind != rpc.Directive_DESIRED_DELTA {
				kept = append(kept, q)
			}
		}
		w.queue = kept
	}
	w.queue = append(w.queue, d)
}

// watchConsumer is the COVERER-HALF demux: the single Watch-consuming goroutine routes
// each EDGE_DIRECTIVE to its per-edge worker. agents is the agent-facing transport
// relayDirective pushes below the seam.
type watchConsumer struct {
	cov     *Coverer
	agents  *grpcsrv.Server
	mu      sync.Mutex
	workers map[model.EdgeID]*edgeWorker
}

// runWatchClient is the load-bearing Watch CLIENT loop (the split's gRPC form of the
// monolith runWatchConsumer): it opens the server-stream rpc.ServerCoverer.Watch and
// reconnects on any transport drop with capped backoff. A reconnect re-Watches from
// scratch; the server re-sends COVERAGE + re-renders the covered edges (the
// initialCovererSync analog). Blocks until ctx is done.
func (c *Coverer) runWatchClient(ctx context.Context) {
	backoff := watchBackoffMin
	for {
		if ctx.Err() != nil {
			return
		}
		stream, err := c.client.Watch(ctx, &rpc.WatchRequest{
			CovererId:     c.self,
			SchemaVersion: int32(model.SchemaVersion),
		})
		if err != nil {
			c.log.Warn("watch: open stream failed; backing off", "err", err, "backoff", backoff)
			if !sleepCtx(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff)
			continue
		}
		wc := &watchConsumer{cov: c, agents: c.agents, workers: map[model.EdgeID]*edgeWorker{}}
		// Run a per-connection context so the edge workers are reaped on a stream drop.
		connCtx, cancel := context.WithCancel(ctx)
		for {
			a, err := stream.Recv()
			if err != nil {
				c.log.Warn("watch: recv failed; reconnecting", "err", err)
				break
			}
			backoff = watchBackoffMin // clean Recv → reset backoff
			switch a.Kind {
			case rpc.Assignment_COVERAGE:
				c.applyCoverage(connCtx, a.CoveredEdges)
			case rpc.Assignment_EDGE_DIRECTIVE:
				if a.Directive == nil {
					continue
				}
				wc.dispatch(connCtx, model.EdgeID(a.EdgeId), a.Directive)
			}
		}
		cancel() // drop this connection's edge workers
		if !sleepCtx(ctx, backoff) {
			return
		}
		backoff = nextBackoff(backoff)
	}
}

// applyCoverage is the LOAD-BEARING handler for a COVERAGE assignment (the monolith was an
// idempotent no-op): drive the tap to (de)tap so it peers with EXACTLY the server's
// covered-edge set. AddPeer for newly-covered edges, RemovePeer for dropped ones — the
// CoverageSink path, but driven by the SERVER's COVERAGE assignment instead of a local
// reconciler.
//
// A resolve() miss (an edge in the COVERAGE set but absent from cfg.BGP.Peers) is logged
// and SKIPPED inside the sink, leaving that edge un-tapped (no liveness for it) — the peer
// list must enumerate every edge the server could assign this coverer (DESIGN risk).
func (c *Coverer) applyCoverage(ctx context.Context, edges []string) {
	want := make([]model.EdgeID, len(edges))
	for i, e := range edges {
		want[i] = model.EdgeID(e)
	}
	if err := c.sink.Ensure(ctx, want); err != nil {
		c.log.Warn("apply coverage: tap (de)tap failed", "edges", len(want), "err", err)
	}
}

// dispatch hands a directive to its edge's worker, creating (and starting) the worker
// lazily. It appends under c.mu so it cannot race the worker's idle-reap into a lost
// directive. It never blocks on the worker's progress.
func (c *watchConsumer) dispatch(ctx context.Context, edge model.EdgeID, d *rpc.Directive) {
	c.mu.Lock()
	w := c.workers[edge]
	if w == nil {
		w = &edgeWorker{edge: edge, wake: make(chan struct{}, 1)}
		c.workers[edge] = w
		go c.runWorker(ctx, w)
	}
	w.mu.Lock()
	w.enqueue(d)
	w.mu.Unlock()
	c.mu.Unlock()
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// runWorker drains one edge's queue FIFO, relaying each directive below the seam. When the
// queue stays empty for edgeWorkerIdle it reaps the worker (removing it from the map under
// both locks so a concurrent dispatch re-creates a fresh one).
func (c *watchConsumer) runWorker(ctx context.Context, w *edgeWorker) {
	idle := time.NewTimer(edgeWorkerIdle)
	defer idle.Stop()
	for {
		w.mu.Lock()
		if len(w.queue) > 0 {
			d := w.queue[0]
			w.queue = w.queue[1:]
			w.mu.Unlock()
			c.relayDirective(w.edge, d)
			if !idle.Stop() {
				select {
				case <-idle.C:
				default:
				}
			}
			idle.Reset(edgeWorkerIdle)
			continue
		}
		w.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-w.wake:
			continue
		case <-idle.C:
			c.mu.Lock()
			w.mu.Lock()
			empty := len(w.queue) == 0
			if empty && c.workers[w.edge] == w {
				delete(c.workers, w.edge)
			}
			w.mu.Unlock()
			c.mu.Unlock()
			if empty {
				return
			}
			idle.Reset(edgeWorkerIdle)
		}
	}
}

// relayDirective unmarshals the typed model back out of the directive and calls the SAME
// grpcsrv.Push* the monolith called directly — so chunking, latest-wins coalescing,
// DESIRED_DELTA-drop→markResync and byte-budget backpressure all execute BELOW the seam
// exactly as before, making the agent-facing bytes identical. FAILOVER/URGENT and any
// stray DESIRED_STATE_CHUNK are relayed verbatim via PushDirective so all six Directive
// kinds survive the seam.
func (c *watchConsumer) relayDirective(edge model.EdgeID, d *rpc.Directive) {
	switch d.Kind {
	case rpc.Directive_DESIRED_STATE:
		var st model.EdgeDesiredState
		if err := json.Unmarshal(d.Payload, &st); err != nil {
			c.cov.log.Warn("seam relay: desired-state unmarshal failed", "edge", edge, "err", err)
			return
		}
		c.relayErr(edge, "desired-state", c.agents.PushDesired(edge, st))
	case rpc.Directive_DESIRED_DELTA:
		var dl model.EdgeDesiredDelta
		if err := json.Unmarshal(d.Payload, &dl); err != nil {
			c.cov.log.Warn("seam relay: desired-delta unmarshal failed", "edge", edge, "err", err)
			return
		}
		c.relayErr(edge, "desired-delta", c.agents.PushDelta(edge, dl))
	case rpc.Directive_REHOME:
		var a model.CovererAssignment
		if err := json.Unmarshal(d.Payload, &a); err != nil {
			c.cov.log.Warn("seam relay: rehome unmarshal failed", "edge", edge, "err", err)
			return
		}
		c.relayErr(edge, "rehome", c.agents.PushRehome(edge, a))
	default:
		// FAILOVER / URGENT / stray DESIRED_STATE_CHUNK: pass the raw payload through.
		c.relayErr(edge, "directive", c.agents.PushDirective(edge, d.Kind, d.Generation, d.Payload))
	}
}

// relayErr logs a below-seam push failure. ErrNotSubscribed is benign (the agent dropped
// its stream between the emit pre-check and here; it resyncs on its next Subscribe / the
// drift backstop recovers it).
func (c *watchConsumer) relayErr(edge model.EdgeID, kind string, err error) {
	if err != nil && err != grpcsrv.ErrNotSubscribed {
		c.cov.log.Warn("seam relay push failed", "edge", edge, "kind", kind, "err", err)
	}
}

// sleepCtx sleeps for d or returns false if ctx is cancelled first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// nextBackoff doubles d, capped at watchBackoffMax.
func nextBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > watchBackoffMax {
		d = watchBackoffMax
	}
	return d
}

// RunWatchClient is the exported entry the cmd starts in a goroutine.
func (c *Coverer) RunWatchClient(ctx context.Context) { c.runWatchClient(ctx) }
