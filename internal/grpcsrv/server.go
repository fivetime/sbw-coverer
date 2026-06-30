// Package grpcsrv is the controller side of the controller↔agent gRPC transport
// (T-704/705, controller §8.2). It implements rpc.AgentService: agents Register,
// open a Subscribe server-stream (the controller pushes desired state + failover/
// urgent directives down it), and Report up. The controller never dials an
// agent — "push to a specific agent" means writing that agent's open stream.
//
// Payloads are JSON of the frozen S-04 model (EdgeDesiredState down, EdgeReport
// up); the server transports bytes and routes, the controller logic decides.
package grpcsrv

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/fivetime/sbw-contract/model"
	"github.com/fivetime/sbw-contract/rpc"
)

// ErrNotSubscribed is returned when no agent stream is open for the edge (it hasn't Subscribed
// or dropped). The caller (reconcile loop) retries on the next pass.
var ErrNotSubscribed = errors.New("grpcsrv: edge not subscribed")

// ErrSlowConsumer is no longer returned by the push path: a full buffer now
// COALESCES (latest-wins) instead of erroring, so PushDesired never fails because
// of a slow consumer. It is retained as an exported sentinel for any caller that
// classified it, and so a buffer-full condition can still be surfaced/logged
// without being fatal. Desired state is a FULL generation-versioned snapshot, so
// keeping only the newest pending snapshot is correct — the agent converges to it.
var ErrSlowConsumer = errors.New("grpcsrv: agent push buffer full")

// pushBuffer is the per-agent directive channel depth. It is large so that bursts
// of concurrent pool creates (each fanning out a full desired-state snapshot) ride
// in the buffer rather than backpressuring the create path. On the rare occasion
// the buffer still fills, the push path coalesces the newest DESIRED_STATE snapshot
// (see latest-wins slot in sub) instead of blocking or erroring.
const pushBuffer = 4096

// maxQueueBytes caps the per-agent queued payload bytes (the directives sitting in
// ch). pushBuffer alone is a COUNT bound, but a chunked huge edge buffers up to ~32MB
// per directive, so 4096 directives could pin >100GB if the agent is slow and the
// controller keeps re-pushing (observed: ctrl RSS → 86GiB at 350K with a backup-skewed
// edge re-pushing a ~450MB snapshot every audit cycle). Bounding by BYTES caps memory
// regardless of directive size; an enqueue that would exceed it takes the overflow
// path. Sized to hold one full healthy-edge snapshot without churning.
const maxQueueBytes = 512 << 20 // 512 MiB

// defaultChunkMembers is the default member-bearing entries per DESIRED_STATE chunk.
// A full snapshot whose member-bearing entries (policers + classify + anchors +
// flow-redirects + legacy ABF/uRPF) exceed this is streamed as DESIRED_STATE_CHUNK
// directives instead of one DESIRED_STATE message. ~50k entries keeps each chunk well
// under the gRPC recv cap (each entry is a few hundred bytes of JSON → ≤~32MB/chunk,
// far below the 512MB net), so a multi-million-member edge ships as a sequence of
// modest messages. Below the threshold the snapshot is sent as ONE plain
// DESIRED_STATE, byte-for-byte unchanged from the pre-chunking path.
const defaultChunkMembers = 50_000

// RegisterFunc handles an agent registration (e.g. ledger.InitAgent + registry).
type RegisterFunc func(ctx context.Context, edge model.EdgeID, capacityBps uint64) error

// ReportFunc handles an agent's uplink EdgeReport (soft-death fusion, capacity).
type ReportFunc func(ctx context.Context, report model.EdgeReport) error

// SubscribeFunc is called when an agent opens its downlink stream, so the
// controller can push the agent's CURRENT desired state right away — the
// re-convergence sync that lets an agent reconnecting to a fresh stateless
// replica recover its state without waiting for the next pool event (§1.2).
type SubscribeFunc func(edge model.EdgeID)

// ResyncFunc is called when the push path had to DROP pending DESIRED_DELTA
// directives under buffer overflow (incremental hot-path scalability, fix #4):
// deltas are NOT coalescible (each pool change matters), so on overflow the server
// discards the pending deltas and asks the controller to render+push a single FULL
// DESIRED_STATE snapshot instead — never losing state silently. Wire it to the same
// full re-render the on-connect sync uses (orchestrator.RerenderEdge). The agent's
// generation-gap detection is the independent backstop.
type ResyncFunc func(edge model.EdgeID)

// CovererFunc computes an edge's coverer assignment at registration so the
// controller can tell the agent its primary/fallback coverers (L-05/L-06). ok
// is false when sharding is off — the agent then just stays on whichever
// controller it reached. Returns an error only on a backend failure.
type CovererFunc func(ctx context.Context, edge model.EdgeID) (model.CovererAssignment, bool, error)

// RegisterFullFunc handles a registration END TO END — the authoritative register AND the
// coverer-assignment reply in one call — so the whole registration is routed through the
// scvr seam (DESIGN-server-coverer-split §8 step3). When set it SUPERSEDES the onRegister +
// onCoverer pair: the coverer relays the agent's RegisterRequest up and the server replies
// with accepted + the coverer set.
type RegisterFullFunc func(ctx context.Context, req *rpc.RegisterRequest) (*rpc.RegisterResponse, error)

// Server implements rpc.AgentServiceServer and a push API for the controller.
type Server struct {
	rpc.UnimplementedAgentServiceServer

	onRegister     RegisterFunc
	onReport       ReportFunc
	onSubscribe    SubscribeFunc
	onResync       ResyncFunc
	onCoverer      CovererFunc
	onRegisterFull RegisterFullFunc
	log            *slog.Logger

	// chunkMembers is the member-bearing entries per DESIRED_STATE chunk. A snapshot
	// exceeding it is streamed as DESIRED_STATE_CHUNK directives; at or below it goes
	// as one plain DESIRED_STATE (unchanged). 0 means defaultChunkMembers.
	chunkMembers int

	mu   sync.Mutex
	subs map[model.EdgeID]*sub
}

// sub is one agent's open downlink. done is closed to supersede it; the data
// channel is never closed (so a concurrent push never sends on a concurrent send).
//
// pending is the latest-wins coalescing area used when the buffered channel is
// full. It is keyed by directive Kind: a DESIRED_STATE is a FULL snapshot that
// supersedes the previous one, so the slow send path only ever needs the NEWEST per
// kind — push replaces a staler same-kind directive (ordered by Generation) and
// drops it. Keying by Kind keeps a rare REHOME / urgent directive from being
// starved by a DESIRED_STATE flood (and vice versa): each kind retains its own
// newest. This makes the push path non-fatal under backpressure while still
// converging the agent to the latest desired state. wake is a depth-1 channel used
// purely as an edge-trigger that a pending directive is available.
//
// DESIRED_DELTA is the exception that the latest-wins rule must NOT touch: each
// delta carries a DIFFERENT set of pool changes, so keeping only the newest would
// silently lose the others. So a delta is NEVER stashed in pending; if it can't be
// buffered it is DROPPED and needResync is set — the next take/send path enqueues a
// single full DESIRED_STATE resync via onResync (fix #4). The agent's
// generation-gap detection is the independent backstop if even that is missed.
type sub struct {
	ch          chan *rpc.Directive
	done        chan struct{}
	queuedBytes atomic.Int64  // payload bytes currently queued in ch (capped at maxQueueBytes)
	drain       chan struct{} // depth-1 wake: send loop freed queue room (chunk backpressure)

	pmu        sync.Mutex
	pending    map[rpc.Directive_Kind]*rpc.Directive // newest coalesced directive per kind
	needResync bool                                  // dropped deltas under overflow → owe a full snapshot
	wake       chan struct{}                         // depth-1 edge-trigger for pending
}

// takePending atomically removes and returns the highest-Generation coalesced
// directive across all kinds, if any. Returning one at a time (the send loop loops
// back via wake while any remain) keeps the per-kind newest each delivered.
func (sb *sub) takePending() *rpc.Directive {
	sb.pmu.Lock()
	defer sb.pmu.Unlock()
	var pick rpc.Directive_Kind
	var best *rpc.Directive
	for k, d := range sb.pending {
		if best == nil || d.Generation > best.Generation {
			best, pick = d, k
		}
	}
	if best != nil {
		delete(sb.pending, pick)
		// More remain → re-arm the trigger so the loop drains them too.
		if len(sb.pending) > 0 {
			select {
			case sb.wake <- struct{}{}:
			default:
			}
		}
	}
	return best
}

// stashPending stores d as the pending directive for its kind, keeping the one with
// the higher Generation (newer wins; the stale one is dropped). It then non-
// blockingly wakes the send loop. Used only when the buffered channel is full.
func (sb *sub) stashPending(d *rpc.Directive) {
	sb.pmu.Lock()
	if cur, ok := sb.pending[d.Kind]; !ok || d.Generation >= cur.Generation {
		sb.pending[d.Kind] = d
	}
	sb.pmu.Unlock()
	select {
	case sb.wake <- struct{}{}:
	default:
	}
}

// markResync records that a DESIRED_DELTA had to be dropped under overflow, so the
// agent now owes a full DESIRED_STATE snapshot (fix #4). It also clears any pending
// DESIRED_DELTA — a coming full snapshot supersedes every dropped/queued delta, so
// delivering stale deltas afterward would be wasted work (and harmless but pointless
// generation churn). Wakes the send loop so the resync is enqueued promptly.
func (sb *sub) markResync() {
	sb.pmu.Lock()
	sb.needResync = true
	delete(sb.pending, rpc.Directive_DESIRED_DELTA)
	sb.pmu.Unlock()
	select {
	case sb.wake <- struct{}{}:
	default:
	}
}

// takeNeedResync atomically reads-and-clears the resync-owed flag. The send loop
// calls it; if set, it invokes onResync to render+push a full DESIRED_STATE.
func (sb *sub) takeNeedResync() bool {
	sb.pmu.Lock()
	defer sb.pmu.Unlock()
	r := sb.needResync
	sb.needResync = false
	return r
}

// Option configures a Server.
type Option func(*Server)

// WithRegister wires the registration handler.
func WithRegister(fn RegisterFunc) Option { return func(s *Server) { s.onRegister = fn } }

// WithReport wires the uplink report handler.
func WithReport(fn ReportFunc) Option { return func(s *Server) { s.onReport = fn } }

// WithOnSubscribe wires the on-connect hook (initial desired-state sync).
func WithOnSubscribe(fn SubscribeFunc) Option { return func(s *Server) { s.onSubscribe = fn } }

// WithOnResync wires the dropped-delta resync hook (fix #4): called when the push
// path discarded pending DESIRED_DELTA directives under overflow, to render+push a
// full DESIRED_STATE snapshot. Wire to orchestrator.RerenderEdge (same full render
// as the on-connect sync). nil means deltas dropped under overflow are recovered
// only by the agent's generation-gap detection / the periodic anti-drift snapshot.
func WithOnResync(fn ResyncFunc) Option { return func(s *Server) { s.onResync = fn } }

// IsSubscribed reports whether an agent currently has an open downlink stream for
// the edge — i.e. it homed HERE and is reporting to THIS replica. Under sharding
// an edge is tap-covered by K replicas but reports (heartbeats) to only ONE; the
// others must not read "no heartbeat to me" as agent death — that path falsely
// failed healthy edges in the K=2 e2e (a re-homed agent leaves the old coverer
// heartbeat-silent). The liveness heartbeat-stale path consults this gate.
func (s *Server) IsSubscribed(edge model.EdgeID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.subs[edge]
	return ok
}

// WithCoverer wires the coverer-assignment hook (sharding, L-05). Settable post-
// construction via SetCoverer too, since the reconciler is built after the server.
func WithCoverer(fn CovererFunc) Option { return func(s *Server) { s.onCoverer = fn } }

// SetCoverer installs (or clears) the coverer-assignment hook after construction
// — used to wire sharding once the coverage reconciler exists (cmd startSharding).
func (s *Server) SetCoverer(fn CovererFunc) {
	s.mu.Lock()
	s.onCoverer = fn
	s.mu.Unlock()
}

// SetRegisterFull installs (or clears) the END-TO-END registration hook after
// construction — used to route the WHOLE registration (register + coverer reply) through
// the scvr seam once the ControlPlane's server-half exists. When set, Register delegates
// to it entirely (the onRegister/onCoverer pair is bypassed).
func (s *Server) SetRegisterFull(fn RegisterFullFunc) {
	s.mu.Lock()
	s.onRegisterFull = fn
	s.mu.Unlock()
}

// WithLogger sets the logger.
func WithLogger(l *slog.Logger) Option { return func(s *Server) { s.log = l } }

// WithChunkMembers sets the member-bearing entries per DESIRED_STATE chunk (the
// threshold above which a full snapshot is streamed as DESIRED_STATE_CHUNK rather than
// one DESIRED_STATE). <=0 keeps the default (defaultChunkMembers). Configurable so an
// operator can tune the chunk size to the deployment's member density / link MTU.
func WithChunkMembers(n int) Option { return func(s *Server) { s.chunkMembers = n } }

// New builds a server.
func New(opts ...Option) *Server {
	s := &Server{subs: map[model.EdgeID]*sub{}, log: slog.New(slog.DiscardHandler)}
	for _, o := range opts {
		o(s)
	}
	if s.chunkMembers <= 0 {
		s.chunkMembers = defaultChunkMembers
	}
	return s
}

// Register accepts an agent and its capacity (idempotent by edge_id).
func (s *Server) Register(ctx context.Context, req *rpc.RegisterRequest) (*rpc.RegisterResponse, error) {
	s.mu.Lock()
	full := s.onRegisterFull
	s.mu.Unlock()
	if full != nil {
		// §8 step3: the whole registration rides the scvr seam — the server-half does the
		// authoritative register + coverer assignment and returns the reply verbatim.
		return full(ctx, req)
	}
	if req.SchemaVersion != 0 && int(req.SchemaVersion) != model.SchemaVersion {
		return &rpc.RegisterResponse{SchemaVersion: model.SchemaVersion, Accepted: false}, nil
	}
	if s.onRegister != nil {
		if err := s.onRegister(ctx, model.EdgeID(req.EdgeId), req.CapacityBps); err != nil {
			return nil, err
		}
	}
	resp := &rpc.RegisterResponse{SchemaVersion: model.SchemaVersion, Accepted: true}

	// Tell the agent its coverers (sharding, L-06): primary to report to, rest as
	// fallback. A coverer-lookup failure must not fail the registration — the
	// agent stays on the controller it reached and gets re-homed by the next
	// REHOME push; we just log it.
	s.mu.Lock()
	coverer := s.onCoverer
	s.mu.Unlock()
	if coverer != nil {
		if a, ok, err := coverer(ctx, model.EdgeID(req.EdgeId)); err != nil {
			s.log.Warn("coverer assignment failed at register", "edge", req.EdgeId, "err", err)
		} else if ok {
			if b, err := json.Marshal(a); err == nil {
				resp.Coverers = b
			}
		}
	}
	s.log.Info("agent registered", "edge", req.EdgeId, "capacity_bps", req.CapacityBps)
	return resp, nil
}

// Subscribe is the downlink stream. The agent calls it once; the controller
// pushes directives via PushDesired/PushDirective until the stream closes.
func (s *Server) Subscribe(req *rpc.SubscribeRequest, stream rpc.AgentService_SubscribeServer) error {
	edge := model.EdgeID(req.EdgeId)
	sb := &sub{ch: make(chan *rpc.Directive, pushBuffer), done: make(chan struct{}), wake: make(chan struct{}, 1), drain: make(chan struct{}, 1), pending: map[rpc.Directive_Kind]*rpc.Directive{}}

	s.mu.Lock()
	if old, ok := s.subs[edge]; ok {
		close(old.done) // a re-subscribe supersedes the previous stream
	}
	s.subs[edge] = sb
	s.mu.Unlock()
	s.log.Info("agent subscribed", "edge", edge)

	// Initial state sync: push the agent's current desired state now that its
	// stream is registered. Async so a slow render never blocks the stream; the
	// push lands on sb.ch which the loop below is about to read.
	if s.onSubscribe != nil {
		go s.onSubscribe(edge)
	}

	defer func() {
		s.mu.Lock()
		if s.subs[edge] == sb {
			delete(s.subs, edge)
			close(sb.done) // release any backpressured chunk push; a superseding Subscribe already closed it
		}
		s.mu.Unlock()
		s.log.Info("agent unsubscribed", "edge", edge)
	}()

	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sb.done:
			return nil // superseded by a newer Subscribe
		case d := <-sb.ch:
			sb.queuedBytes.Add(-int64(len(d.Payload)))
			select { // wake a backpressured chunk push — queue room just freed
			case sb.drain <- struct{}{}:
			default:
			}
			if err := stream.Send(d); err != nil {
				return err
			}
		case <-sb.wake:
			// A coalesced (latest-wins) snapshot was stashed because the channel was
			// full, and/or deltas were dropped (needResync). Resync first: if deltas
			// were lost, ask the controller to render+push a full DESIRED_STATE — that
			// snapshot supersedes every dropped delta (fix #4). Async so a slow render
			// never blocks the stream; the push lands back on sb.ch / pending.
			if sb.takeNeedResync() {
				s.mu.Lock()
				resync := s.onResync
				s.mu.Unlock()
				if resync != nil {
					go resync(edge)
				}
			}
			// Then send the newest coalesced snapshot. The buffered channel drains
			// concurrently; taking pending here guarantees the agent eventually
			// receives the latest desired state even under sustained backpressure.
			if d := sb.takePending(); d != nil {
				if err := stream.Send(d); err != nil {
					return err
				}
			}
		}
	}
}

// Report receives an agent's uplink EdgeReport.
func (s *Server) Report(ctx context.Context, req *rpc.ReportRequest) (*rpc.ReportAck, error) {
	var report model.EdgeReport
	if err := json.Unmarshal(req.Payload, &report); err != nil {
		return nil, err
	}
	if s.onReport != nil {
		if err := s.onReport(ctx, report); err != nil {
			return nil, err
		}
	}
	return &rpc.ReportAck{}, nil
}

// PushDesired sends an EdgeDesiredState to the edge's open stream.
//
// CHUNKING: if the snapshot's member-bearing entries (policers + classify + anchors +
// flow-redirects + legacy ABF/uRPF) exceed the chunk threshold, it is split into K
// disjoint fragments and streamed as DESIRED_STATE_CHUNK directives (one snapshot =
// one Epoch = state.Generation, Seq 0..K-1, Last on the final). The agent reassembles
// and applies them as ONE atomic state, byte-equivalent to the single message, then
// echoes state.Generation — identical to the non-chunked path. At or below the
// threshold the snapshot is sent as ONE plain DESIRED_STATE, byte-for-byte unchanged.
//
// The chunk directives are pushed in Seq order through the same push() path. They are
// NOT coalescible (see push): if the buffer fills mid-sequence the partial is dropped
// and a fresh full resync is owed (markResync → onResync, new Epoch). PushDesired
// returns the first push error (e.g. ErrNotSubscribed); a backpressure drop is
// swallowed by push (returns nil) exactly as for a single DESIRED_STATE.
func (s *Server) PushDesired(edge model.EdgeID, state model.EdgeDesiredState) error {
	if memberCount(state) <= s.chunkMembers {
		// Small edge: ONE plain DESIRED_STATE, identical to the pre-chunking path.
		payload, err := json.Marshal(state)
		if err != nil {
			return err
		}
		return s.push(edge, &rpc.Directive{
			Kind: rpc.Directive_DESIRED_STATE, Generation: state.Generation, Payload: payload,
		})
	}

	// Large edge: stream as DESIRED_STATE_CHUNK fragments. Epoch == state.Generation.
	chunks := model.SplitDesiredState(state, s.chunkMembers)
	s.log.Info("chunking full desired state",
		"edge", edge, "generation", state.Generation, "members", memberCount(state), "chunks", len(chunks))
	for i := range chunks {
		payload, err := json.Marshal(chunks[i])
		if err != nil {
			return err
		}
		if err := s.push(edge, &rpc.Directive{
			Kind: rpc.Directive_DESIRED_STATE_CHUNK, Generation: state.Generation, Payload: payload,
		}); err != nil {
			// ErrNotSubscribed (stream gone) — stop streaming the rest; the agent never
			// sees a Last for this Epoch so it applies nothing (no partial) and resyncs on
			// its next Subscribe. A backpressure drop returns nil from push (markResync),
			// so the loop continues but the sequence is already abandoned server-side.
			return err
		}
	}
	return nil
}

// memberCount is the number of member-scale entries in a snapshot — the split
// granularity unit. It is the SAME sum SplitDesiredState partitions on, so a state
// with memberCount <= chunkMembers always yields exactly one chunk and thus rides the
// single-message path.
func memberCount(st model.EdgeDesiredState) int {
	return len(st.Policers) + len(st.ClassifySessions) + len(st.Anchors) +
		len(st.FlowRedirects) + len(st.ABFPolicies) + len(st.UrpfSettings)
}

// PushDelta sends an incremental EdgeDesiredDelta to the edge's open stream (the
// O(delta) hot path). On buffer overflow the delta is dropped and a full
// DESIRED_STATE resync is owed instead (see push / markResync) — state is never
// lost silently. ErrNotSubscribed when the agent has no open stream (the create
// path treats that as best-effort; the agent resyncs on its next Subscribe).
func (s *Server) PushDelta(edge model.EdgeID, delta model.EdgeDesiredDelta) error {
	payload, err := json.Marshal(delta)
	if err != nil {
		return err
	}
	return s.push(edge, &rpc.Directive{
		Kind: rpc.Directive_DESIRED_DELTA, Generation: delta.Generation, Payload: payload,
	})
}

// PushRehome tells a subscribed agent its coverage moved — re-home to the new
// primary, keep the fallbacks (L-05/L-06). No-op error (ErrNotSubscribed) when
// the agent has no open stream; it picks up the new assignment on its next
// Register instead.
func (s *Server) PushRehome(edge model.EdgeID, a model.CovererAssignment) error {
	payload, err := json.Marshal(a)
	if err != nil {
		return err
	}
	return s.push(edge, &rpc.Directive{Kind: rpc.Directive_REHOME, Payload: payload})
}

// PushDirective sends a raw directive (failover/urgent, payload already JSON).
func (s *Server) PushDirective(edge model.EdgeID, kind rpc.Directive_Kind, generation uint64, payload []byte) error {
	return s.push(edge, &rpc.Directive{Kind: kind, Generation: generation, Payload: payload})
}

func (s *Server) push(edge model.EdgeID, d *rpc.Directive) error {
	s.mu.Lock()
	sb, ok := s.subs[edge]
	s.mu.Unlock()
	if !ok {
		return ErrNotSubscribed
	}
	sz := int64(len(d.Payload))

	// A DESIRED_STATE_CHUNK is an ATOMIC fragment of a snapshot — dropping ONE middle
	// fragment makes the agent discard the WHOLE snapshot ("missing fragment; not applying
	// partial") so it never converges (the bug the byte-budget DROP introduced at scale). A
	// chunk therefore NEVER drops: it BACKPRESSURES, blocking until the queued bytes fall
	// under the budget (the send loop signals sb.drain as it frees room). The render goroutine
	// is coalesced one-per-edge (orchestrator singleflight) so at most one blocks here; sb.done
	// releases it if the stream is superseded/closed. The queuedBytes>0 guard lets a lone
	// oversized chunk through once the queue empties (atomicity wins over the cap).
	if d.Kind == rpc.Directive_DESIRED_STATE_CHUNK {
		for sb.queuedBytes.Load() > 0 && sb.queuedBytes.Load()+sz > maxQueueBytes {
			select {
			case <-sb.drain:
			case <-sb.done:
				return ErrNotSubscribed
			}
		}
		select {
		case sb.ch <- d:
			sb.queuedBytes.Add(sz)
			return nil
		case <-sb.done:
			return ErrNotSubscribed
		}
	}

	// Non-atomic kinds enqueue under the BYTE budget — pushBuffer is only a COUNT bound, and a
	// ~32MB directive makes the count meaningless (4096 of them pinned >100GB at 350K). Over
	// budget or a full channel takes the overflow path below.
	if sb.queuedBytes.Load()+sz <= maxQueueBytes {
		select {
		case sb.ch <- d:
			sb.queuedBytes.Add(sz)
			return nil
		case <-sb.done:
			return ErrNotSubscribed
		default:
			// channel depth full — fall through to the overflow path
		}
	}
	// Buffer full (slow consumer). DESIRED_DELTA is NOT coalescible — each delta carries a
	// distinct set of pool changes, so keeping only the newest would silently lose the others.
	// DROP it and mark a full resync: the send loop renders+pushes a DESIRED_STATE that
	// supersedes every dropped delta; the agent's generation-gap detection is the further
	// backstop. (Chunks never reach here — they backpressure above.)
	if d.Kind == rpc.Directive_DESIRED_DELTA {
		sb.markResync()
		return nil
	}
	// Other kinds COALESCE latest-wins: a full DESIRED_STATE supersedes the previous one, so
	// delivering only the newest is correct and the agent still converges. Push never fails
	// because of a slow consumer — the create path can commit best-effort.
	sb.stashPending(d)
	return nil
}

// IsBackpressure reports whether err is the non-fatal slow-consumer / buffer-full
// backpressure condition (ErrSlowConsumer). It lets the orchestrator treat a push
// that couldn't be delivered immediately as best-effort (commit the create, the
// agent converges via the coalescing buffer / periodic resync) rather than rolling
// back. Note: with the latest-wins coalescing push path, PushDesired no longer
// returns ErrSlowConsumer at all, so in practice this returns true only if a future
// path resurfaces the sentinel — it is the belt-and-suspenders classifier.
func (s *Server) IsBackpressure(err error) bool { return errors.Is(err, ErrSlowConsumer) }

// Subscribers returns the edges with an open stream (audit/tests).
func (s *Server) Subscribers() []model.EdgeID {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]model.EdgeID, 0, len(s.subs))
	for e := range s.subs {
		out = append(out, e)
	}
	return out
}
