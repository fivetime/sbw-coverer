package ribevent

import "context"

// Handler consumes one Event. The guard's event-processing method is passed as
// a Handler; producers call it for each normalized event, in order.
type Handler func(Event)

// Producer is the RIB-event source (DESIGN.md §6.1). A producer adapts a wire
// protocol — GoBGP's embedded server in V1, a BMP collector in V2 (§11) — into
// the normalized Event stream. The guard depends only on this interface, so a
// new RIB source is "add one more Producer" with zero guard changes.
//
// Run must, in order:
//
//  1. Replay the CURRENT adj-in as PathUpdate events, then emit one EOR per
//     (edge, family) whose dump is complete. This is the full-replay entry the
//     guard relies on so a stateless controller restart rebuilds the entire
//     view (§6.3-8 / T-608) — start-up is just a replay followed by an EOR.
//  2. Stream live events (PathUpdate / Withdrawal / EOR / PeerUp / PeerDown)
//     as they occur.
//
// It delivers every event to handler, synchronously and in arrival order, so
// the guard can be a single-goroutine state machine with no internal locking.
// Run blocks until ctx is cancelled (returning ctx.Err()) or a fatal producer
// error occurs.
type Producer interface {
	Run(ctx context.Context, handler Handler) error
}
