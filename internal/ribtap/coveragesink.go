package ribtap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"

	"github.com/fivetime/sbw-contract/model"
)

// tapPeers is the slice of *Server the CoverageSink drives — broken out as an
// interface so the diff logic is unit-tested with a fake (no embedded GoBGP).
type tapPeers interface {
	Peers() map[model.EdgeID]netip.Addr
	AddPeer(ctx context.Context, p Peer) error
	RemovePeer(ctx context.Context, neighborAddress string) error
}

// EdgePeer resolves an edge to its tap coordinates (the edge BIRD's source
// address + peer ASN). Built from the controller's configured peer list — the
// coverage layer only knows edge ids, this supplies the dial target.
type EdgePeer func(model.EdgeID) (Peer, bool)

// CoverageSink adapts a ribtap.Server to coverage.TapSink (L-05): Ensure makes
// the tap peer with EXACTLY the edges this replica covers — adding sessions for
// newly-covered edges and removing them when coverage moves away. It is the
// active-dial glue between the sharding "brain" (coverage.Reconciler) and the
// live GoBGP tap. Use with Config.ActiveDial so the controller initiates.
type CoverageSink struct {
	tap     tapPeers
	resolve EdgePeer
	log     *slog.Logger
}

// NewCoverageSink builds the adapter. resolve maps a covered edge id to its dial
// target (address+ASN); an edge that can't be resolved is logged and skipped
// (it stays uncovered until the resolver knows it).
func NewCoverageSink(srv *Server, resolve EdgePeer) *CoverageSink {
	return newCoverageSink(srv, resolve, srv.log)
}

func newCoverageSink(tap tapPeers, resolve EdgePeer, log *slog.Logger) *CoverageSink {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &CoverageSink{tap: tap, resolve: resolve, log: log}
}

// Ensure converges the live tap sessions to want: AddPeer for each covered edge
// not yet peered, RemovePeer for each peered edge no longer covered. Errors on
// individual peers are accumulated (one bad peer doesn't abort the rest) so a
// single transient failure can't strand the whole reconcile.
func (c *CoverageSink) Ensure(ctx context.Context, want []model.EdgeID) error {
	wantSet := make(map[model.EdgeID]bool, len(want))
	for _, e := range want {
		wantSet[e] = true
	}
	have := c.tap.Peers()

	var errs []error
	// Add newly-covered edges.
	for _, e := range want {
		if _, ok := have[e]; ok {
			continue // already peered
		}
		p, ok := c.resolve(e)
		if !ok {
			c.log.Warn("coverage: cannot resolve edge to a tap peer; skipping", "edge", e)
			continue
		}
		p.Edge = e
		if err := c.tap.AddPeer(ctx, p); err != nil {
			errs = append(errs, fmt.Errorf("add %s: %w", e, err))
		}
	}
	// Drop edges no longer covered.
	for e, addr := range have {
		if wantSet[e] {
			continue
		}
		if err := c.tap.RemovePeer(ctx, addr.String()); err != nil {
			errs = append(errs, fmt.Errorf("remove %s: %w", e, err))
		}
	}
	return errors.Join(errs...)
}
