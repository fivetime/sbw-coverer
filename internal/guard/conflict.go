package guard

import (
	"net/netip"
	"sort"

	"github.com/fivetime/sbw-contract/model"
	"github.com/fivetime/sbw-coverer/internal/ribevent"
)

// Conflict reports a member prefix advertised by more than one edge — a second
// source claiming the same /32 (VM-migration residue, an L-switch overlap, or a
// misconfiguration). This is a SILENT failure (no drop, no error: traffic is
// merely steered to the wrong place / settled in the wrong pool, §6.4-6), so the
// guard surfaces it as an alarm. It NEVER auto-withdraws — withdrawing the wrong
// source is more dangerous; resolution (community + local-pref to lock the home
// winner) is an operator/controller decision.
type Conflict struct {
	Prefix netip.Prefix
	Edges  []model.EdgeID // all edges currently advertising Prefix, sorted
}

type conflictNotice struct {
	conflict Conflict
	resolved bool
}

// affectedHosts returns the host prefixes whose source set this event may
// change. Caller holds the lock. canary/EOR events change no host's sources.
func (g *Guard) affectedHosts(e ribevent.Event) []netip.Prefix {
	switch e.Kind {
	case ribevent.PathUpdate, ribevent.Withdrawal:
		if g.isCanary(e) || !model.IsHost(e.Prefix) {
			return nil
		}
		return []netip.Prefix{e.Prefix}
	case ribevent.PeerUp, ribevent.PeerDown:
		// reset() clears all of this edge's hosts at once; every one of them may
		// lose a source. Snapshot them before the fold mutates the view.
		v, ok := g.edges[e.Edge]
		if !ok {
			return nil
		}
		out := make([]netip.Prefix, 0, len(v.hosts))
		for p := range v.hosts {
			out = append(out, p)
		}
		return out
	default:
		return nil
	}
}

// multiSourced reports, for each given prefix, whether it currently has more
// than one source edge. Caller holds the lock.
func (g *Guard) multiSourced(prefixes []netip.Prefix) map[netip.Prefix]bool {
	out := make(map[netip.Prefix]bool, len(prefixes))
	for _, p := range prefixes {
		out[p] = len(g.sourcesOf(p)) > 1
	}
	return out
}

// conflictTransitions compares the post-fold multi-source status of the affected
// prefixes against their pre-fold status and emits an alarm notice for each that
// crossed the single↔multi boundary. Caller holds the lock.
func (g *Guard) conflictTransitions(affected []netip.Prefix, before map[netip.Prefix]bool) []conflictNotice {
	if g.onConflict == nil || len(affected) == 0 {
		return nil
	}
	var notices []conflictNotice
	for _, p := range affected {
		srcs := g.sourcesOf(p)
		nowMulti := len(srcs) > 1
		if nowMulti == before[p] {
			continue
		}
		notices = append(notices, conflictNotice{
			conflict: Conflict{Prefix: p, Edges: srcs},
			resolved: !nowMulti,
		})
	}
	return notices
}

// sourcesOf returns the edges currently mirroring host prefix, sorted. Caller
// holds the lock. A down edge contributes nothing (PeerDown cleared its hosts).
func (g *Guard) sourcesOf(prefix netip.Prefix) []model.EdgeID {
	var edges []model.EdgeID
	for id, v := range g.edges {
		if _, ok := v.hosts[prefix]; ok {
			edges = append(edges, id)
		}
	}
	sort.Slice(edges, func(i, j int) bool { return edges[i] < edges[j] })
	return edges
}

// Sources returns the edges currently advertising host prefix (sorted). A length
// > 1 is a unique-advertisement conflict (rule 6).
func (g *Guard) Sources(prefix netip.Prefix) []model.EdgeID {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.sourcesOf(prefix)
}

// Conflicts returns every member prefix currently advertised by more than one
// edge. Use for a periodic audit; the conflict handler covers live transitions.
func (g *Guard) Conflicts() []Conflict {
	g.mu.Lock()
	defer g.mu.Unlock()

	byPrefix := map[netip.Prefix][]model.EdgeID{}
	for id, v := range g.edges {
		for p := range v.hosts {
			byPrefix[p] = append(byPrefix[p], id)
		}
	}
	var out []Conflict
	for p, edges := range byPrefix {
		if len(edges) <= 1 {
			continue
		}
		sort.Slice(edges, func(i, j int) bool { return edges[i] < edges[j] })
		out = append(out, Conflict{Prefix: p, Edges: edges})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Prefix.String() < out[j].Prefix.String() })
	return out
}
