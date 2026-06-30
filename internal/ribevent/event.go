// Package ribevent defines the controller's internal RIB-event abstraction
// (DESIGN.md §6.1, the "RouteEvent" contract). It is the hard boundary between
// the RIB-survival guard (§6.3) and whatever feeds it: the guard consumes only
// ribevent.Event and a ribevent.Producer, and NEVER a GoBGP or BMP wire type.
//
// Why this exists: GoBGP is the V1 RIB source, but the design keeps BMP as a V2
// option (§11). Pinning the guard to GoBGP types would force a rewrite to
// migrate. With this abstraction the migration is "add one more Producer"; the
// guard, decision, and distribution layers don't change. It also lets the guard
// be unit-tested with a mock producer feeding scripted Events, with no real BGP.
//
// The Event vocabulary is deliberately small and normalized so the two
// producers look identical to the guard. The load-bearing normalization is
// PeerDown: a session loss surfaces as a single explicit PeerDown event (the
// adapter synthesizes it from the peer FSM), NOT as a storm of withdrawals the
// guard would have to count — see DESIGN.md §6.1 and the Producer contract.
package ribevent

import (
	"fmt"
	"net/netip"
	"strings"
	"time"

	"github.com/fivetime/sbw-contract/model"
)

// Kind is the normalized event type. These five are the entire vocabulary the
// guard reacts to; producers must map their wire events onto exactly these.
type Kind uint8

const (
	// PathUpdate is a path advertised or refreshed for a prefix.
	PathUpdate Kind = iota + 1
	// Withdrawal is a path withdrawn for a prefix. (GoBGP delivers this as a
	// path with Withdrawal=true; the adapter maps it to this kind, so the guard
	// switches on Kind and never inspects a wire bool.)
	Withdrawal
	// EOR marks end-of-RIB for one (edge, family): the initial table dump is
	// complete, so from here "absence" of a prefix is trustworthy (§6.3-4).
	EOR
	// PeerUp is a peer session establishing. Absence is untrustworthy again
	// until the next EOR.
	PeerUp
	// PeerDown is a peer session loss, synthesized by the adapter from the peer
	// FSM. The guard invalidates that edge's whole view on it (§6.3-2/3) — it
	// must never have to infer a session loss from withdrawal volume.
	PeerDown
)

var kindNames = map[Kind]string{
	PathUpdate: "PathUpdate", Withdrawal: "Withdrawal", EOR: "EOR",
	PeerUp: "PeerUp", PeerDown: "PeerDown",
}

func (k Kind) String() string {
	if n, ok := kindNames[k]; ok {
		return n
	}
	return fmt.Sprintf("Kind(%d)", uint8(k))
}

// Event is one normalized RIB event (the §6.1 RouteEvent). Prefixes use
// netip.Prefix; no NLRI or other wire type appears here.
//
// Field presence by Kind:
//   - PathUpdate / Withdrawal: Edge, Family, Prefix, NextHop, LargeCommunities.
//   - EOR:                     Edge, Family.
//   - PeerUp / PeerDown:       Edge.
//
// NextHop is included so the guard can tell apart MULTIPLE paths for the same
// prefix from different sources/next-hops — the §6.3-6 uniqueness check (T-612)
// needs that discriminator, and "different next-hop" is exactly the signal.
type Event struct {
	Kind             Kind
	Edge             model.EdgeID
	Family           model.Family
	Prefix           netip.Prefix
	NextHop          netip.Addr
	LargeCommunities []model.LargeCommunity
	Timestamp        time.Time
}

// NewPathUpdate builds a PathUpdate event. Family is derived from the prefix.
func NewPathUpdate(edge model.EdgeID, prefix netip.Prefix, nextHop netip.Addr, lcs []model.LargeCommunity, ts time.Time) Event {
	return Event{
		Kind: PathUpdate, Edge: edge, Family: model.FamilyOf(prefix),
		Prefix: prefix.Masked(), NextHop: nextHop, LargeCommunities: lcs, Timestamp: ts,
	}
}

// NewWithdrawal builds a Withdrawal event. Family is derived from the prefix.
func NewWithdrawal(edge model.EdgeID, prefix netip.Prefix, nextHop netip.Addr, lcs []model.LargeCommunity, ts time.Time) Event {
	return Event{
		Kind: Withdrawal, Edge: edge, Family: model.FamilyOf(prefix),
		Prefix: prefix.Masked(), NextHop: nextHop, LargeCommunities: lcs, Timestamp: ts,
	}
}

// NewEOR builds an end-of-RIB event for one (edge, family).
func NewEOR(edge model.EdgeID, family model.Family, ts time.Time) Event {
	return Event{Kind: EOR, Edge: edge, Family: family, Timestamp: ts}
}

// NewPeerUp builds a peer-up event.
func NewPeerUp(edge model.EdgeID, ts time.Time) Event {
	return Event{Kind: PeerUp, Edge: edge, Timestamp: ts}
}

// NewPeerDown builds a peer-down event (the adapter's normalized session-loss).
func NewPeerDown(edge model.EdgeID, ts time.Time) Event {
	return Event{Kind: PeerDown, Edge: edge, Timestamp: ts}
}

// IsPath reports whether the event carries a prefix (PathUpdate or Withdrawal).
func (e Event) IsPath() bool { return e.Kind == PathUpdate || e.Kind == Withdrawal }

// Validate checks the field-presence invariants for the kind, so producers can
// assert they emit well-formed events and the guard can trust what it receives.
func (e Event) Validate() error {
	if e.Edge == "" {
		return fmt.Errorf("ribevent: %s missing edge", e.Kind)
	}
	switch e.Kind {
	case PathUpdate, Withdrawal:
		if !e.Prefix.IsValid() {
			return fmt.Errorf("ribevent: %s missing prefix", e.Kind)
		}
		if e.Family != model.FamilyOf(e.Prefix) {
			return fmt.Errorf("ribevent: %s family %s does not match prefix %s", e.Kind, e.Family, e.Prefix)
		}
	case EOR:
		if e.Family != model.FamilyIPv4 && e.Family != model.FamilyIPv6 {
			return fmt.Errorf("ribevent: EOR missing family")
		}
		if e.Prefix.IsValid() {
			return fmt.Errorf("ribevent: EOR must not carry a prefix")
		}
	case PeerUp, PeerDown:
		if e.Prefix.IsValid() {
			return fmt.Errorf("ribevent: %s must not carry a prefix", e.Kind)
		}
	default:
		return fmt.Errorf("ribevent: unknown kind %d", uint8(e.Kind))
	}
	return nil
}

// String renders a compact one-line form for logging.
func (e Event) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s edge=%s", e.Kind, e.Edge)
	if e.Family == model.FamilyIPv4 || e.Family == model.FamilyIPv6 {
		fmt.Fprintf(&b, " %s", e.Family)
	}
	if e.Prefix.IsValid() {
		fmt.Fprintf(&b, " %s", e.Prefix)
	}
	if e.NextHop.IsValid() {
		fmt.Fprintf(&b, " nh=%s", e.NextHop)
	}
	if len(e.LargeCommunities) > 0 {
		parts := make([]string, len(e.LargeCommunities))
		for i, lc := range e.LargeCommunities {
			parts[i] = fmt.Sprintf("%d:%d:%d", lc.GlobalAdmin, lc.LocalData1, lc.LocalData2)
		}
		fmt.Fprintf(&b, " lc=[%s]", strings.Join(parts, ","))
	}
	return b.String()
}
