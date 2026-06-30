package ribevent

import (
	"net/netip"
	"testing"
	"time"

	"github.com/fivetime/sbw-contract/model"
)

var ts = time.Unix(1_700_000_000, 0)

func TestConstructorsDeriveFamilyAndMaskPrefix(t *testing.T) {
	lc := []model.LargeCommunity{{GlobalAdmin: 65010, LocalData1: 101, LocalData2: 5}}
	// A host route given un-masked still normalizes; family derived from prefix.
	e := NewPathUpdate("edge-1", netip.MustParsePrefix("203.0.113.10/32"),
		netip.MustParseAddr("10.0.0.1"), lc, ts)
	if e.Kind != PathUpdate || e.Edge != "edge-1" || e.Family != model.FamilyIPv4 {
		t.Fatalf("path event = %+v", e)
	}
	if e.Prefix.String() != "203.0.113.10/32" {
		t.Errorf("prefix = %s", e.Prefix)
	}

	v6 := NewWithdrawal("edge-2", netip.MustParsePrefix("2001:db8::/48"), netip.Addr{}, nil, ts)
	if v6.Kind != Withdrawal || v6.Family != model.FamilyIPv6 {
		t.Fatalf("withdrawal v6 = %+v", v6)
	}
	if !v6.IsPath() {
		t.Error("withdrawal should report IsPath")
	}
}

func TestPrefixIsMasked(t *testing.T) {
	// Host bits set on a non-host prefix get cleared (canonical key for the guard).
	e := NewPathUpdate("e", netip.MustParsePrefix("203.0.113.5/24"), netip.Addr{}, nil, ts)
	if e.Prefix.String() != "203.0.113.0/24" {
		t.Fatalf("prefix not masked: %s", e.Prefix)
	}
}

func TestEORAndPeerEvents(t *testing.T) {
	eor := NewEOR("edge-1", model.FamilyIPv4, ts)
	if eor.Kind != EOR || eor.Family != model.FamilyIPv4 || eor.Prefix.IsValid() {
		t.Fatalf("eor = %+v", eor)
	}
	if eor.IsPath() {
		t.Error("EOR is not a path event")
	}
	for _, e := range []Event{NewPeerUp("edge-1", ts), NewPeerDown("edge-1", ts)} {
		if e.Edge != "edge-1" || e.Prefix.IsValid() || e.Family != 0 {
			t.Errorf("peer event carries unexpected fields: %+v", e)
		}
	}
}

func TestValidate(t *testing.T) {
	good := []Event{
		NewPathUpdate("e", netip.MustParsePrefix("203.0.113.0/24"), netip.Addr{}, nil, ts),
		NewWithdrawal("e", netip.MustParsePrefix("2001:db8::/48"), netip.Addr{}, nil, ts),
		NewEOR("e", model.FamilyIPv6, ts),
		NewPeerUp("e", ts),
		NewPeerDown("e", ts),
	}
	for _, e := range good {
		if err := e.Validate(); err != nil {
			t.Errorf("valid event %s rejected: %v", e.Kind, err)
		}
	}

	bad := map[string]Event{
		"no edge":          {Kind: PathUpdate, Prefix: netip.MustParsePrefix("203.0.113.0/24"), Family: model.FamilyIPv4},
		"path no prefix":   {Kind: PathUpdate, Edge: "e"},
		"family mismatch":  {Kind: PathUpdate, Edge: "e", Prefix: netip.MustParsePrefix("203.0.113.0/24"), Family: model.FamilyIPv6},
		"eor no family":    {Kind: EOR, Edge: "e"},
		"eor with prefix":  {Kind: EOR, Edge: "e", Family: model.FamilyIPv4, Prefix: netip.MustParsePrefix("203.0.113.0/24")},
		"peer with prefix": {Kind: PeerDown, Edge: "e", Prefix: netip.MustParsePrefix("203.0.113.0/24")},
		"unknown kind":     {Kind: 99, Edge: "e"},
	}
	for name, e := range bad {
		if err := e.Validate(); err == nil {
			t.Errorf("invalid event %q accepted", name)
		}
	}
}

func TestKindAndEventString(t *testing.T) {
	if PathUpdate.String() != "PathUpdate" || Kind(99).String() != "Kind(99)" {
		t.Error("Kind.String wrong")
	}
	e := NewPathUpdate("edge-1", netip.MustParsePrefix("203.0.113.10/32"),
		netip.MustParseAddr("10.0.0.1"),
		[]model.LargeCommunity{{GlobalAdmin: 65010, LocalData1: 101, LocalData2: 5}}, ts)
	got := e.String()
	want := "PathUpdate edge=edge-1 ipv4 203.0.113.10/32 nh=10.0.0.1 lc=[65010:101:5]"
	if got != want {
		t.Errorf("String() = %q\nwant %q", got, want)
	}
}
