package guard

import (
	"testing"

	"github.com/fivetime/sbw-contract/model"

	"github.com/fivetime/sbw-coverer/internal/ribevent"
)

// canaryGoneNoAttr is a REALISTIC canary withdrawal: a BGP withdrawal carries no
// attributes, so it has no large community — the guard must recognise it by the
// remembered canary prefix, not the LC (the lab-exposed case).
func canaryGoneNoAttr(e model.EdgeID) ribevent.Event {
	return ribevent.Event{Kind: ribevent.Withdrawal, Edge: e, Prefix: mustPfx("10.255.0.2/32")}
}

// TestViewFreezeThawNotices: the view-change handler (T-1004) fires exactly on
// per-family validity flips — thaw when the third signal completes, freeze on a
// canary loss or PeerDown, thaw again on recovery — and never spuriously for a
// family that was never valid (v6 here).
func TestViewFreezeThawNotices(t *testing.T) {
	type ev struct {
		family model.Family
		valid  bool
	}
	var got []ev
	g := New(canaryLC, WithViewChangeHandler(func(_ model.EdgeID, f model.Family, v bool) {
		got = append(got, ev{f, v})
	}))

	// Ramp to valid: peer + canary + EOR(v4). Only the EOR completes validity, so
	// exactly one notice (v4 valid=true); v6 never gets an EOR → no v6 notice.
	g.OnEvent(peerUp(edge))
	g.OnEvent(canaryUp(edge))
	g.OnEvent(eor(edge, model.FamilyIPv4))
	if len(got) != 1 || got[0].family != model.FamilyIPv4 || !got[0].valid {
		t.Fatalf("ramp should emit one v4 valid=true, got %+v", got)
	}

	got = nil
	g.OnEvent(canaryGoneNoAttr(edge)) // attribute-less canary withdrawal → v4 frozen
	if len(got) != 1 || got[0].valid {
		t.Fatalf("attribute-less canary loss should freeze v4 (valid=false), got %+v", got)
	}

	got = nil
	g.OnEvent(canaryUp(edge)) // canary back (peer+eor still held) → v4 thaws
	if len(got) != 1 || !got[0].valid {
		t.Fatalf("canary back should thaw v4 (valid=true), got %+v", got)
	}

	got = nil
	g.OnEvent(peerDown(edge)) // session lost → reset → v4 frozen
	if len(got) != 1 || got[0].valid {
		t.Fatalf("PeerDown should freeze v4, got %+v", got)
	}
}

// TestViewNoNoticeWithoutHandler: with no handler the validity machinery is a
// no-op (no panic, no work).
func TestViewNoNoticeWithoutHandler(t *testing.T) {
	g := New(canaryLC)
	bringValid(g, edge)
	g.OnEvent(peerDown(edge)) // would freeze; must not panic with nil handler
}
