package ribtap

import "testing"

func TestBFDConfigDisabled(t *testing.T) {
	if got := (Config{}).bfdConfig(); got != nil {
		t.Fatalf("BFD disabled must yield nil, got %+v", got)
	}
}

func TestBFDConfigDefaults(t *testing.T) {
	c := Config{BFDEnabled: true}.bfdConfig()
	if c == nil || !c.Enabled {
		t.Fatal("BFD enabled must yield an enabled config")
	}
	// GoBGP reads these as microseconds; our ms defaults (300) → 300000µs.
	if c.DesiredMinimumTxInterval != 300_000 || c.RequiredMinimumReceive != 300_000 || c.DetectionMultiplier != 3 {
		t.Errorf("defaults wrong: tx=%d rx=%d mult=%d (want 300000/300000/3 µs)",
			c.DesiredMinimumTxInterval, c.RequiredMinimumReceive, c.DetectionMultiplier)
	}
}

func TestBFDConfigMultihop(t *testing.T) {
	// Multihop targets the RFC 5883 port 4784; GoBGP (L-01) listens on it too.
	if c := (Config{BFDEnabled: true, BFDMultihop: true}).bfdConfig(); c.Port != bfdMultihopPort {
		t.Errorf("multihop BFD must target port %d, got %d", bfdMultihopPort, c.Port)
	}
	// Single-hop leaves Port unset (0 → GoBGP defaults to 3784).
	if c := (Config{BFDEnabled: true}).bfdConfig(); c.Port != 0 {
		t.Errorf("single-hop BFD must leave Port unset, got %d", c.Port)
	}
}

func TestBFDConfigExplicit(t *testing.T) {
	c := Config{BFDEnabled: true, BFDTxMs: 100, BFDRxMs: 150, BFDMultiplier: 5}.bfdConfig()
	// 100ms/150ms → 100000µs/150000µs on the wire.
	if c.DesiredMinimumTxInterval != 100_000 || c.RequiredMinimumReceive != 150_000 || c.DetectionMultiplier != 5 {
		t.Errorf("explicit values not honoured (µs): %+v", c)
	}
}
