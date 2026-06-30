// Package ribtap is the GoBGP-backed RIB source for the controller (DESIGN.md
// §6.2): it embeds a GoBGP v4 server that the edge BIRDs peer into (the "tap"),
// and adapts GoBGP's events into the gobgp-free ribevent.Event stream the
// RIB-survival guard consumes.
//
// This package is the §6.1 producer adapter — the ONLY place GoBGP types are
// allowed. The RouteEvent abstraction itself lives in internal/ribevent and
// imports no GoBGP, so the guard (and its tests) never compile GoBGP in. T-601
// is the embedded server and peering here; T-611 adds the WatchEvent →
// ribevent.Event producer on top.
package ribtap
