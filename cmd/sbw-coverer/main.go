// Command sbw-coverer is the SBW control plane's SHARDED sensor + actuator
// (DESIGN-server-coverer-split): over its K-covered edges it runs the GoBGP RIB-tap +
// liveness, and pushes desired-state to its agents — but it ONLY watches sbw-server
// (rpc.ServerCoverer client) and NEVER touches YugabyteDB/etcd, so coverers scale with
// the edge count without fanning store connections out.
//
// SCAFFOLD (§8 step 2): module + skeleton only. The coverer-half packages migrate here in
// §8 step 3 — ribtap / shard / coverage / liveness / guard / deathvote / ribevent /
// grpcsrv (the agent-facing server) — out of sbw-controller, which then retires.
package main

import (
	"flag"
	"fmt"

	"github.com/fivetime/sbw-contract/buildinfo"
	"github.com/fivetime/sbw-contract/logx"
	"github.com/fivetime/sbw-contract/rpc"
)

// the coverer is a CLIENT of the ServerCoverer contract (it watches the server).
var _ rpc.ServerCovererClient

func main() {
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println(buildinfo.String())
		return
	}
	log := logx.Default()
	log.Info("sbw-coverer scaffold — not yet wired (DESIGN-server-coverer-split §8)",
		"version", buildinfo.Version, "component", "coverer")
	// TODO(§8 step3): watch rpc.ServerCoverer (coverage + per-edge Directive) and relay
	//   to agents; run the RIB-tap/liveness over covered edges; Report votes/member→edge up.
}
