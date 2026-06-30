package coverer

import (
	"fmt"
	"net/netip"

	"github.com/fivetime/sbw-contract/config"
	"github.com/fivetime/sbw-contract/logx"
)

// Config is the sbw-coverer's runtime configuration — the COVERER-HALF fields only
// (DESIGN-server-coverer-split §8). The coverer is a sharded sensor/actuator: it runs
// the GoBGP RIB-tap + guard over its covered edges and serves the agent-facing
// AgentService, but it OWNS NO STORE — it dials sbw-server (rpc.ServerCoverer) and
// NEVER opens etcd/YugabyteDB. So the store/placement/render fields of the monolith
// controller config are gone (Etcd, Yugabyte, AdminListenAddr, Replicas, EdgeAddrs,
// DriftSweep, Redpanda*, HomeMarker — all SERVER-side now); coverage decisions arrive
// over the Watch stream instead of a local reconciler.
type Config struct {
	Log logx.Config `json:"log"`
	BGP BGPConfig   `json:"bgp"`

	// Sharding carries ONLY the shard key (ReplicaID/RouterID → CovererId on the Watch
	// request + every Report). The failover decision + membership are server-side, so
	// LeaseTTL/Quorum/HardDebounce/GRPCEndpoint are gone; K is informational.
	Sharding ShardingConfig `json:"sharding"`

	// AgentGRPCListenAddr is where the L/R agents dial the agent-facing AgentService
	// (Register/Subscribe/Report). Was the monolith's GRPCListenAddr. Default ":1791".
	AgentGRPCListenAddr string `json:"agent_grpc_listen_addr"`

	// AgentAdvertiseAddr is the EXTERNALLY-ROUTABLE address agents use to reach THIS
	// coverer's AgentService (e.g. "coverer-0.sbw-system:1791") — distinct from the bind
	// AgentGRPCListenAddr (":1791"). Sent up as WatchRequest.agent_endpoint so the server
	// can hand it back in the agent's coverer-assignment (Register reply / REHOME) and the
	// agent homes to its PRIMARY coverer. Empty is allowed (K=1 reaches its sole coverer via
	// the load-balanced bootstrap), but for K>1 it MUST be set or agents cannot re-home off
	// the bootstrap onto their primary. Empty → no agent_endpoint advertised.
	AgentAdvertiseAddr string `json:"agent_advertise_addr"`

	// ServerAddr is the sbw-server dial target (rpc.ServerCoverer). REQUIRED — an empty
	// value is FATAL: with no server the coverer has nowhere to Watch coverage / Report
	// votes, so it can do nothing useful.
	ServerAddr string `json:"server_addr"`

	// MetricsListenAddr serves Prometheus /metrics. Empty disables it.
	MetricsListenAddr string `json:"metrics_listen_addr"`
}

// ShardingConfig carries the coverer's stable shard key (= CovererId). The server owns
// the failover/membership decision, so the monolith's lease/quorum/debounce/endpoint
// fields do not appear here. K is kept informational (the coverage set comes from the
// server's Watch stream, not a local K-redundant computation).
type ShardingConfig struct {
	// ReplicaID is this coverer's stable shard key — the CovererId on the WatchRequest
	// and stamped on EVERY CovererReport. Empty → BGP.RouterID. The server keys the
	// FailoverQuorum on it: it MUST be unique & stable per coverer, or K coverers'
	// votes collapse to one identity and the quorum can never form.
	ReplicaID string `json:"replica_id"`
	// K is informational only (coverage arrives from the server). 0 → 2.
	K int `json:"k"`
}

// ResolveReplicaID returns the coverer's shard key: the explicit ReplicaID, else the
// router id (the BGP tap's RouterID, validated non-empty).
func (s ShardingConfig) ResolveReplicaID(routerID string) string {
	if s.ReplicaID != "" {
		return s.ReplicaID
	}
	return routerID
}

// BGPConfig configures the embedded GoBGP RIB-tap (DESIGN.md §6.2, T-601). The coverer
// ALWAYS taps (it is the sensor), so RouterID is mandatory.
type BGPConfig struct {
	ASN             uint32       `json:"asn"`
	RouterID        string       `json:"router_id"`        // IP literal; the shard key + GoBGP requires it
	ListenPort      int          `json:"listen_port"`      // 1790 (§6.2)
	ListenAddresses []string     `json:"listen_addresses"` // e.g. ["0.0.0.0", "::"]
	Peers           []EdgePeer   `json:"peers"`            // edge→dial-target resolver for applyCoverage
	Canary          CanaryConfig `json:"canary"`           // canary large community (§6.4/T-305)

	BFDEnabled    bool   `json:"bfd_enabled"`
	BFDTxMs       uint32 `json:"bfd_tx_ms"`      // desired min TX (ms); 0 → 300
	BFDRxMs       uint32 `json:"bfd_rx_ms"`      // required min RX (ms); 0 → 300
	BFDMultiplier uint32 `json:"bfd_multiplier"` // detection multiplier; 0 → 3
	BFDMultihop   bool   `json:"bfd_multihop"`   // RFC 5883 multihop (UDP 4784) + eBGP-multihop
}

// EdgePeer maps one edge's BIRD tap session to its logical edge id — the
// applyCoverage resolver's source (edge → dial address + ASN).
type EdgePeer struct {
	Edge    string `json:"edge"`    // logical edge id
	Address string `json:"address"` // the edge BIRD's source address
	ASN     uint32 `json:"asn"`     // 0 → coverer ASN (iBGP)
}

// CanaryConfig is the per-edge canary route's large community. The zero value means no
// canary — the tap then derives liveness from PeerDown/PeerUp only.
type CanaryConfig struct {
	GlobalAdmin uint32 `json:"global_admin"`
	LocalData1  uint32 `json:"local_data1"`
	LocalData2  uint32 `json:"local_data2"`
}

// DefaultConfig returns the coverer defaults.
func DefaultConfig() Config {
	return Config{
		Log: logx.Config{Level: "info", Format: logx.FormatJSON},
		BGP: BGPConfig{
			ASN:             65010,
			ListenPort:      1790,
			ListenAddresses: []string{"0.0.0.0", "::"},
		},
		AgentGRPCListenAddr: ":1791",
		MetricsListenAddr:   ":9102",
	}
}

// LoadConfig builds the coverer config: defaults → optional JSON file → env overrides →
// validation. It always returns a defaults-populated Config (so the caller can still
// build a logger) alongside any error.
func LoadConfig(path string) (Config, error) {
	cfg := DefaultConfig()
	if err := config.LoadFile(path, &cfg); err != nil {
		return cfg, err
	}
	if err := cfg.applyEnv(); err != nil {
		return cfg, err
	}
	if err := cfg.Validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func (c *Config) applyEnv() error {
	c.Log.Level = config.String("LOG_LEVEL", c.Log.Level)
	c.Log.Format = logx.Format(config.String("LOG_FORMAT", string(c.Log.Format)))

	asn, err := config.Uint32("BGP_ASN", c.BGP.ASN)
	if err != nil {
		return err
	}
	c.BGP.ASN = asn

	c.BGP.RouterID = config.String("BGP_ROUTER_ID", c.BGP.RouterID)

	port, err := config.Int("BGP_LISTEN_PORT", c.BGP.ListenPort)
	if err != nil {
		return err
	}
	c.BGP.ListenPort = port

	if c.BGP.BFDEnabled, err = config.Bool("BGP_BFD_ENABLED", c.BGP.BFDEnabled); err != nil {
		return err
	}
	if c.BGP.BFDTxMs, err = config.Uint32("BGP_BFD_TX_MS", c.BGP.BFDTxMs); err != nil {
		return err
	}
	if c.BGP.BFDRxMs, err = config.Uint32("BGP_BFD_RX_MS", c.BGP.BFDRxMs); err != nil {
		return err
	}
	if c.BGP.BFDMultiplier, err = config.Uint32("BGP_BFD_MULTIPLIER", c.BGP.BFDMultiplier); err != nil {
		return err
	}
	if c.BGP.BFDMultihop, err = config.Bool("BGP_BFD_MULTIHOP", c.BGP.BFDMultihop); err != nil {
		return err
	}

	c.Sharding.ReplicaID = config.String("SHARDING_REPLICA_ID", c.Sharding.ReplicaID)
	if c.Sharding.K, err = config.Int("SHARDING_K", c.Sharding.K); err != nil {
		return err
	}

	c.AgentGRPCListenAddr = config.String("AGENT_GRPC_LISTEN_ADDR", c.AgentGRPCListenAddr)
	c.AgentAdvertiseAddr = config.String("AGENT_ADVERTISE_ADDR", c.AgentAdvertiseAddr)
	c.ServerAddr = config.String("SBW_SERVER_ADDR", c.ServerAddr)
	c.MetricsListenAddr = config.String("METRICS_LISTEN_ADDR", c.MetricsListenAddr)
	return nil
}

// Validate checks the coverer config for startup-blocking errors.
func (c Config) Validate() error {
	if c.BGP.ASN == 0 {
		return fmt.Errorf("coverer config: bgp.asn must be set")
	}
	if c.BGP.ListenPort < 1 || c.BGP.ListenPort > 65535 {
		return fmt.Errorf("coverer config: bgp.listen_port out of range: %d", c.BGP.ListenPort)
	}
	// RouterID is MANDATORY: it is the GoBGP router id AND the coverer's shard key
	// (CovererId). An empty value collapses K coverers' failover votes into one identity.
	if _, err := netip.ParseAddr(c.BGP.RouterID); err != nil {
		return fmt.Errorf("coverer config: bgp.router_id %q must be an IP literal (it is the CovererId shard key): %w", c.BGP.RouterID, err)
	}
	if c.ServerAddr == "" {
		return fmt.Errorf("coverer config: server_addr must be set (the sbw-server dial target)")
	}
	if c.AgentGRPCListenAddr == "" {
		return fmt.Errorf("coverer config: agent_grpc_listen_addr must be set")
	}
	// One tap session per edge (the resolver + Peers() map assume a single address per
	// edge): a duplicate edge id orphans a session, a duplicate address collides two edges.
	seenEdge := make(map[string]bool, len(c.BGP.Peers))
	seenAddr := make(map[string]bool, len(c.BGP.Peers))
	for i, p := range c.BGP.Peers {
		if p.Edge == "" {
			return fmt.Errorf("coverer config: bgp.peers[%d].edge must be set", i)
		}
		if _, err := netip.ParseAddr(p.Address); err != nil {
			return fmt.Errorf("coverer config: bgp.peers[%d].address = %q must be a valid IP address", i, p.Address)
		}
		if seenEdge[p.Edge] {
			return fmt.Errorf("coverer config: bgp.peers has duplicate edge %q (exactly one tap session per edge)", p.Edge)
		}
		if seenAddr[p.Address] {
			return fmt.Errorf("coverer config: bgp.peers has duplicate address %q (exactly one edge per tap address)", p.Address)
		}
		seenEdge[p.Edge] = true
		seenAddr[p.Address] = true
	}
	return nil
}
