package config

import (
	"strings"
	"testing"
)

func validStellar() *StellarConfig {
	return &StellarConfig{
		RPC:                   "http://127.0.0.1:8000",
		GatewayGRPCAddress:    "127.0.0.1:9100",
		NetworkPassphrase:     "Test SDF Network ; September 2015",
		RouterContractID:      "CAAAA",
		PinnedQuorumSetHashes: []string{strings.Repeat("ab", 32)},
	}
}

func TestStellarConfigValidateAcceptsACompleteConfig(t *testing.T) {
	if err := validStellar().Validate(); err != nil {
		t.Fatalf("expected a complete config to validate, got %v", err)
	}
}

func TestStellarConfigValidateRejectsAnEmptyTrustRoot(t *testing.T) {
	cfg := validStellar()
	cfg.PinnedQuorumSetHashes = nil

	err := cfg.Validate()
	if err == nil {
		t.Fatal("an empty pin list must refuse to run, not default to trusting the gateway")
	}
	if !strings.Contains(err.Error(), "untrusted transport") {
		t.Fatalf("the error must say why pinning matters, got %v", err)
	}
}

func TestStellarConfigValidateRejectsAMalformedPin(t *testing.T) {
	for name, pin := range map[string]string{
		"not hex":   "zzzz",
		"too short": "abcd",
	} {
		t.Run(name, func(t *testing.T) {
			cfg := validStellar()
			cfg.PinnedQuorumSetHashes = []string{pin}
			if err := cfg.Validate(); err == nil {
				t.Fatalf("expected %s to be rejected", name)
			}
		})
	}
}

func TestStellarConfigValidateAcceptsAPrefixedPin(t *testing.T) {
	cfg := validStellar()
	cfg.PinnedQuorumSetHashes = []string{"0x" + strings.Repeat("ab", 32)}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("a 0x-prefixed sha256 must be accepted, got %v", err)
	}
}

func TestStellarConfigValidateRejectsMissingFields(t *testing.T) {
	cases := map[string]func(*StellarConfig){
		"rpc":                  func(c *StellarConfig) { c.RPC = "" },
		"gateway_grpc_address": func(c *StellarConfig) { c.GatewayGRPCAddress = "" },
		"network_passphrase":   func(c *StellarConfig) { c.NetworkPassphrase = "" },
		"router_contract_id":   func(c *StellarConfig) { c.RouterContractID = "" },
	}

	for field, blank := range cases {
		t.Run(field, func(t *testing.T) {
			cfg := validStellar()
			blank(cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatalf("expected a missing %s to be rejected", field)
			}
		})
	}
}

func TestStellarConfigValidateRejectsANilConfig(t *testing.T) {
	var cfg *StellarConfig
	if err := cfg.Validate(); err == nil {
		t.Fatal("a stellar chain with no stellar block must be rejected")
	}
}

func TestConfigValidateChecksStellarChains(t *testing.T) {
	cfg := Config{
		RelayerAPI: RelayerAPIConfig{Address: "127.0.0.1:9001"},
		Chains: map[string]ChainConfig{
			"stellar-testnet": {
				ChainID: "stellar-testnet",
				Type:    ChainTypeStellar,
				Stellar: validStellar(),
			},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected a valid stellar chain to pass, got %v", err)
	}

	cfg.Chains["stellar-testnet"].Stellar.PinnedQuorumSetHashes = nil
	if err := cfg.Validate(); err == nil {
		t.Fatal("Config.Validate must reach into stellar chains")
	}
}

func TestGetRPCAndGRPCEndpointsForStellar(t *testing.T) {
	cfg := Config{
		RelayerAPI: RelayerAPIConfig{Address: "127.0.0.1:9001"},
		Chains: map[string]ChainConfig{
			"stellar-testnet": {
				ChainID: "stellar-testnet",
				Type:    ChainTypeStellar,
				Stellar: validStellar(),
			},
		},
	}
	reader := NewConfigReader(cfg)

	rpc, err := reader.GetRPCEndpoint("stellar-testnet")
	if err != nil || rpc != "http://127.0.0.1:8000" {
		t.Fatalf("expected the soroban rpc, got %q %v", rpc, err)
	}

	grpc, tls, err := reader.GetGRPCEndpoint("stellar-testnet")
	if err != nil || grpc != "127.0.0.1:9100" || tls {
		t.Fatalf("expected the gateway address, got %q tls=%v %v", grpc, tls, err)
	}
}

func TestStellarConfigValidateRejectsQuorumConfigsSharingAValidFrom(t *testing.T) {
	cfg := validStellar()
	cfg.QuorumConfigs = []StellarQuorumConfig{
		{QuorumSetXDR: "aabb", ValidFrom: 0},
		{QuorumSetXDR: "ccdd", ValidFrom: 0},
	}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("a tie must be rejected rather than resolved by list order")
	}
	if !strings.Contains(err.Error(), "chosen by list order") {
		t.Fatalf("the error must say why a tie matters, got %v", err)
	}
}

func TestStellarConfigValidateAcceptsARotation(t *testing.T) {
	cfg := validStellar()
	cfg.QuorumConfigs = []StellarQuorumConfig{
		{QuorumSetXDR: "aabb", ValidFrom: 0},
		{QuorumSetXDR: "ccdd", ValidFrom: 63907880},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("distinct validity ranges must be accepted, got %v", err)
	}
}

func TestStellarConfigValidateRejectsAMalformedQuorumSet(t *testing.T) {
	cfg := validStellar()
	cfg.QuorumConfigs = []StellarQuorumConfig{{QuorumSetXDR: "zz", ValidFrom: 0}}

	if err := cfg.Validate(); err == nil {
		t.Fatal("a quorum set that is not hex must be rejected")
	}
}
