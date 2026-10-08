package config

import (
	"os"
	"testing"
)

const localConfig = "../../config/local/config.yml"

func TestTheConfigComesFromTheEnvironmentWhenSet(t *testing.T) {
	raw, err := os.ReadFile(localConfig)
	if err != nil {
		t.Fatalf("reading %s: %v", localConfig, err)
	}
	fromFile, err := LoadConfig(localConfig)
	if err != nil {
		t.Fatalf("the local config must load from its file: %v", err)
	}

	t.Setenv(InlineConfigVariable, string(raw))

	fromEnv, source, err := LoadConfigFromEnvOrFile("/nowhere/relayer.yml")
	if err != nil {
		t.Fatalf("the inline config must load without the file: %v", err)
	}
	if source != InlineConfigVariable {
		t.Fatalf("expected the source to be %s, got %s", InlineConfigVariable, source)
	}
	if len(fromEnv.Chains) != len(fromFile.Chains) || fromEnv.IBCV2ProofAPI != fromFile.IBCV2ProofAPI {
		t.Fatal("the inline config must decode to the same config as the file")
	}
}

func TestABlankEnvironmentVariableFallsBackToTheFile(t *testing.T) {
	t.Setenv(InlineConfigVariable, "   ")

	_, source, err := LoadConfigFromEnvOrFile(localConfig)
	if err != nil {
		t.Fatalf("the file must load: %v", err)
	}
	if source != localConfig {
		t.Fatalf("expected the file as the source, got %s", source)
	}
}

func TestABrokenInlineConfigFailsInsteadOfFallingBack(t *testing.T) {
	t.Setenv(InlineConfigVariable, "chains: [not: a map")

	_, source, err := LoadConfigFromEnvOrFile(localConfig)
	if err == nil {
		t.Fatal("a broken inline config must fail, not quietly use the file")
	}
	if source != InlineConfigVariable {
		t.Fatalf("the error must name the inline source, got %s", source)
	}
}
