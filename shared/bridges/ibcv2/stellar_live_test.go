package ibcv2

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/cosmos/ibc-relayer/shared/config"
	"github.com/cosmos/ibc-relayer/shared/signing"
)

func TestLiveIsPacketCommitted(t *testing.T) {
	router := os.Getenv("STELLAR_LIVE_ROUTER")
	if router == "" {
		t.Skip("STELLAR_LIVE_ROUTER unset")
	}

	signer, err := signing.NewLocalStellarSigner(os.Getenv("STELLAR_LIVE_KEY"))
	if err != nil {
		t.Fatalf("building the signer: %v", err)
	}

	cfg := &config.StellarConfig{
		RPC:                "https://soroban-testnet.stellar.org",
		GatewayGRPCAddress: "127.0.0.1:50052",
		NetworkPassphrase:  "Test SDF Network ; September 2015",
		RouterContractID:   router,
		PinnedQuorumSetHashes: []string{
			"59d361aef699a1ca165dfcea7ebdedf8f9889c35cf76b35cfcb173cd72a2d669",
		},
	}

	client, err := NewStellarBridgeClient("stellar-testnet", cfg, nil, signer)
	if err != nil {
		t.Fatalf("building the bridge client: %v", err)
	}

	clientID := os.Getenv("STELLAR_LIVE_CLIENT")

	for _, sequence := range []uint64{1, 2, 99999} {
		committed, err := client.IsPacketCommitted(context.Background(), clientID, sequence)
		if err != nil {
			t.Fatalf("sequence %d: %v", sequence, err)
		}

		t.Logf("client %s sequence %d committed=%v", clientID, sequence, committed)
	}
}

func TestLiveFindPacketTx(t *testing.T) {
	router := os.Getenv("STELLAR_LIVE_ROUTER")
	if router == "" {
		t.Skip("STELLAR_LIVE_ROUTER unset")
	}

	signer, err := signing.NewLocalStellarSigner(os.Getenv("STELLAR_LIVE_KEY"))
	if err != nil {
		t.Fatalf("building the signer: %v", err)
	}

	cfg := &config.StellarConfig{
		RPC:                "https://soroban-testnet.stellar.org",
		GatewayGRPCAddress: "127.0.0.1:50052",
		NetworkPassphrase:  "Test SDF Network ; September 2015",
		RouterContractID:   router,
		PinnedQuorumSetHashes: []string{
			"59d361aef699a1ca165dfcea7ebdedf8f9889c35cf76b35cfcb173cd72a2d669",
		},
	}

	client, err := NewStellarBridgeClient("stellar-testnet", cfg, nil, signer)
	if err != nil {
		t.Fatalf("building the bridge client: %v", err)
	}

	source := os.Getenv("STELLAR_LIVE_CLIENT")
	dest := os.Getenv("STELLAR_LIVE_DEST_CLIENT")

	for _, sequence := range []uint64{1, 2} {
		ack, err := client.FindAckTx(context.Background(), source, dest, sequence)
		switch {
		case errors.Is(err, ErrTxNotFound):
			t.Logf("sequence %d: no ack tx", sequence)
		case err != nil:
			t.Fatalf("FindAckTx sequence %d: %v", sequence, err)
		default:
			t.Logf("sequence %d: ack tx %s at %s", sequence, ack.Hash, ack.Timestamp)
		}

		timeout, err := client.FindTimeoutTx(context.Background(), source, dest, sequence)
		switch {
		case errors.Is(err, ErrTxNotFound):
			t.Logf("sequence %d: no timeout tx", sequence)
		case err != nil:
			t.Fatalf("FindTimeoutTx sequence %d: %v", sequence, err)
		default:
			t.Logf("sequence %d: timeout tx %s at %s", sequence, timeout.Hash, timeout.Timestamp)
		}
	}
}
