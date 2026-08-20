package ibcv2

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/cosmos/ibc-relayer/shared/config"
)

var _ BridgeClient = (*StellarBridgeClient)(nil)

type fakeStellarRPC struct {
	latest protocol.GetLatestLedgerResponse
	tx     protocol.GetTransactionResponse
	ledger protocol.GetLedgersResponse
	err    error
}

func (f *fakeStellarRPC) GetLatestLedger(context.Context) (protocol.GetLatestLedgerResponse, error) {
	return f.latest, f.err
}

func (f *fakeStellarRPC) GetTransaction(
	context.Context, protocol.GetTransactionRequest,
) (protocol.GetTransactionResponse, error) {
	return f.tx, f.err
}

func (f *fakeStellarRPC) GetLedgers(
	context.Context, protocol.GetLedgersRequest,
) (protocol.GetLedgersResponse, error) {
	return f.ledger, f.err
}

func stellarClient(t *testing.T, rpc StellarRPC) *StellarBridgeClient {
	t.Helper()
	cfg := &config.StellarConfig{
		RPC:                   "http://127.0.0.1:8000",
		GatewayGRPCAddress:    "127.0.0.1:9100",
		NetworkPassphrase:     "Test SDF Network ; September 2015",
		RouterContractID:      "CAAAA",
		PinnedQuorumSetHashes: []string{strings.Repeat("ab", 32)},
	}
	client, err := NewStellarBridgeClient("stellar-testnet", cfg, rpc)
	if err != nil {
		t.Fatalf("building the client: %v", err)
	}
	return client
}

func TestNewStellarBridgeClientRefusesAnUnpinnedChain(t *testing.T) {
	cfg := &config.StellarConfig{
		RPC:                "http://127.0.0.1:8000",
		GatewayGRPCAddress: "127.0.0.1:9100",
		NetworkPassphrase:  "Test SDF Network ; September 2015",
		RouterContractID:   "CAAAA",
	}

	if _, err := NewStellarBridgeClient("stellar-testnet", cfg, &fakeStellarRPC{}); err == nil {
		t.Fatal("a chain with no pinned quorum set must not produce a client")
	}
}

func TestStellarChainType(t *testing.T) {
	if got := stellarClient(t, &fakeStellarRPC{}).ChainType(); got != config.ChainTypeStellar {
		t.Fatalf("expected the stellar chain type, got %q", got)
	}
}

func TestStellarLatestOnChainTimestamp(t *testing.T) {
	rpc := &fakeStellarRPC{
		latest: protocol.GetLatestLedgerResponse{Sequence: 100, LedgerCloseTime: 1_700_000_000},
	}

	got, err := stellarClient(t, rpc).LatestOnChainTimestamp(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !got.Equal(time.Unix(1_700_000_000, 0).UTC()) {
		t.Fatalf("expected the ledger close time, got %s", got)
	}
}

func TestStellarTimestampAtHeight(t *testing.T) {
	rpc := &fakeStellarRPC{
		ledger: protocol.GetLedgersResponse{
			Ledgers: []protocol.LedgerInfo{{Sequence: 42, LedgerCloseTime: 1_700_000_500}},
		},
	}

	got, err := stellarClient(t, rpc).TimestampAtHeight(context.Background(), 42)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !got.Equal(time.Unix(1_700_000_500, 0).UTC()) {
		t.Fatalf("expected the ledger close time, got %s", got)
	}
}

func TestStellarTimestampAtHeightRejectsAnOversizedLedger(t *testing.T) {
	_, err := stellarClient(t, &fakeStellarRPC{}).TimestampAtHeight(context.Background(), 1<<40)
	if err == nil {
		t.Fatal("a height beyond a stellar sequence must be rejected, not truncated")
	}
}

// A closed ledger is final under SCP, so a successful transaction is final with
// no offset to wait out.
func TestStellarIsTxFinalizedOnAClosedLedger(t *testing.T) {
	rpc := &fakeStellarRPC{
		tx: protocol.GetTransactionResponse{
			TransactionDetails: protocol.TransactionDetails{
				Status: protocol.TransactionStatusSuccess,
				Ledger: 100,
			},
			LatestLedger: 101,
		},
	}

	final, err := stellarClient(t, rpc).IsTxFinalized(context.Background(), "abc", nil)
	if err != nil || !final {
		t.Fatalf("expected a closed ledger to be final, got %v %v", final, err)
	}
}

func TestStellarIsTxFinalizedReportsAMissingTx(t *testing.T) {
	rpc := &fakeStellarRPC{
		tx: protocol.GetTransactionResponse{
			TransactionDetails: protocol.TransactionDetails{
				Status: protocol.TransactionStatusNotFound,
			},
		},
	}

	_, err := stellarClient(t, rpc).IsTxFinalized(context.Background(), "abc", nil)
	if !errors.Is(err, ErrTxNotFound) {
		t.Fatalf("expected ErrTxNotFound so the caller can retry, got %v", err)
	}
}

func TestStellarIsTxFinalizedHonoursAnOffset(t *testing.T) {
	rpc := &fakeStellarRPC{
		tx: protocol.GetTransactionResponse{
			TransactionDetails: protocol.TransactionDetails{
				Status: protocol.TransactionStatusSuccess,
				Ledger: 100,
			},
			LatestLedger: 102,
		},
	}
	offset := uint64(5)

	final, err := stellarClient(t, rpc).IsTxFinalized(context.Background(), "abc", &offset)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if final {
		t.Fatal("two ledgers of depth must not satisfy an offset of five")
	}
}

func TestStellarTxExecutionStatus(t *testing.T) {
	rpc := &fakeStellarRPC{
		tx: protocol.GetTransactionResponse{
			TransactionDetails: protocol.TransactionDetails{
				Status: protocol.TransactionStatusFailed,
			},
		},
	}

	status, err := stellarClient(t, rpc).TxExecutionStatus(context.Background(), "abc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status.Status != TxExecutionStatusFailed {
		t.Fatalf("expected a failed status, got %q", status.Status)
	}
}

func TestStellarIsTimestampFinalized(t *testing.T) {
	rpc := &fakeStellarRPC{
		latest: protocol.GetLatestLedgerResponse{LedgerCloseTime: 1_700_000_000},
	}
	client := stellarClient(t, rpc)

	past, err := client.IsTimestampFinalized(context.Background(), time.Unix(1_699_999_000, 0), nil)
	if err != nil || !past {
		t.Fatalf("a timestamp the chain has passed must be final, got %v %v", past, err)
	}

	future, err := client.IsTimestampFinalized(context.Background(), time.Unix(1_700_000_500, 0), nil)
	if err != nil || future {
		t.Fatalf("a timestamp the chain has not reached must not be final, got %v %v", future, err)
	}
}

func TestStellarUnimplementedMethodsSaySo(t *testing.T) {
	client := stellarClient(t, &fakeStellarRPC{})

	if _, err := client.DeliverTx(context.Background(), nil, ""); !errors.Is(err, ErrStellarNotImplemented) {
		t.Fatalf("DeliverTx must report that it is not implemented, got %v", err)
	}
	if _, err := client.ClientState(context.Background(), "07-tendermint-0"); !errors.Is(err, ErrStellarNotImplemented) {
		t.Fatalf("ClientState must report that it is not implemented, got %v", err)
	}
}
