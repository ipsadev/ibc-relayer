package ibcv2

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/cosmos/ibc-relayer/shared/config"
)

const (
	findTestTxHash = "3f1c1e1b2a2d3c4b5a69788796a5b4c3d2e1f00112233445566778899aabbccdd"

	findTestRouter = "CCBFFQLBB6G4PQLQFJAFDDJPWEFK33FTOVWGLS4N6AP5RLZDICFTC6RD"
)

func findTestClient(t *testing.T, rpc StellarRPC) *StellarBridgeClient {
	t.Helper()

	cfg := &config.StellarConfig{
		RPC:                   "http://127.0.0.1:8000",
		GatewayGRPCAddress:    "127.0.0.1:9100",
		NetworkPassphrase:     "Test SDF Network ; September 2015",
		RouterContractID:      findTestRouter,
		PinnedQuorumSetHashes: []string{strings.Repeat("ab", 32)},
	}

	client, err := NewStellarBridgeClient("stellar-testnet", cfg, rpc, nil)
	if err != nil {
		t.Fatalf("building the client: %v", err)
	}

	return client
}

func packetEventValue(t *testing.T, destClient string) string {
	t.Helper()

	packet := scMap(
		xdr.ScMapEntry{Key: scSymbol("dest_client"), Val: scStringVal(destClient)},
		xdr.ScMapEntry{Key: scSymbol("sequence"), Val: scU64(7)},
		xdr.ScMapEntry{Key: scSymbol("source_client"), Val: scStringVal("07-tendermint-0")},
		xdr.ScMapEntry{Key: scSymbol("timeout_timestamp"), Val: scU64(1_800_000_000)},
	)
	value := scMap(xdr.ScMapEntry{Key: scSymbol("packet"), Val: packet})

	encoded, err := xdr.MarshalBase64(value)
	if err != nil {
		t.Fatalf("encoding the event value: %v", err)
	}

	return encoded
}

func findTestRPC(t *testing.T, destClient string) *fakeStellarRPC {
	t.Helper()

	return &fakeStellarRPC{
		latest: protocol.GetLatestLedgerResponse{Sequence: 100_000},
		events: protocol.GetEventsResponse{
			Events: []protocol.EventInfo{{
				TransactionHash: findTestTxHash,
				LedgerClosedAt:  "2026-08-20T10:00:00Z",
				ValueXDR:        packetEventValue(t, destClient),
			}},
		},
	}
}

func TestFindAckTxReportsNotFoundWithoutEvents(t *testing.T) {
	rpc := &fakeStellarRPC{latest: protocol.GetLatestLedgerResponse{Sequence: 100_000}}
	client := findTestClient(t, rpc)

	_, err := client.FindAckTx(context.Background(), "07-tendermint-0", "08-wasm-0", 7)
	if !errors.Is(err, ErrTxNotFound) {
		t.Fatalf("expected ErrTxNotFound, got %v", err)
	}
}

func TestFindAckTxIgnoresAnotherDestination(t *testing.T) {
	client := findTestClient(t, findTestRPC(t, "08-wasm-9"))

	_, err := client.FindAckTx(context.Background(), "07-tendermint-0", "08-wasm-0", 7)
	if !errors.Is(err, ErrTxNotFound) {
		t.Fatalf("an event for another destination must not match, got %v", err)
	}
}

func TestFindTimeoutTxReportsNotFoundWithoutEvents(t *testing.T) {
	rpc := &fakeStellarRPC{latest: protocol.GetLatestLedgerResponse{Sequence: 100_000}}
	client := findTestClient(t, rpc)

	_, err := client.FindTimeoutTx(context.Background(), "07-tendermint-0", "08-wasm-0", 7)
	if !errors.Is(err, ErrTxNotFound) {
		t.Fatalf("expected ErrTxNotFound, got %v", err)
	}
}

func TestFindPacketTxClampsTheSearchWindow(t *testing.T) {
	for _, tc := range []struct {
		name   string
		latest uint32
		start  uint32
	}{
		{name: "young chain floors at one", latest: 100, start: 1},
		{name: "mature chain subtracts the lookback", latest: 100_000, start: 100_000 - stellarEventLookbackLedgers},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rpc := &recordingStellarRPC{
				fakeStellarRPC: fakeStellarRPC{
					latest: protocol.GetLatestLedgerResponse{Sequence: tc.latest},
				},
			}
			client := findTestClient(t, rpc)

			_, _ = client.FindAckTx(context.Background(), "07-tendermint-0", "08-wasm-0", 7)

			if rpc.request.StartLedger != tc.start {
				t.Fatalf("expected startLedger %d, got %d", tc.start, rpc.request.StartLedger)
			}
		})
	}
}

func TestEventMatchesRejectsAnEmptyValue(t *testing.T) {
	matcher := packetNames("dest_client", "08-wasm-0")
	if _, err := eventMatches(protocol.EventInfo{}, matcher); err == nil {
		t.Fatal("an event with no value must be an error, not a silent false")
	}
}

func TestFindAckTxRejectsAnUnparseableCloseTime(t *testing.T) {
	rpc := findTestRPC(t, "08-wasm-0")
	rpc.events.Events[0].LedgerClosedAt = "not a timestamp"
	client := findTestClient(t, rpc)

	_, err := client.FindAckTx(context.Background(), "07-tendermint-0", "08-wasm-0", 7)
	if err == nil || !strings.Contains(err.Error(), "close time") {
		t.Fatalf("a bad close time must surface as such, got %v", err)
	}
}

type recordingStellarRPC struct {
	fakeStellarRPC
	request protocol.GetEventsRequest
}

func (r *recordingStellarRPC) GetEvents(
	ctx context.Context, req protocol.GetEventsRequest,
) (protocol.GetEventsResponse, error) {
	r.request = req

	return r.fakeStellarRPC.GetEvents(ctx, req)
}

func findTestEnvelope(t *testing.T) string {
	t.Helper()

	var account xdr.MuxedAccount
	if err := account.SetAddress("GDRXE2BQUC3AZNPVFSCEZ76NJ3WWL25FYFK6RGZGIEKWE4SOOHSUJUJ6"); err != nil {
		t.Fatalf("building the source account: %v", err)
	}

	envelope := xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTx,
		V1: &xdr.TransactionV1Envelope{
			Tx: xdr.Transaction{SourceAccount: account},
		},
	}

	encoded, err := xdr.MarshalBase64(envelope)
	if err != nil {
		t.Fatalf("encoding the envelope: %v", err)
	}

	return encoded
}

func TestFindRecvTxMatchesTheSourceClientAndTimeout(t *testing.T) {
	rpc := findTestRPC(t, "08-wasm-0")
	rpc.tx.EnvelopeXDR = findTestEnvelope(t)
	client := findTestClient(t, rpc)

	tx, err := client.FindRecvTx(
		context.Background(),
		"07-tendermint-0",
		"08-wasm-0",
		7,
		time.Unix(1_800_000_000, 0),
	)
	if err != nil {
		t.Fatalf("finding the recv tx: %v", err)
	}
	if tx.Hash != findTestTxHash {
		t.Fatalf("got hash %s, want %s", tx.Hash, findTestTxHash)
	}
}

func TestFindRecvTxIgnoresAnotherSourceClient(t *testing.T) {
	rpc := findTestRPC(t, "08-wasm-0")
	client := findTestClient(t, rpc)

	_, err := client.FindRecvTx(
		context.Background(),
		"07-tendermint-9",
		"08-wasm-0",
		7,
		time.Unix(1_800_000_000, 0),
	)
	if !errors.Is(err, ErrTxNotFound) {
		t.Fatalf("a recv from another source client must not match, got %v", err)
	}
}

func TestFindRecvTxIgnoresAnotherTimeout(t *testing.T) {
	rpc := findTestRPC(t, "08-wasm-0")
	client := findTestClient(t, rpc)

	_, err := client.FindRecvTx(
		context.Background(),
		"07-tendermint-0",
		"08-wasm-0",
		7,
		time.Unix(1_799_999_999, 0),
	)
	if !errors.Is(err, ErrTxNotFound) {
		t.Fatalf("a packet with a different timeout must not match, got %v", err)
	}
}

func TestFindRecvTxFiltersOnTheDestinationClientTopic(t *testing.T) {
	rpc := &recordingStellarRPC{fakeStellarRPC: *findTestRPC(t, "08-wasm-0")}
	client := findTestClient(t, rpc)

	_, _ = client.FindRecvTx(
		context.Background(),
		"07-tendermint-0",
		"08-wasm-0",
		7,
		time.Unix(1_800_000_000, 0),
	)

	topics := rpc.request.Filters[0].Topics[0]
	name, ok := topics[0].ScVal.GetSym()
	if !ok || string(name) != stellarRecvPacketEvent {
		t.Fatalf("the first topic must be the recv_packet symbol, got %v", topics[0].ScVal)
	}
	client_, ok := topics[1].ScVal.GetStr()
	if !ok || string(client_) != "08-wasm-0" {
		t.Fatalf("recv events are keyed by the destination client, got %v", topics[1].ScVal)
	}
}
