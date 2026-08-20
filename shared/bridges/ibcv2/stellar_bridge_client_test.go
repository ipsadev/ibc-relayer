package ibcv2

import (
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/cosmos/ibc-relayer/shared/config"
)

var _ BridgeClient = (*StellarBridgeClient)(nil)

type fakeStellarRPC struct {
	latest   protocol.GetLatestLedgerResponse
	tx       protocol.GetTransactionResponse
	ledger   protocol.GetLedgersResponse
	simulate protocol.SimulateTransactionResponse
	send     protocol.SendTransactionResponse
	err      error
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

func (f *fakeStellarRPC) LoadAccount(context.Context, string) (txnbuild.Account, error) {
	return &txnbuild.SimpleAccount{
		AccountID: "GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H",
		Sequence:  1,
	}, f.err
}

func (f *fakeStellarRPC) SimulateTransaction(
	context.Context, protocol.SimulateTransactionRequest,
) (protocol.SimulateTransactionResponse, error) {
	return f.simulate, f.err
}

func (f *fakeStellarRPC) SendTransaction(
	context.Context, protocol.SendTransactionRequest,
) (protocol.SendTransactionResponse, error) {
	return f.send, f.err
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
	client, err := NewStellarBridgeClient("stellar-testnet", cfg, rpc, nil)
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

	if _, err := NewStellarBridgeClient("stellar-testnet", cfg, &fakeStellarRPC{}, nil); err == nil {
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

	if _, err := client.ClientState(context.Background(), "07-tendermint-0"); !errors.Is(err, ErrStellarNotImplemented) {
		t.Fatalf("ClientState must report that it is not implemented, got %v", err)
	}
}

func encodeHostFunctions(t *testing.T, count uint32, functions ...xdr.HostFunction) []byte {
	t.Helper()

	out := make([]byte, 4)
	binary.BigEndian.PutUint32(out, count)
	for _, function := range functions {
		encoded, err := function.MarshalBinary()
		if err != nil {
			t.Fatalf("encoding a host function: %v", err)
		}
		out = append(out, encoded...)
	}
	return out
}

func sampleHostFunction(t *testing.T) xdr.HostFunction {
	t.Helper()

	var contract xdr.ContractId
	symbol := xdr.ScSymbol("recv_packet")
	address := xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &contract}

	return xdr.HostFunction{
		Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
		InvokeContract: &xdr.InvokeContractArgs{
			ContractAddress: address,
			FunctionName:    symbol,
			Args:            []xdr.ScVal{},
		},
	}
}

func TestDecodeHostFunctionsReadsTheArray(t *testing.T) {
	function := sampleHostFunction(t)
	raw := encodeHostFunctions(t, 2, function, function)

	functions, err := decodeHostFunctions(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(functions) != 2 {
		t.Fatalf("expected both functions, got %d", len(functions))
	}
}

func TestDecodeHostFunctionsRejectsATruncatedArray(t *testing.T) {
	if _, err := decodeHostFunctions([]byte{0, 0}); err == nil {
		t.Fatal("a buffer too short for the count must be rejected")
	}
}

// A count that disagrees with the payload means the caller and the proof API
// disagree about the batch, which must not be papered over.
func TestDecodeHostFunctionsRejectsTrailingBytes(t *testing.T) {
	function := sampleHostFunction(t)
	raw := append(encodeHostFunctions(t, 1, function), 0xff)

	if _, err := decodeHostFunctions(raw); err == nil {
		t.Fatal("trailing bytes after the declared count must be rejected")
	}
}

func TestDecodeHostFunctionsRejectsAShortCount(t *testing.T) {
	function := sampleHostFunction(t)
	raw := encodeHostFunctions(t, 3, function)

	if _, err := decodeHostFunctions(raw); err == nil {
		t.Fatal("a count larger than the payload must be rejected")
	}
}

func TestDeliverTxRequiresASigner(t *testing.T) {
	client := stellarClient(t, &fakeStellarRPC{})
	raw := encodeHostFunctions(t, 1, sampleHostFunction(t))

	if _, err := client.DeliverTx(context.Background(), raw, ""); err == nil {
		t.Fatal("submitting without a signer must fail rather than silently do nothing")
	}
}

func scSymbol(s string) xdr.ScVal {
	symbol := xdr.ScSymbol(s)
	return xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &symbol}
}

func scStringVal(s string) xdr.ScVal {
	text := xdr.ScString(s)
	return xdr.ScVal{Type: xdr.ScValTypeScvString, Str: &text}
}

func scU64(v uint64) xdr.ScVal {
	number := xdr.Uint64(v)
	return xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &number}
}

func scMap(entries ...xdr.ScMapEntry) xdr.ScVal {
	m := xdr.ScMap(entries)
	pointer := &m
	return xdr.ScVal{Type: xdr.ScValTypeScvMap, Map: &pointer}
}

func sendPacketEvent(contract xdr.ContractId, sequence uint64) xdr.ContractEvent {
	packet := scMap(
		xdr.ScMapEntry{Key: scSymbol("dest_client"), Val: scStringVal("08-wasm-0")},
		xdr.ScMapEntry{Key: scSymbol("sequence"), Val: scU64(sequence)},
		xdr.ScMapEntry{Key: scSymbol("source_client"), Val: scStringVal("07-tendermint-0")},
		xdr.ScMapEntry{Key: scSymbol("timeout_timestamp"), Val: scU64(1_800_000_000)},
	)
	data := scMap(xdr.ScMapEntry{Key: scSymbol("packet"), Val: packet})

	return xdr.ContractEvent{
		ContractId: &contract,
		Type:       xdr.ContractEventTypeContract,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: []xdr.ScVal{scSymbol("send_packet")},
				Data:   data,
			},
		},
	}
}

func TestDecodePacketInfoReadsTheRouterEvent(t *testing.T) {
	var contract xdr.ContractId
	closeTime := time.Unix(1_700_000_000, 0).UTC()

	packet, err := decodePacketInfo(sendPacketEvent(contract, 7), closeTime)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if packet.Sequence != 7 {
		t.Fatalf("expected sequence 7, got %d", packet.Sequence)
	}
	if packet.SourceClient != "07-tendermint-0" || packet.DestinationClient != "08-wasm-0" {
		t.Fatalf("expected the client ids to round trip, got %+v", packet)
	}
	if !packet.Timestamp.Equal(closeTime) {
		t.Fatalf("expected the ledger close time, got %s", packet.Timestamp)
	}
}

func TestDecodePacketInfoRejectsAnIncompletePacket(t *testing.T) {
	var contract xdr.ContractId
	event := sendPacketEvent(contract, 7)
	event.Body.V0.Data = scMap()

	if _, err := decodePacketInfo(event, time.Now()); err == nil {
		t.Fatal("an event with no packet must be rejected, not silently skipped")
	}
}

func TestEventTopicReadsTheFirstSymbol(t *testing.T) {
	var contract xdr.ContractId
	if got := eventTopic(sendPacketEvent(contract, 1)); got != "send_packet" {
		t.Fatalf("expected send_packet, got %q", got)
	}
}

func TestRouterContractRejectsABadStrkey(t *testing.T) {
	client := stellarClient(t, &fakeStellarRPC{})
	if _, err := client.routerContract(); err == nil {
		t.Fatal("a router id that is not a contract strkey must be rejected")
	}
}
