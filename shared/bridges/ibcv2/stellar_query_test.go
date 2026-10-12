package ibcv2

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/cosmos/ibc-relayer/shared/config"
	"github.com/cosmos/ibc-relayer/shared/signing"
)

const (
	queryTestSeed = "SDYLUALQA7HEHHR2VRHCI4Y4EDLMN7A52UMCXTP2BG7DHNKF27HWFH3M"

	queryTestLightClient = "CAIRCEIRCEIRCEIRCEIRCEIRCEIRCEIRCEIRCEIRCEIRCEIRCEIRDB3V"
)

type scriptedStellarRPC struct {
	fakeStellarRPC
	returns map[string]xdr.ScVal
	calls   []string
}

func (s *scriptedStellarRPC) SimulateTransaction(
	_ context.Context, req protocol.SimulateTransactionRequest,
) (protocol.SimulateTransactionResponse, error) {
	var envelope xdr.TransactionEnvelope
	if err := unmarshalBase64(req.Transaction, &envelope); err != nil {
		return protocol.SimulateTransactionResponse{Error: err.Error()}, nil
	}

	operation := envelope.Operations()[0].Body.MustInvokeHostFunctionOp()
	function := string(operation.HostFunction.MustInvokeContract().FunctionName)
	s.calls = append(s.calls, function)

	value, ok := s.returns[function]
	if !ok {
		return protocol.SimulateTransactionResponse{
			Error: "no scripted return for " + function,
		}, nil
	}

	encoded, err := xdr.MarshalBase64(value)
	if err != nil {
		return protocol.SimulateTransactionResponse{Error: err.Error()}, nil
	}

	return protocol.SimulateTransactionResponse{
		Results: []protocol.SimulateHostFunctionResult{{ReturnValueXDR: &encoded}},
	}, nil
}

func queryTestClient(t *testing.T, rpc StellarRPC) *StellarBridgeClient {
	t.Helper()

	signer, err := signing.NewLocalStellarSigner(queryTestSeed)
	if err != nil {
		t.Fatalf("building the signer: %v", err)
	}

	cfg := &config.StellarConfig{
		RPC:                   "http://127.0.0.1:8000",
		GatewayGRPCAddress:    "127.0.0.1:9100",
		NetworkPassphrase:     "Test SDF Network ; September 2015",
		RouterContractID:      findTestRouter,
		PinnedQuorumSetHashes: []string{strings.Repeat("ab", 32)},
	}

	client, err := NewStellarBridgeClient("stellar-testnet", cfg, rpc, signer)
	if err != nil {
		t.Fatalf("building the client: %v", err)
	}

	return client
}

func contractAddress(t *testing.T, id string) xdr.ScVal {
	t.Helper()

	decoded, err := strkey.Decode(strkey.VersionByteContract, id)
	if err != nil {
		t.Fatalf("decoding %s: %v", id, err)
	}

	var contract xdr.ContractId
	copy(contract[:], decoded)
	address := xdr.ScAddress{
		Type:       xdr.ScAddressTypeScAddressTypeContract,
		ContractId: &contract,
	}

	return xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &address}
}

func tendermintStateBytes(t *testing.T, trusting uint64, height uint64) xdr.ScVal {
	t.Helper()

	state := scMap(
		xdr.ScMapEntry{Key: scSymbol("chain_id"), Val: scStringVal("simd-1")},
		xdr.ScMapEntry{Key: scSymbol("latest_height"), Val: scMap(
			xdr.ScMapEntry{Key: scSymbol("revision_height"), Val: scU64(height)},
			xdr.ScMapEntry{Key: scSymbol("revision_number"), Val: scU64(1)},
		)},
		xdr.ScMapEntry{Key: scSymbol("trusting_period_secs"), Val: scU64(trusting)},
	)

	encoded, err := xdr.MarshalBase64(state)
	if err != nil {
		t.Fatalf("encoding the client state: %v", err)
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("decoding the client state: %v", err)
	}

	value := xdr.ScBytes(raw)

	return xdr.ScVal{Type: xdr.ScValTypeScvBytes, Bytes: &value}
}

func TestClientStateReadsTheLightClientTheRouterNames(t *testing.T) {
	rpc := &scriptedStellarRPC{
		returns: map[string]xdr.ScVal{
			"light_client_address": contractAddress(t, queryTestLightClient),
			"client_state":         tendermintStateBytes(t, 1209600, 4321),
		},
	}

	state, err := queryTestClient(t, rpc).ClientState(context.Background(), "07-tendermint-6")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if state.TendermintClientState == nil {
		t.Fatal("expected a tendermint client state")
	}
	if state.TendermintClientState.TrustingPeriod != 1209600*time.Second {
		t.Fatalf("expected a 14 day trusting period, got %s", state.TendermintClientState.TrustingPeriod)
	}
	if state.TendermintClientState.LatestHeight != 4321 {
		t.Fatalf("expected height 4321, got %d", state.TendermintClientState.LatestHeight)
	}

	want := []string{"light_client_address", "client_state"}
	if len(rpc.calls) != len(want) {
		t.Fatalf("expected the calls %v, got %v", want, rpc.calls)
	}
	for i := range want {
		if rpc.calls[i] != want[i] {
			t.Fatalf("expected the calls %v, got %v", want, rpc.calls)
		}
	}
}

func TestClientStateRejectsAnUnregisteredClient(t *testing.T) {
	rpc := &scriptedStellarRPC{
		returns: map[string]xdr.ScVal{
			"light_client_address": {Type: xdr.ScValTypeScvVoid},
		},
	}

	_, err := queryTestClient(t, rpc).ClientState(context.Background(), "07-tendermint-9")
	if err == nil || !strings.Contains(err.Error(), "no light client registered") {
		t.Fatalf("a client the router does not know must be rejected, got %v", err)
	}
}

func TestDecodeTendermintClientStateRejectsAMissingHeight(t *testing.T) {
	state := scMap(
		xdr.ScMapEntry{Key: scSymbol("trusting_period_secs"), Val: scU64(1209600)},
	)
	encoded, err := xdr.MarshalBase64(state)
	if err != nil {
		t.Fatalf("encoding the client state: %v", err)
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("decoding the client state: %v", err)
	}

	if _, err := decodeTendermintClientState(raw); err == nil {
		t.Fatal("a client state with no latest_height must be rejected")
	}
}
