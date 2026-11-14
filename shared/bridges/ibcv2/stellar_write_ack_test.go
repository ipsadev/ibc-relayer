package ibcv2

import (
	"context"
	"errors"
	"testing"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/cosmos/ibc-relayer/db/gen/db"
)

const (
	writeAckSource   = "07-tendermint-6"
	writeAckDest     = "08-wasm-2"
	writeAckSequence = 7
)

func writeAckRouter(t *testing.T) xdr.ContractId {
	t.Helper()

	decoded, err := strkey.Decode(strkey.VersionByteContract, findTestRouter)
	if err != nil {
		t.Fatalf("decoding the router id: %v", err)
	}

	var contract xdr.ContractId
	copy(contract[:], decoded)

	return contract
}

func scBytesVal(raw []byte) xdr.ScVal {
	value := xdr.ScBytes(raw)

	return xdr.ScVal{Type: xdr.ScValTypeScvBytes, Bytes: &value}
}

func scVecVal(values ...xdr.ScVal) xdr.ScVal {
	vec := xdr.ScVec(values)
	pointer := &vec

	return xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: &pointer}
}

func routerEvent(
	contract xdr.ContractId,
	name string,
	clientID string,
	sequence uint64,
	data xdr.ScVal,
) xdr.ContractEvent {
	return xdr.ContractEvent{
		ContractId: &contract,
		Type:       xdr.ContractEventTypeContract,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: []xdr.ScVal{
					scSymbol(name),
					scStringVal(clientID),
					scU64(sequence),
				},
				Data: data,
			},
		},
	}
}

func recvEvent(contract xdr.ContractId, sourceClient string) xdr.ContractEvent {
	packet := scMap(
		xdr.ScMapEntry{Key: scSymbol("dest_client"), Val: scStringVal(writeAckDest)},
		xdr.ScMapEntry{Key: scSymbol("sequence"), Val: scU64(writeAckSequence)},
		xdr.ScMapEntry{Key: scSymbol("source_client"), Val: scStringVal(sourceClient)},
		xdr.ScMapEntry{Key: scSymbol("timeout_timestamp"), Val: scU64(1_800_000_000)},
	)

	return routerEvent(
		contract, stellarRecvPacketEvent, writeAckDest, writeAckSequence,
		scMap(xdr.ScMapEntry{Key: scSymbol("packet"), Val: packet}),
	)
}

func writeAckEvent(contract xdr.ContractId, acks ...xdr.ScVal) xdr.ContractEvent {
	return routerEvent(
		contract, stellarWriteAckEvent, writeAckDest, writeAckSequence,
		scMap(xdr.ScMapEntry{
			Key: scSymbol("acknowledgements"),
			Val: scVecVal(acks...),
		}),
	)
}

func writeAckMeta(t *testing.T, events ...xdr.ContractEvent) string {
	t.Helper()

	meta := xdr.TransactionMeta{
		V: 4,
		V4: &xdr.TransactionMetaV4{
			Operations: []xdr.OperationMetaV2{{Events: events}},
		},
	}

	encoded, err := xdr.MarshalBase64(meta)
	if err != nil {
		t.Fatalf("encoding the meta: %v", err)
	}

	return encoded
}

func writeAckRPC(t *testing.T, events ...xdr.ContractEvent) *fakeStellarRPC {
	t.Helper()

	return &fakeStellarRPC{
		tx: protocol.GetTransactionResponse{
			TransactionDetails: protocol.TransactionDetails{
				Status:        protocol.TransactionStatusSuccess,
				ResultMetaXDR: writeAckMeta(t, events...),
			},
		},
	}
}

func writeAckStatusOf(t *testing.T, events ...xdr.ContractEvent) (db.Ibcv2WriteAckStatus, error) {
	t.Helper()

	return findTestClient(t, writeAckRPC(t, events...)).PacketWriteAckStatus(
		context.Background(), findTestTxHash, writeAckSequence, writeAckSource, writeAckDest,
	)
}

func TestPacketWriteAckStatusReadsAnApplicationAck(t *testing.T) {
	contract := writeAckRouter(t)

	status, err := writeAckStatusOf(t,
		recvEvent(contract, writeAckSource),
		writeAckEvent(contract, scBytesVal([]byte{0x01})),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != db.Ibcv2WriteAckStatusSUCCESS {
		t.Fatalf("expected SUCCESS, got %q", status)
	}
}

func TestPacketWriteAckStatusReadsTheUniversalErrorAck(t *testing.T) {
	contract := writeAckRouter(t)

	status, err := writeAckStatusOf(t,
		recvEvent(contract, writeAckSource),
		writeAckEvent(contract, scBytesVal(ErrorAcknowledgement[:])),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != db.Ibcv2WriteAckStatusERROR {
		t.Fatalf("expected ERROR, got %q", status)
	}
}

func TestPacketWriteAckStatusTreatsAMultiPayloadAckAsSuccess(t *testing.T) {
	contract := writeAckRouter(t)

	status, err := writeAckStatusOf(t,
		recvEvent(contract, writeAckSource),
		writeAckEvent(contract,
			scBytesVal(ErrorAcknowledgement[:]),
			scBytesVal([]byte{0x01}),
		),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != db.Ibcv2WriteAckStatusSUCCESS {
		t.Fatalf("expected SUCCESS, got %q", status)
	}
}

func TestPacketWriteAckStatusIgnoresAnotherSequence(t *testing.T) {
	contract := writeAckRouter(t)
	event := writeAckEvent(contract, scBytesVal([]byte{0x01}))
	event.Body.V0.Topics[2] = scU64(writeAckSequence + 1)

	_, err := writeAckStatusOf(t, recvEvent(contract, writeAckSource), event)
	if !errors.Is(err, ErrWriteAckNotFoundForPacket) {
		t.Fatalf("expected ErrWriteAckNotFoundForPacket, got %v", err)
	}
}

func TestPacketWriteAckStatusRejectsAnotherSourceClient(t *testing.T) {
	contract := writeAckRouter(t)

	_, err := writeAckStatusOf(t,
		recvEvent(contract, "07-tendermint-9"),
		writeAckEvent(contract, scBytesVal([]byte{0x01})),
	)
	if !errors.Is(err, ErrWriteAckNotFoundForPacket) {
		t.Fatalf("expected ErrWriteAckNotFoundForPacket, got %v", err)
	}
}

func TestPacketWriteAckStatusIgnoresAnotherContract(t *testing.T) {
	contract := writeAckRouter(t)

	var other xdr.ContractId
	stray := writeAckEvent(contract, scBytesVal([]byte{0x01}))
	stray.ContractId = &other

	_, err := writeAckStatusOf(t, recvEvent(contract, writeAckSource), stray)
	if !errors.Is(err, ErrWriteAckNotFoundForPacket) {
		t.Fatalf("an event from another contract must not be read, got %v", err)
	}
}

func TestPacketWriteAckStatusReportsAMissingTx(t *testing.T) {
	rpc := &fakeStellarRPC{
		tx: protocol.GetTransactionResponse{
			TransactionDetails: protocol.TransactionDetails{
				Status: protocol.TransactionStatusNotFound,
			},
		},
	}

	_, err := findTestClient(t, rpc).PacketWriteAckStatus(
		context.Background(), findTestTxHash, writeAckSequence, writeAckSource, writeAckDest,
	)
	if !errors.Is(err, ErrTxNotFound) {
		t.Fatalf("expected ErrTxNotFound, got %v", err)
	}
}

func TestPacketWriteAckStatusReportsAnUndecodableAck(t *testing.T) {
	contract := writeAckRouter(t)

	_, err := writeAckStatusOf(t,
		recvEvent(contract, writeAckSource),
		writeAckEvent(contract, scU64(1)),
	)
	if !errors.Is(err, ErrWriteAckDecoding) {
		t.Fatalf("expected ErrWriteAckDecoding, got %v", err)
	}
}
