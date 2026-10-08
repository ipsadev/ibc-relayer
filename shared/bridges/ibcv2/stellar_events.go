package ibcv2

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"math/big"
	"time"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/cosmos/ibc-relayer/db/gen/db"
)

const (
	stellarSendPacketEvent = "send_packet"
	stellarWriteAckEvent   = "write_ack"
)

func (c *StellarBridgeClient) GetTransactionSender(ctx context.Context, hash string) (string, error) {
	tx, err := c.rpc.GetTransaction(ctx, protocol.GetTransactionRequest{Hash: hash})
	if err != nil {
		return "", fmt.Errorf("getting transaction %s: %w", hash, err)
	}
	if tx.Status == protocol.TransactionStatusNotFound {
		return "", ErrTxNotFound
	}

	var envelope xdr.TransactionEnvelope
	if err = unmarshalBase64(tx.EnvelopeXDR, &envelope); err != nil {
		return "", fmt.Errorf("decoding the envelope of %s: %w", hash, err)
	}

	return envelope.SourceAccount().ToAccountId().Address(), nil
}

func (c *StellarBridgeClient) TxFee(ctx context.Context, txHash string) (*big.Int, error) {
	tx, err := c.rpc.GetTransaction(ctx, protocol.GetTransactionRequest{Hash: txHash})
	if err != nil {
		return nil, fmt.Errorf("getting transaction %s: %w", txHash, err)
	}
	if tx.Status == protocol.TransactionStatusNotFound {
		return nil, ErrTxNotFound
	}

	var result xdr.TransactionResult
	if err = unmarshalBase64(tx.ResultXDR, &result); err != nil {
		return nil, fmt.Errorf("decoding the result of %s: %w", txHash, err)
	}

	return big.NewInt(int64(result.FeeCharged)), nil
}

// SendPacketsFromTx reads the router's send_packet events out of the
// transaction meta. The events are the only place the packet fields appear;
// the transaction arguments carry them in an encoding the router chose.
func (c *StellarBridgeClient) SendPacketsFromTx(
	ctx context.Context,
	_ string,
	txHash string,
) ([]*PacketInfo, error) {
	tx, err := c.rpc.GetTransaction(ctx, protocol.GetTransactionRequest{Hash: txHash})
	if err != nil {
		return nil, fmt.Errorf("getting transaction %s: %w", txHash, err)
	}
	if tx.Status == protocol.TransactionStatusNotFound {
		return nil, ErrTxNotFound
	}

	router, err := c.routerContract()
	if err != nil {
		return nil, err
	}

	events, err := decodeContractEvents(tx.ResultMetaXDR)
	if err != nil {
		return nil, fmt.Errorf("decoding the meta of %s: %w", txHash, err)
	}

	closeTime := time.Unix(tx.LedgerCloseTime, 0).UTC()

	var packets []*PacketInfo
	for _, event := range events {
		if event.ContractId == nil || *event.ContractId != router {
			continue
		}
		if eventTopic(event) != stellarSendPacketEvent {
			continue
		}

		packet, err := decodePacketInfo(event, closeTime)
		if err != nil {
			return nil, fmt.Errorf("decoding a send_packet event in %s: %w", txHash, err)
		}
		packets = append(packets, packet)
	}

	return packets, nil
}

func (c *StellarBridgeClient) routerContract() (xdr.ContractId, error) {
	decoded, err := strkey.Decode(strkey.VersionByteContract, c.cfg.RouterContractID)
	if err != nil {
		return xdr.ContractId{}, fmt.Errorf(
			"router_contract_id %q is not a contract strkey: %w", c.cfg.RouterContractID, err,
		)
	}
	if len(decoded) != 32 {
		return xdr.ContractId{}, fmt.Errorf("router_contract_id decodes to %d bytes", len(decoded))
	}

	var contract xdr.ContractId
	copy(contract[:], decoded)
	return contract, nil
}

func decodeContractEvents(metaXDR string) ([]xdr.ContractEvent, error) {
	if metaXDR == "" {
		return nil, nil
	}

	var meta xdr.TransactionMeta
	if err := unmarshalBase64(metaXDR, &meta); err != nil {
		return nil, err
	}

	switch meta.V {
	case 3:
		if meta.V3 == nil || meta.V3.SorobanMeta == nil {
			return nil, nil
		}
		return meta.V3.SorobanMeta.Events, nil
	case 4:
		if meta.V4 == nil {
			return nil, nil
		}
		var events []xdr.ContractEvent
		for _, operation := range meta.V4.Operations {
			events = append(events, operation.Events...)
		}
		return events, nil
	default:
		return nil, nil
	}
}

func eventTopic(event xdr.ContractEvent) string {
	body, ok := event.Body.GetV0()
	if !ok || len(body.Topics) == 0 {
		return ""
	}
	symbol, ok := body.Topics[0].GetSym()
	if !ok {
		return ""
	}
	return string(symbol)
}

func decodePacketInfo(event xdr.ContractEvent, closeTime time.Time) (*PacketInfo, error) {
	body, ok := event.Body.GetV0()
	if !ok {
		return nil, fmt.Errorf("the event carries no body")
	}

	packet, ok := scMapField(body.Data, "packet")
	if !ok {
		return nil, fmt.Errorf("the event carries no packet")
	}

	sequence, ok := scUint64(packet, "sequence")
	if !ok {
		return nil, fmt.Errorf("the packet carries no sequence")
	}
	source, ok := scString(packet, "source_client")
	if !ok {
		return nil, fmt.Errorf("the packet carries no source_client")
	}
	destination, ok := scString(packet, "dest_client")
	if !ok {
		return nil, fmt.Errorf("the packet carries no dest_client")
	}
	timeout, ok := scUint64(packet, "timeout_timestamp")
	if !ok {
		return nil, fmt.Errorf("the packet carries no timeout_timestamp")
	}

	return &PacketInfo{
		Sequence:          sequence,
		SourceClient:      source,
		DestinationClient: destination,
		TimeoutTimestamp:  time.Unix(int64(timeout), 0).UTC(),
		Timestamp:         closeTime,
	}, nil
}

func scMapField(value xdr.ScVal, name string) (xdr.ScVal, bool) {
	entries, ok := value.GetMap()
	if !ok || entries == nil {
		return xdr.ScVal{}, false
	}
	for _, entry := range *entries {
		symbol, ok := entry.Key.GetSym()
		if ok && string(symbol) == name {
			return entry.Val, true
		}
	}
	return xdr.ScVal{}, false
}

func scUint64(value xdr.ScVal, name string) (uint64, bool) {
	field, ok := scMapField(value, name)
	if !ok {
		return 0, false
	}
	number, ok := field.GetU64()
	return uint64(number), ok
}

func scString(value xdr.ScVal, name string) (string, bool) {
	field, ok := scMapField(value, name)
	if !ok {
		return "", false
	}
	if text, ok := field.GetStr(); ok {
		return string(text), true
	}
	if symbol, ok := field.GetSym(); ok {
		return string(symbol), true
	}
	return "", false
}

func unmarshalBase64(encoded string, into any) error {
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return fmt.Errorf("value is not base64: %w", err)
	}
	return xdr.SafeUnmarshal(raw, into)
}

func (c *StellarBridgeClient) PacketWriteAckStatus(
	ctx context.Context,
	hash string,
	sequence uint64,
	sourceClientID string,
	destClientID string,
) (db.Ibcv2WriteAckStatus, error) {
	tx, err := c.rpc.GetTransaction(ctx, protocol.GetTransactionRequest{Hash: hash})
	if err != nil {
		return db.Ibcv2WriteAckStatusUNKNOWN, fmt.Errorf("getting transaction %s: %w", hash, err)
	}
	if tx.Status == protocol.TransactionStatusNotFound {
		return db.Ibcv2WriteAckStatusUNKNOWN, ErrTxNotFound
	}

	router, err := c.routerContract()
	if err != nil {
		return db.Ibcv2WriteAckStatusUNKNOWN, err
	}

	events, err := decodeContractEvents(tx.ResultMetaXDR)
	if err != nil {
		return db.Ibcv2WriteAckStatusUNKNOWN, fmt.Errorf("decoding the meta of %s: %w", hash, err)
	}

	routed := make([]xdr.ContractEvent, 0, len(events))
	for _, event := range events {
		if event.ContractId == nil || *event.ContractId != router {
			continue
		}
		routed = append(routed, event)
	}

	if err = recvNamesSource(routed, destClientID, sequence, sourceClientID); err != nil {
		return db.Ibcv2WriteAckStatusUNKNOWN, err
	}

	for _, event := range routed {
		if eventTopic(event) != stellarWriteAckEvent {
			continue
		}
		if !packetTopicsMatch(event, destClientID, sequence) {
			continue
		}
		return writeAckStatus(event)
	}

	return db.Ibcv2WriteAckStatusUNKNOWN, ErrWriteAckNotFoundForPacket
}

func recvNamesSource(
	events []xdr.ContractEvent,
	destClientID string,
	sequence uint64,
	sourceClientID string,
) error {
	for _, event := range events {
		if eventTopic(event) != stellarRecvPacketEvent {
			continue
		}
		if !packetTopicsMatch(event, destClientID, sequence) {
			continue
		}

		body, ok := event.Body.GetV0()
		if !ok {
			return fmt.Errorf("%w: the recv_packet event carries no body", ErrWriteAckDecoding)
		}
		packet, ok := scMapField(body.Data, "packet")
		if !ok {
			return fmt.Errorf("%w: the recv_packet event carries no packet", ErrWriteAckDecoding)
		}
		source, ok := scString(packet, "source_client")
		if !ok {
			return fmt.Errorf("%w: the packet carries no source_client", ErrWriteAckDecoding)
		}

		if source != sourceClientID {
			return ErrWriteAckNotFoundForPacket
		}
		return nil
	}

	return nil
}

func packetTopicsMatch(event xdr.ContractEvent, clientID string, sequence uint64) bool {
	body, ok := event.Body.GetV0()
	if !ok || len(body.Topics) != 3 {
		return false
	}

	topicClient, ok := body.Topics[1].GetStr()
	if !ok || string(topicClient) != clientID {
		return false
	}

	topicSequence, ok := body.Topics[2].GetU64()

	return ok && uint64(topicSequence) == sequence
}

func writeAckStatus(event xdr.ContractEvent) (db.Ibcv2WriteAckStatus, error) {
	body, ok := event.Body.GetV0()
	if !ok {
		return db.Ibcv2WriteAckStatusUNKNOWN, fmt.Errorf(
			"%w: the write_ack event carries no body", ErrWriteAckDecoding,
		)
	}

	field, ok := scMapField(body.Data, "acknowledgements")
	if !ok {
		return db.Ibcv2WriteAckStatusUNKNOWN, fmt.Errorf(
			"%w: the write_ack event carries no acknowledgements", ErrWriteAckDecoding,
		)
	}

	acknowledgements, ok := field.GetVec()
	if !ok || acknowledgements == nil {
		return db.Ibcv2WriteAckStatusUNKNOWN, fmt.Errorf(
			"%w: the acknowledgements are %s, expected a vec", ErrWriteAckDecoding, field.Type,
		)
	}
	if len(*acknowledgements) == 0 {
		return db.Ibcv2WriteAckStatusUNKNOWN, fmt.Errorf(
			"%w: the write_ack event carries no acknowledgement", ErrWriteAckDecoding,
		)
	}

	if len(*acknowledgements) == 1 {
		first, ok := (*acknowledgements)[0].GetBytes()
		if !ok {
			return db.Ibcv2WriteAckStatusUNKNOWN, fmt.Errorf(
				"%w: an acknowledgement is %s, expected bytes",
				ErrWriteAckDecoding, (*acknowledgements)[0].Type,
			)
		}
		if bytes.Equal(first, ErrorAcknowledgement[:]) {
			return db.Ibcv2WriteAckStatusERROR, nil
		}
	}

	return db.Ibcv2WriteAckStatusSUCCESS, nil
}
