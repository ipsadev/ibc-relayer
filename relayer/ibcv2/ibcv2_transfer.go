package ibcv2

import (
	"context"
	"math/big"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/zap"

	"github.com/cosmos/ibc-relayer/db/gen/db"
	"github.com/cosmos/ibc-relayer/shared/lmt"
)

//nolint:revive // IBCV2Transfer is the canonical name across the codebase
type IBCV2Transfer struct {
	ID int32

	// State is the latest state reached when processing this IBCV2Transfer
	State db.Ibcv2RelayStatus

	SourceChainID             string
	DestinationChainID        string
	SourceTxHash              string
	SourceTxTime              time.Time
	SourceTxFinalizedTime     *time.Time
	PacketSequenceNumber      uint32
	PacketSourceClientID      string
	PacketDestinationClientID string
	PacketTimeoutTimestamp    time.Time

	RecvTxBytes          []byte
	RecvTxToAddress      *string
	RecvTxHash           *string
	RecvTxTime           *time.Time
	RecvTxRelayerAddress *string
	RecvTxGasCostUSD     *big.Int
	FeeDestination       *big.Int

	WriteAckTxHash          *string
	WriteAckTxTime          *time.Time
	WriteAckStatus          *db.Ibcv2WriteAckStatus
	WriteAckTxFinalizedTime *time.Time

	AckTxBytes          []byte
	AckTxToAddress      *string
	AckTxHash           *string
	AckTxTime           *time.Time
	AckTxRelayerAddress *string
	AckTxGasCostUSD     *big.Int

	TimeoutTxBytes          []byte
	TimeoutTxHash           *string
	TimeoutTxTime           *time.Time
	TimeoutTxRelayerAddress *string
	TimeoutTxGasCostUSD     *big.Int

	// ProcessingError is an error that was occurred while currently processing
	// this IBCV2Transfer
	ProcessingError error

	// Logger is the logger to be used when logging info about this transfer.
	// This is a struct field rather than being kept in the context since
	// IBCV2Transfer's are typically used in a pipeline context, which does
	// not contain information specific to this since IBCV2Transfer, it could
	// be for any IBCV2Transfer in the pipeline. Thus, to keep a logger
	// specific to the transfer, it is stored as a struct field.
	Logger *zap.Logger
}

// NewIBCV2Transfer converts a db representation of a ibcv2 transfer to the
// in memory representation.
func NewIBCV2Transfer(ctx context.Context, transfer db.Ibcv2Transfer) *IBCV2Transfer {
	var sourceTxTime time.Time
	if transfer.SourceTxTime.Valid {
		sourceTxTime = transfer.SourceTxTime.Time
	}

	var packetTimeoutTimestamp time.Time
	if transfer.PacketTimeoutTimestamp.Valid {
		packetTimeoutTimestamp = transfer.PacketTimeoutTimestamp.Time
	}

	var sourceTxFinalizedTime *time.Time
	if transfer.SourceTxFinalizedTime.Valid {
		sourceTxFinalizedTime = &transfer.SourceTxFinalizedTime.Time
	}

	var writeAckTxFinalizedTime *time.Time
	if transfer.WriteAckTxFinalizedTime.Valid {
		writeAckTxFinalizedTime = &transfer.WriteAckTxFinalizedTime.Time
	}

	var writeAckStatus *db.Ibcv2WriteAckStatus
	if transfer.WriteAckStatus.Valid {
		writeAckStatus = &transfer.WriteAckStatus.Ibcv2WriteAckStatus
	}

	var recvTxGasCostUSD *big.Int
	if transfer.RecvTxGasCostUsd.Valid {
		recvTxGasCostUSD = transfer.RecvTxGasCostUsd.Int
	}

	var ackTxGasCostUSD *big.Int
	if transfer.AckTxGasCostUsd.Valid {
		ackTxGasCostUSD = transfer.AckTxGasCostUsd.Int
	}

	var timeoutTxGasCostUSD *big.Int
	if transfer.TimeoutTxGasCostUsd.Valid {
		timeoutTxGasCostUSD = transfer.TimeoutTxGasCostUsd.Int
	}

	var feeDestination *big.Int
	if transfer.FeeDestination.Valid {
		feeDestination = transfer.FeeDestination.Int
	}

	logger := lmt.Logger(ctx).With(
		zap.String("source_chain_id", transfer.SourceChainID),
		zap.String("destination_chain_id", transfer.DestinationChainID),
		zap.String("source_tx_hash", transfer.SourceTxHash),
		zap.Int32("packet_sequence_number", transfer.PacketSequenceNumber),
		zap.String("packet_source_client_id", transfer.PacketSourceClientID),
		zap.String("packet_destination_client_id", transfer.PacketDestinationClientID),
		zap.Time("packet_timeout_timestamp", packetTimeoutTimestamp),
	)

	return &IBCV2Transfer{
		ID:                        transfer.ID,
		State:                     transfer.Status,
		SourceChainID:             transfer.SourceChainID,
		DestinationChainID:        transfer.DestinationChainID,
		SourceTxHash:              transfer.SourceTxHash,
		SourceTxTime:              sourceTxTime,
		SourceTxFinalizedTime:     sourceTxFinalizedTime,
		PacketSequenceNumber:      uint32(transfer.PacketSequenceNumber), //nolint:gosec // G115 bounded conversion
		PacketSourceClientID:      transfer.PacketSourceClientID,
		PacketDestinationClientID: transfer.PacketDestinationClientID,
		PacketTimeoutTimestamp:    packetTimeoutTimestamp,
		RecvTxHash:                nullStr(transfer.RecvTxHash),
		RecvTxTime:                nullTime(transfer.RecvTxTime),
		RecvTxRelayerAddress:      nullStr(transfer.RecvTxRelayerAddress),
		RecvTxGasCostUSD:          recvTxGasCostUSD,
		FeeDestination:            feeDestination,
		WriteAckTxHash:            nullStr(transfer.WriteAckTxHash),
		WriteAckTxTime:            nullTime(transfer.WriteAckTxTime),
		WriteAckStatus:            writeAckStatus,
		WriteAckTxFinalizedTime:   writeAckTxFinalizedTime,
		AckTxHash:                 nullStr(transfer.AckTxHash),
		AckTxTime:                 nullTime(transfer.AckTxTime),
		AckTxRelayerAddress:       nullStr(transfer.AckTxRelayerAddress),
		AckTxGasCostUSD:           ackTxGasCostUSD,
		TimeoutTxHash:             nullStr(transfer.TimeoutTxHash),
		TimeoutTxTime:             nullTime(transfer.TimeoutTxTime),
		TimeoutTxRelayerAddress:   nullStr(transfer.TimeoutTxRelayerAddress),
		TimeoutTxGasCostUSD:       timeoutTxGasCostUSD,
		Logger:                    logger,
	}
}

// Getter methods for each pointer field of the IBCV2Transfer (and non pointer
// for convenience). These methods should be used to access the fields instead
// of direct struct access when reading. This is because some fields may be
// populated or not based on the current state of the transfer. To be more
// clear about bad states, we explicitly return a bool so that users can check
// if the pointer that they expect to have a value has not been set, and
// quickly error before acting on some bad state.

func (e *IBCV2Transfer) GetID() int32 {
	return e.ID
}

func (e *IBCV2Transfer) GetSourceChainID() string {
	return e.SourceChainID
}

func (e *IBCV2Transfer) GetSourceTxTime() time.Time {
	return e.SourceTxTime
}

func (e *IBCV2Transfer) GetSourceTxHash() string {
	return e.SourceTxHash
}

func (e *IBCV2Transfer) GetSourceTxFinalizedTime() (time.Time, bool) {
	if e.SourceTxFinalizedTime == nil {
		return time.Time{}, false
	}
	return *e.SourceTxFinalizedTime, true
}

func (e *IBCV2Transfer) GetPacketSequenceNumber() uint32 {
	return e.PacketSequenceNumber
}

func (e *IBCV2Transfer) GetPacketSourceClientID() string {
	return e.PacketSourceClientID
}

func (e *IBCV2Transfer) GetPacketDestinationClientID() string {
	return e.PacketDestinationClientID
}

func (e *IBCV2Transfer) GetPacketTimeoutTimestamp() time.Time {
	return e.PacketTimeoutTimestamp
}

func (e *IBCV2Transfer) GetRecvTxHash() (string, bool) {
	if e.RecvTxHash == nil {
		return "", false
	}
	return *e.RecvTxHash, true
}

func (e *IBCV2Transfer) GetRecvTxToAddress() (string, bool) {
	if e.RecvTxToAddress == nil {
		return "", false
	}
	return *e.RecvTxToAddress, true
}

func (e *IBCV2Transfer) GetRecvTxTime() (time.Time, bool) {
	if e.RecvTxTime == nil {
		return time.Time{}, false
	}
	return *e.RecvTxTime, true
}

func (e *IBCV2Transfer) GetRecvTxRelayerAddress() (string, bool) {
	if e.RecvTxRelayerAddress == nil {
		return "", false
	}
	return *e.RecvTxRelayerAddress, true
}

func (e *IBCV2Transfer) GetRecvGasCostUSD() (*big.Int, bool) {
	if e.RecvTxGasCostUSD == nil {
		return nil, false
	}
	return e.RecvTxGasCostUSD, true
}

func (e *IBCV2Transfer) GetDestinationFee() (*big.Int, bool) {
	if e.FeeDestination == nil {
		return nil, false
	}
	return e.FeeDestination, true
}

func (e *IBCV2Transfer) GetWriteAckTxHash() (string, bool) {
	if e.WriteAckTxHash == nil {
		return "", false
	}
	return *e.WriteAckTxHash, true
}

func (e *IBCV2Transfer) GetWriteAckTxTime() (time.Time, bool) {
	if e.WriteAckTxTime == nil {
		return time.Time{}, false
	}
	return *e.WriteAckTxTime, true
}

func (e *IBCV2Transfer) GetWriteAckStatus() (db.Ibcv2WriteAckStatus, bool) {
	if e.WriteAckStatus == nil {
		return db.Ibcv2WriteAckStatus(""), false
	}
	return *e.WriteAckStatus, true
}

func (e *IBCV2Transfer) GetWriteAckTxFinalizedTime() (time.Time, bool) {
	if e.WriteAckTxFinalizedTime == nil {
		return time.Time{}, false
	}
	return *e.WriteAckTxFinalizedTime, true
}

func (e *IBCV2Transfer) GetAckTxHash() (string, bool) {
	if e.AckTxHash == nil {
		return "", false
	}
	return *e.AckTxHash, true
}

func (e *IBCV2Transfer) GetAckTxTime() (time.Time, bool) {
	if e.AckTxTime == nil {
		return time.Time{}, false
	}
	return *e.AckTxTime, true
}

func (e *IBCV2Transfer) GetAckTxRelayerAddress() (string, bool) {
	if e.AckTxRelayerAddress == nil {
		return "", false
	}
	return *e.AckTxRelayerAddress, true
}

func (e *IBCV2Transfer) GetAckGasCostUSD() (*big.Int, bool) {
	if e.AckTxGasCostUSD == nil {
		return nil, false
	}
	return e.AckTxGasCostUSD, true
}

func (e *IBCV2Transfer) GetTimeoutTxHash() (string, bool) {
	if e.TimeoutTxHash == nil {
		return "", false
	}
	return *e.TimeoutTxHash, true
}

func (e *IBCV2Transfer) GetTimeoutTxRelayerAddress() (string, bool) {
	if e.TimeoutTxRelayerAddress == nil {
		return "", false
	}
	return *e.TimeoutTxRelayerAddress, true
}

func (e *IBCV2Transfer) GetTimeoutTxTime() (time.Time, bool) {
	if e.TimeoutTxTime == nil {
		return time.Time{}, false
	}
	return *e.TimeoutTxTime, true
}

func (e *IBCV2Transfer) GetRecvTxBytes() ([]byte, bool) {
	if e.RecvTxBytes == nil {
		return nil, false
	}
	return e.RecvTxBytes, true
}

func (e *IBCV2Transfer) GetTimeoutTxBytes() ([]byte, bool) {
	if e.TimeoutTxBytes == nil {
		return nil, false
	}
	return e.TimeoutTxBytes, true
}

func (e *IBCV2Transfer) GetTimeoutGasCostUSD() (*big.Int, bool) {
	if e.TimeoutTxGasCostUSD == nil {
		return nil, false
	}
	return e.TimeoutTxGasCostUSD, true
}

func (e *IBCV2Transfer) GetAckTxBytes() ([]byte, bool) {
	if e.AckTxBytes == nil {
		return nil, false
	}
	return e.AckTxBytes, true
}

func (e *IBCV2Transfer) GetAckTxToAddress() (string, bool) {
	if e.AckTxToAddress == nil {
		return "", false
	}
	return *e.AckTxToAddress, true
}

func (e *IBCV2Transfer) GetDestinationChainID() string {
	return e.DestinationChainID
}

func (e *IBCV2Transfer) GetState() db.Ibcv2RelayStatus {
	return e.State
}

func (e *IBCV2Transfer) Error() string {
	if e.ProcessingError == nil {
		return ""
	}
	return e.ProcessingError.Error()
}

func (e *IBCV2Transfer) GetLogger() *zap.Logger {
	if e.Logger == nil {
		// default global logger with transfer fields attached
		return zap.L().With(
			zap.String("source_chain_id", e.GetSourceChainID()),
			zap.String("destination_chain_id", e.GetDestinationChainID()),
			zap.String("source_tx_hash", e.GetSourceTxHash()),
			zap.Int32("packet_sequence_number", int32(e.GetPacketSequenceNumber())), //nolint:gosec // G115 bounded conversion
			zap.String("packet_source_client_id", e.GetPacketSourceClientID()),
			zap.String("packet_destination_client_id", e.GetPacketDestinationClientID()),
			zap.Time("packet_timeout_timestamp", e.GetPacketTimeoutTimestamp()),
			zap.String("status", string(e.GetState())),
		)
	}
	return e.Logger
}

func (e *IBCV2Transfer) IsComplete(shouldRelaySuccessAcks, shouldRelayErrorAcks bool) bool {
	if e.ProcessingError != nil {
		return false
	}

	_, hasTimeoutHash := e.GetTimeoutTxHash()
	if hasTimeoutHash {
		return true
	}

	_, hasRecvHash := e.GetRecvTxHash()
	_, hasWriteAckHash := e.GetWriteAckTxHash()
	if !hasRecvHash || !hasWriteAckHash {
		return false
	}

	_, hasAckHash := e.GetAckTxHash()
	if hasAckHash {
		return true
	}

	writeAckStatus, hasWriteAckStatus := e.GetWriteAckStatus()
	if !hasWriteAckStatus {
		// should not be possible, log a warning if we see this case
		e.GetLogger().Warn("this is a bug! tx has write ack tx hash but does not have a write ack status")
		return false
	}

	isErrorAck := writeAckStatus == db.Ibcv2WriteAckStatusERROR || writeAckStatus == db.Ibcv2WriteAckStatusUNKNOWN
	isSuccessAck := writeAckStatus == db.Ibcv2WriteAckStatusSUCCESS
	if (isErrorAck && shouldRelayErrorAcks) || (isSuccessAck && shouldRelaySuccessAcks) {
		// if this is an error ack and we require them to be relayed, but we do
		// not have an ack tx hash, or if this is a success ack and we require
		// them to be relayed, but we do not have an ack tx hash, then this
		// transfer is not done (since no ack tx hash)
		return false
	}

	// either the write ack was not a success and we do not require error acks
	// to be relayed, or the write ack was a success and we do not require
	// success acks to be relayed. in these cases we are ok with no ack tx hash
	// being present and the transfer is complete
	return true
}

func (e *IBCV2Transfer) IsTimedOut() bool {
	return time.Now().After(e.GetPacketTimeoutTimestamp())
}

func nullTime(t pgtype.Timestamp) *time.Time {
	if !t.Valid {
		return nil
	}
	return &t.Time
}

func nullStr(text pgtype.Text) *string {
	if !text.Valid {
		return nil
	}
	return &text.String
}
