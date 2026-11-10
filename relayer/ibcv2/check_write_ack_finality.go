package ibcv2

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/zap"

	"github.com/cosmos/ibc-relayer/db/gen/db"
)

type TransferWriteAckFinalityStorage interface {
	UpdateTransferWriteAckTxFinalizedTime(ctx context.Context, arg db.UpdateTransferWriteAckTxFinalizedTimeParams) error
}

type CheckWriteAckFinalityProcessor struct {
	bridgeClientManager       BridgeClientManager
	destinationChainID        string
	shouldRelaySuccessAcks    bool
	shouldRelayErrorAcks      bool
	storage                   TransferWriteAckFinalityStorage
	destinationFinalityOffset *uint64
}

func NewCheckWriteAckFinalityProcessor(
	bridgeClientmanager BridgeClientManager,
	destinationChainID string,
	shouldRelaySuccessAcks bool,
	shouldRelayErrorAcks bool,
	storage TransferWriteAckFinalityStorage,
	destinationFinalityOffset *uint64,
) CheckWriteAckFinalityProcessor {
	return CheckWriteAckFinalityProcessor{
		bridgeClientManager:       bridgeClientmanager,
		destinationChainID:        destinationChainID,
		shouldRelaySuccessAcks:    shouldRelaySuccessAcks,
		shouldRelayErrorAcks:      shouldRelayErrorAcks,
		storage:                   storage,
		destinationFinalityOffset: destinationFinalityOffset,
	}
}

var ErrWriteAckNotFinalized = errors.New("write tx not finalized")

func (p CheckWriteAckFinalityProcessor) Process(ctx context.Context, transfer *IBCV2Transfer) (*IBCV2Transfer, error) {
	destinationChainClient, err := p.bridgeClientManager.GetClient(ctx, transfer.GetDestinationChainID())
	if err != nil {
		return nil, fmt.Errorf("getting source bridge client for chain %s: %w", transfer.GetDestinationChainID(), err)
	}

	writeAckTxHash, ok := transfer.GetWriteAckTxHash()
	if !ok {
		return nil, errors.New("write ack tx hash not finalized, violates should process")
	}

	finalized, err := destinationChainClient.IsTxFinalized(ctx, writeAckTxHash, p.destinationFinalityOffset)
	if err != nil {
		return nil, fmt.Errorf("checking if tx %s is finalized on chain %s: %w", writeAckTxHash, transfer.GetDestinationChainID(), err)
	}
	if !finalized {
		return nil, ErrWriteAckNotFinalized
	}

	if _, writeAckTxFinalizedTimeSet := transfer.GetWriteAckTxFinalizedTime(); !writeAckTxFinalizedTimeSet {
		finalizedTime := time.Now()

		if err := p.storage.UpdateTransferWriteAckTxFinalizedTime(ctx, db.UpdateTransferWriteAckTxFinalizedTimeParams{
			WriteAckTxFinalizedTime: pgtype.Timestamp{Valid: true, Time: finalizedTime.UTC()},
		}); err != nil {
			return nil, fmt.Errorf("updating write ack tx finalized time: %w", err)
		}

		transfer.WriteAckTxFinalizedTime = &finalizedTime
	}

	return transfer, nil
}

func (CheckWriteAckFinalityProcessor) Cancel(transfer *IBCV2Transfer, err error) {
	// log error, mark packet as failed if fatal error and we cannot retry, if
	// not fatal error, do nothing so it will be retried by this stage
	if errors.Is(err, ErrWriteAckNotFinalized) {
		writeAckTxTime, _ := transfer.GetWriteAckTxTime()
		writeAckTxHash, _ := transfer.GetWriteAckTxHash()
		if time.Since(writeAckTxTime) > time.Minute*30 {
			transfer.GetLogger().Warn(
				"write ack transaction not finalized on destination chain after 30 mins, is the node lagging behind?",
				zap.String("tx_hash", writeAckTxHash),
				zap.Time("tx_time", writeAckTxTime),
				zap.Error(err),
			)
		}
		return
	}

	writeAckTxHash, _ := transfer.GetWriteAckTxHash()
	transfer.GetLogger().Error(
		"error checking finality of write ack tx",
		zap.Error(err),
		zap.String("write_ack_tx_hash", writeAckTxHash),
	)
}

// ShouldProcess determines when this processor should be run.
func (p CheckWriteAckFinalityProcessor) ShouldProcess(transfer *IBCV2Transfer) bool {
	_, hasWriteAckTxHash := transfer.GetWriteAckTxHash()
	if !hasWriteAckTxHash {
		// transfer does not have a valid write ack, so there is nothing to
		// check finality for, we should not process
		return false
	}

	// only want to process error acks
	writeAckStatus, hasWriteAckStatus := transfer.GetWriteAckStatus()
	if !hasWriteAckStatus {
		// transfer has a write ack tx hash but has no write ack status, this
		// is a bug, but err on the side of **not** relaying the packet, for now
		// TODO: once we fix the bug with error acks, err on the side of
		// relaying here
		transfer.GetLogger().Warn("this is a bug! transfer does not have a write ack status when checking write ack finality")
		return false
	}

	isErrorAck := writeAckStatus == db.Ibcv2WriteAckStatusERROR || writeAckStatus == db.Ibcv2WriteAckStatusUNKNOWN
	isSuccessAck := writeAckStatus == db.Ibcv2WriteAckStatusSUCCESS
	return (isErrorAck && p.shouldRelayErrorAcks) || (isSuccessAck && p.shouldRelaySuccessAcks)
}

func (CheckWriteAckFinalityProcessor) State() db.Ibcv2RelayStatus {
	return db.Ibcv2RelayStatusAWAITINGWRITEACKFINALITY
}
