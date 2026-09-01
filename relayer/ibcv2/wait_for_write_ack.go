package ibcv2

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/zap"

	"github.com/cosmos/ibc-relayer/db/gen/db"
	"github.com/cosmos/ibc-relayer/shared/bridges/ibcv2"
	"github.com/cosmos/ibc-relayer/shared/lmt"
)

type TransferWriteAckTxStorage interface {
	TransferClearRecvTxStorage
	UpdateTransferWriteAckTx(ctx context.Context, arg db.UpdateTransferWriteAckTxParams) error
}

type WaitForWriteAckPacket struct {
	bridgeClientManager       BridgeClientManager
	transferWriteAckTxStorage TransferWriteAckTxStorage
}

func NewWaitFoWriteAckPacketProcessor(
	bridgeClientManager BridgeClientManager,
	transferWriteAckTxStorage TransferWriteAckTxStorage,
) WaitForWriteAckPacket {
	return WaitForWriteAckPacket{
		bridgeClientManager:       bridgeClientManager,
		transferWriteAckTxStorage: transferWriteAckTxStorage,
	}
}

// Process uses the recv tx hash and the packet sequence number to wait for the
// write ack packet on the chain where the recv packet was delivered.
//
// NOTE: this only supports synchronous acks where the write ack is in the same
// tx as the recv tx hash
func (p WaitForWriteAckPacket) Process(ctx context.Context, transfer *IBCV2Transfer) (*IBCV2Transfer, error) {
	destinationChainClient, err := p.bridgeClientManager.GetClient(ctx, transfer.GetDestinationChainID())
	if err != nil {
		return nil, fmt.Errorf("getting client for transfer destination chain %s: %w", transfer.GetDestinationChainID(), err)
	}

	recvHash, ok := transfer.GetRecvTxHash()
	if !ok {
		return nil, errors.New("transfer does not have recv tx hash, violating ShouldProcess")
	}

	status, err := destinationChainClient.PacketWriteAckStatus(
		ctx,
		recvHash,
		uint64(transfer.GetPacketSequenceNumber()),
		transfer.GetPacketSourceClientID(),
		transfer.GetPacketDestinationClientID(),
	)
	if errors.Is(err, ibcv2.ErrWriteAckNotFoundForPacket) {
		// recv tx found on chain but the write ack for this transfer was not
		// found in it, retry the recv tx. This may happen if we submit a batch
		// recv tx for many transfers, but some of those transfers timed out
		// while accumulating the batch. In that case we clear the recv tx and
		// return an error so it is tried again and then timed out.
		update := db.ClearRecvTxParams{
			SourceChainID:        transfer.GetSourceChainID(),
			PacketSourceClientID: transfer.GetPacketSourceClientID(),
			PacketSequenceNumber: int32(transfer.GetPacketSequenceNumber()), //nolint:gosec // G115 bounded conversion
		}
		if err = p.transferWriteAckTxStorage.ClearRecvTx(ctx, update); err != nil {
			return nil, fmt.Errorf("clearing transfer recv tx hash %s: %w", recvHash, err)
		}
		return nil, fmt.Errorf("write ack for transfer not found in recv tx %s", recvHash)
	}
	if err != nil && !errors.Is(err, ibcv2.ErrWriteAckDecoding) {
		return nil, fmt.Errorf(
			"finding write ack for packet with sequence number %d on chain %s in recv tx with hash %s: %w",
			transfer.GetPacketSequenceNumber(),
			transfer.GetDestinationChainID(),
			recvHash,
			err,
		)
	}
	if errors.Is(err, ibcv2.ErrWriteAckDecoding) {
		// it is possible that a ibc application is not following the standard
		// format for acknowledgements and we are unable to decode the ack to
		// determine if it is a success or error ack, in this case we simply
		// mark the ack as unknown.
		lmt.Logger(ctx).Warn("could not decode write ack data for packet, ack status is unknown", zap.Error(err))
		status = db.Ibcv2WriteAckStatusUNKNOWN
	}

	// write ack was found for the packet in the recv tx hash, record recv
	// latency and increase count of recvs relayed
	transfer.RecordRecvSendLatency(ctx)
	transfer.RecordRecvRelayed(ctx)

	// expecting the write ack to happen in the same tx as the recv packet, so
	// use the same time for both
	recvTxTime, ok := transfer.GetRecvTxTime()
	if !ok {
		// dont want to fully error if only times are missing, but this is a
		// bug if this happens so log an error
		transfer.GetLogger().Error("transfer has recv tx hash but not recv tx time", zap.String("recv_tx_hash", recvHash))
	}

	update := db.UpdateTransferWriteAckTxParams{
		WriteAckTxHash:       pgtype.Text{Valid: true, String: recvHash},
		WriteAckTxTime:       pgtype.Timestamp{Valid: true, Time: recvTxTime.UTC()},
		WriteAckStatus:       db.NullIbcv2WriteAckStatus{Valid: true, Ibcv2WriteAckStatus: status},
		SourceChainID:        transfer.GetSourceChainID(),
		PacketSourceClientID: transfer.GetPacketSourceClientID(),
		PacketSequenceNumber: int32(transfer.GetPacketSequenceNumber()), //nolint:gosec // G115 bounded conversion
	}
	if err = p.transferWriteAckTxStorage.UpdateTransferWriteAckTx(ctx, update); err != nil {
		return nil, fmt.Errorf("updating transfer with write ack tx hash %s, time %s, and status %s: %w", recvHash, recvTxTime, status, err)
	}

	transfer.WriteAckTxHash = &recvHash
	transfer.WriteAckTxTime = &recvTxTime
	transfer.WriteAckStatus = &status
	return transfer, nil
}

func (WaitForWriteAckPacket) Cancel(transfer *IBCV2Transfer, err error) {
	// log error, mark packet as failed if fatal error and we cannot retry, if
	// not fatal error, do nothing so it will be retried by this stage
	if errors.Is(err, ibcv2.ErrTxNotFound) {
		transfer.GetLogger().Debug("write ack tx not yet found on chain")
		return
	}
	transfer.GetLogger().Error("error waiting for write ack packet", zap.Error(err))
}

// ShouldProcess determines when this processor should be run.
func (WaitForWriteAckPacket) ShouldProcess(transfer *IBCV2Transfer) bool {
	// the data we need to run this processor
	_, hasRecvTxHash := transfer.GetRecvTxHash()

	// the data we are going to populate with this step, no need to run if this
	// info is already present
	_, hasWriteAckTxHash := transfer.GetWriteAckTxHash()

	// dont need to process if the transfer already has a timeout submitted for
	// it. however we are not checking if the transfer is past its timeout
	// timestamp, since if it is we should still run if a recv tx hash exists.
	_, hasTimeoutTxHash := transfer.GetTimeoutTxHash()

	return (hasRecvTxHash && !hasWriteAckTxHash) && !hasTimeoutTxHash
}

func (WaitForWriteAckPacket) State() db.Ibcv2RelayStatus {
	return db.Ibcv2RelayStatusWAITFORWRITEACK
}
