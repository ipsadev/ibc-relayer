package ibcv2

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/cosmos/ibc-relayer/db/gen/db"
	"github.com/cosmos/ibc-relayer/shared/bridges/ibcv2"
)

type TransferAckTimeoutTxStorage interface {
	TransferAckTxStorage
	TransferTimeoutTxStorage
}

type TransferAckTxStorage interface {
	UpdateTransferAckTx(ctx context.Context, arg db.UpdateTransferAckTxParams) error
}

type TransferTimeoutTxStorage interface {
	UpdateTransferTimeoutTx(ctx context.Context, arg db.UpdateTransferTimeoutTxParams) error
}

type CheckPacketCommitmentProcessor struct {
	bridgeClientManager         BridgeClientManager
	transferAckTimeoutTxStorage TransferAckTimeoutTxStorage
}

func NewCheckPacketCommitmentProcessor(
	bridgeClientManager BridgeClientManager,
	transferAckTimeoutTxStorage TransferAckTimeoutTxStorage,
) CheckPacketCommitmentProcessor {
	return CheckPacketCommitmentProcessor{
		bridgeClientManager:         bridgeClientManager,
		transferAckTimeoutTxStorage: transferAckTimeoutTxStorage,
	}
}

func (p CheckPacketCommitmentProcessor) Process(ctx context.Context, transfer *IBCV2Transfer) (*IBCV2Transfer, error) {
	sourceChainClient, err := p.bridgeClientManager.GetClient(ctx, transfer.GetSourceChainID())
	if err != nil {
		return nil, fmt.Errorf("getting bridge client for transfer source chain %s: %w", transfer.GetSourceChainID(), err)
	}

	committed, err := sourceChainClient.IsPacketCommitted(
		ctx,
		transfer.GetPacketSourceClientID(),
		uint64(transfer.GetPacketSequenceNumber()),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"checking if packet with sequence number %d is already ack'd on source chain %s for client %s: %w",
			transfer.GetPacketSequenceNumber(),
			transfer.GetSourceChainID(),
			transfer.GetPacketSourceClientID(),
			err,
		)
	}
	if committed {
		// an ack or timeout packet for this transfer has not been received by
		// the source chain, continue without modifying the transfer to deliver
		// as normal
		return transfer, nil
	}

	transfer.GetLogger().Info("no packet commitment exists for packet on source chain, ack or timeout packet must have been already received on source chain, searching source chain for tx hash")

	var ackTx, timeoutTx *ibcv2.BridgeTx
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		tx, err := sourceChainClient.FindAckTx(
			gctx,
			transfer.GetPacketSourceClientID(),
			transfer.GetPacketDestinationClientID(),
			uint64(transfer.GetPacketSequenceNumber()),
		)
		if err != nil {
			if errors.Is(err, ibcv2.ErrTxNotFound) && errors.Is(err, context.Canceled) {
				// if we simply didnt find the packet or the other goroutine found the timeout packet, do not log
				return nil
			}
			transfer.GetLogger().Warn("error finding ack tx after packet commitment already found", zap.Error(err))

			// not returning the error since we should not cancel the goroutine
			// finding the timeout tx
			return nil
		}

		ackTx = tx
		transfer.GetLogger().Info(
			"found existing ack packet for transfer",
			zap.String("ack_tx_hash", tx.Hash),
			zap.Time("ack_tx_timestamp", tx.Timestamp),
			zap.String("existing_ack_tx_relayer_address", tx.RelayerAddress),
		)

		// returning an error since now that the ack is found, we can cancel
		// the context of the other goroutine searching for the timeout tx
		return errors.New("found ack tx")
	})

	g.Go(func() error {
		tx, err := sourceChainClient.FindTimeoutTx(
			gctx,
			transfer.GetPacketSourceClientID(),
			transfer.GetPacketDestinationClientID(),
			uint64(transfer.GetPacketSequenceNumber()),
		)
		if err != nil {
			if errors.Is(err, ibcv2.ErrTxNotFound) && errors.Is(err, context.Canceled) {
				// if we simply didnt find the packet or the other goroutine found the ack packet, do not log
				return nil
			}
			transfer.GetLogger().Warn("error finding timeout tx after packet commitment already found", zap.Error(err))

			// not returning the error since we should not cancel the goroutine
			// finding the ack tx
			return nil
		}

		timeoutTx = tx
		transfer.GetLogger().Info(
			"found existing timeout packet for transfer",
			zap.String("existing_timeout_tx_hash", tx.Hash),
			zap.Time("existing_timeout_tx_timestamp", tx.Timestamp),
			zap.String("existing_timeout_tx_relayer_address", tx.RelayerAddress),
		)

		// returning an error since now that the timeout is found, we can
		// cancel the context of the other goroutine searching for the ack tx
		return errors.New("found timeout tx")
	})

	_ = g.Wait()

	switch {
	case ackTx != nil && timeoutTx != nil:
		return nil, fmt.Errorf("found both timeout tx %s and ack tx %s packet, should not be possible", timeoutTx.Hash, ackTx.Hash)
	case ackTx != nil:
		update := db.UpdateTransferAckTxParams{
			AckTxHash:            pgtype.Text{Valid: true, String: ackTx.Hash},
			AckTxTime:            pgtype.Timestamp{Valid: true, Time: ackTx.Timestamp.UTC()},
			AckTxRelayerAddress:  pgtype.Text{Valid: true, String: ackTx.RelayerAddress},
			SourceChainID:        transfer.GetSourceChainID(),
			PacketSourceClientID: transfer.GetPacketSourceClientID(),
			PacketSequenceNumber: int32(transfer.GetPacketSequenceNumber()), //nolint:gosec // G115 bounded conversion
		}
		if err = p.transferAckTimeoutTxStorage.UpdateTransferAckTx(ctx, update); err != nil {
			return nil, fmt.Errorf("updating transfer ack tx with existing tx hash %s and timestamp %s: %w", ackTx.Hash, ackTx.Timestamp.Format(time.RFC3339), err)
		}

		transfer.AckTxHash = &ackTx.Hash
		transfer.AckTxTime = &ackTx.Timestamp
		transfer.AckTxRelayerAddress = &ackTx.RelayerAddress
		return transfer, nil
	case timeoutTx != nil:
		update := db.UpdateTransferTimeoutTxParams{
			TimeoutTxHash:           pgtype.Text{Valid: true, String: timeoutTx.Hash},
			TimeoutTxTime:           pgtype.Timestamp{Valid: true, Time: timeoutTx.Timestamp.UTC()},
			TimeoutTxRelayerAddress: pgtype.Text{Valid: true, String: timeoutTx.RelayerAddress},
			SourceChainID:           transfer.GetSourceChainID(),
			PacketSourceClientID:    transfer.GetPacketSourceClientID(),
			PacketSequenceNumber:    int32(transfer.GetPacketSequenceNumber()), //nolint:gosec // G115 bounded conversion
		}
		if err = p.transferAckTimeoutTxStorage.UpdateTransferTimeoutTx(ctx, update); err != nil {
			return nil, fmt.Errorf("updating transfer timeout tx with existing tx hash %s and timestamp %s: %w", timeoutTx.Hash, timeoutTx.Timestamp.Format(time.RFC3339), err)
		}

		transfer.TimeoutTxHash = &timeoutTx.Hash
		transfer.TimeoutTxTime = &timeoutTx.Timestamp
		transfer.TimeoutTxRelayerAddress = &timeoutTx.RelayerAddress
		return transfer, nil
	default:
		return nil, fmt.Errorf("could not find timeout tx or ack tx after packet commitment not found on chain %s", transfer.GetSourceChainID())
	}
}

// Cancel should log error, mark packet as failed in the db if a fatal error
// and we cannot retry, if not fatal error, do nothing so it will be retried by
// this stage
func (CheckPacketCommitmentProcessor) Cancel(transfer *IBCV2Transfer, err error) {
	// TODO: possibly some db updating for fatal errors (not being able to
	// decode the source tx hash feels like a fatal error). for now we just
	// always retry to make it simpler and will come back to impl this 'fatal
	// error' behavior to limit retries
	transfer.GetLogger().Error("checking if packet commitment is found on the source client", zap.Error(err))
}

// ShouldProcess determines when this processor should be run.
func (CheckPacketCommitmentProcessor) ShouldProcess(transfer *IBCV2Transfer) bool {
	// the data that we need to run this processor

	// the data we are going to populate with this processor, no need to run if
	// we already have this info
	//
	// NOTE: this processor may or may not populate one of the tx hashes
	_, hasAckTxHash := transfer.GetAckTxHash()
	_, hasTimeoutTxHash := transfer.GetTimeoutTxHash()

	return !hasAckTxHash && !hasTimeoutTxHash
}

func (CheckPacketCommitmentProcessor) State() db.Ibcv2RelayStatus {
	return db.Ibcv2RelayStatusCHECKACKPACKETDELIVERY
}
