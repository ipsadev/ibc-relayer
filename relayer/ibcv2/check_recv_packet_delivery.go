package ibcv2

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/zap"

	"github.com/cosmos/ibc-relayer/db/gen/db"
)

type TransferRecvTxStorage interface {
	UpdateTransferRecvTx(ctx context.Context, arg db.UpdateTransferRecvTxParams) error
}

type CheckRecvPacketDeliveryProcessor struct {
	bridgeClientManager   BridgeClientManager
	transferRecvTxStorage TransferRecvTxStorage
}

func NewCheckRecvPacketDeliveryProcessor(bridgeClientManager BridgeClientManager, transferRecvTxStorage TransferRecvTxStorage) CheckRecvPacketDeliveryProcessor {
	return CheckRecvPacketDeliveryProcessor{
		bridgeClientManager:   bridgeClientManager,
		transferRecvTxStorage: transferRecvTxStorage,
	}
}

func (p CheckRecvPacketDeliveryProcessor) Process(ctx context.Context, transfer *IBCV2Transfer) (*IBCV2Transfer, error) {
	destinationBridgeClient, err := p.bridgeClientManager.GetClient(ctx, transfer.GetDestinationChainID())
	if err != nil {
		return nil, fmt.Errorf("getting bridge client for transfer destination chain %s: %w", transfer.GetDestinationChainID(), err)
	}

	received, err := destinationBridgeClient.IsPacketReceived(
		ctx,
		transfer.GetPacketDestinationClientID(),
		uint64(transfer.GetPacketSequenceNumber()),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"checking if packet with sequence number %d is already received on destination chain %s for client %s: %w",
			transfer.GetPacketSequenceNumber(),
			transfer.GetDestinationChainID(),
			transfer.GetPacketDestinationClientID(),
			err,
		)
	}
	if !received {
		// the recv packet for this transfer has not been received by the
		// destination chain, continue without modifying the transfer to
		// delivery the recv packet as normal
		return transfer, nil
	}

	transfer.GetLogger().Info("packet already received on destination chain, searching chain for recv tx hash")

	recvTx, err := destinationBridgeClient.FindRecvTx(
		ctx,
		transfer.GetPacketSourceClientID(),
		transfer.GetPacketDestinationClientID(),
		uint64(transfer.GetPacketSequenceNumber()),
		transfer.GetPacketTimeoutTimestamp(),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"finding for recv tx hash for transfer from source client %s to dest client %s with sequence %d and timeout tx %s: %w",
			transfer.GetPacketSourceClientID(),
			transfer.GetPacketDestinationClientID(),
			uint64(transfer.GetPacketSequenceNumber()),
			transfer.GetPacketTimeoutTimestamp().Format(time.RFC3339),
			err,
		)
	}

	transfer.GetLogger().Info(
		"found existing recv tx for packet on destination chain",
		zap.String("existing_recv_tx_hash", recvTx.Hash),
		zap.Time("existing_recv_tx_time", recvTx.Timestamp),
		zap.String("existing_recv_tx_relayer_address", recvTx.RelayerAddress),
	)

	update := db.UpdateTransferRecvTxParams{
		RecvTxHash:           pgtype.Text{Valid: true, String: recvTx.Hash},
		RecvTxTime:           pgtype.Timestamp{Valid: true, Time: recvTx.Timestamp.UTC()},
		RecvTxRelayerAddress: pgtype.Text{Valid: true, String: recvTx.RelayerAddress},
		SourceChainID:        transfer.GetSourceChainID(),
		PacketSourceClientID: transfer.GetPacketSourceClientID(),
		PacketSequenceNumber: int32(transfer.GetPacketSequenceNumber()), //nolint:gosec // G115 bounded conversion
	}
	if err = p.transferRecvTxStorage.UpdateTransferRecvTx(ctx, update); err != nil {
		return nil, fmt.Errorf("updating transfer recv tx with existing tx hash %s and timestamp %s: %w", recvTx.Hash, recvTx.Timestamp.Format(time.RFC3339), err)
	}

	transfer.RecvTxHash = &recvTx.Hash
	transfer.RecvTxTime = &recvTx.Timestamp
	transfer.RecvTxRelayerAddress = &recvTx.RelayerAddress
	return transfer, nil
}

// Cancel should log error, mark packet as failed in the db if a fatal error
// and we cannot retry, if not fatal error, do nothing so it will be retried by
// this stage
func (CheckRecvPacketDeliveryProcessor) Cancel(transfer *IBCV2Transfer, err error) {
	// TODO: possibly some db updating for fatal errors (not being able to
	// decode the source tx hash feels like a fatal error). for now we just
	// always retry to make it simpler and will come back to impl this 'fatal
	// error' behavior to limit retries
	transfer.GetLogger().Error("checking if recv packet has already been delivered on the destination client", zap.Error(err))
}

// ShouldProcess determines when this processor should be run.
func (CheckRecvPacketDeliveryProcessor) ShouldProcess(transfer *IBCV2Transfer) bool {
	// always expect transfers to have the necessary data (since it is required)

	// the data we are going to populate with this processor, no need to run if
	// we already have this info
	//
	// NOTE: this processor may or may not populate the tx hash
	_, hasRecvTxHash := transfer.GetRecvTxHash()
	_, hasAckTxHash := transfer.GetAckTxHash()
	_, hasTimeoutTxHash := transfer.GetTimeoutTxHash()

	return !hasRecvTxHash && !hasAckTxHash && !hasTimeoutTxHash
}

func (CheckRecvPacketDeliveryProcessor) State() db.Ibcv2RelayStatus {
	return db.Ibcv2RelayStatusCHECKRECVPACKETDELIVERY
}
