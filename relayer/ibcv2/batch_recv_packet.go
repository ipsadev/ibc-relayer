package ibcv2

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/zap"

	"github.com/cosmos/ibc-relayer/db/gen/db"
	"github.com/cosmos/ibc-relayer/proto/gen/proofapi"
	"github.com/cosmos/ibc-relayer/shared/config"
	"github.com/cosmos/ibc-relayer/shared/lmt"
	"github.com/cosmos/ibc-relayer/shared/metrics"
)

type TransferRecvTxWithTxStorage interface {
	TransferRecvTxStorage
	ExecTx(ctx context.Context, fn func(q *db.Queries) error) error
}

type BatchRecvPacketProcessor struct {
	bridgeClientManager         BridgeClientManager
	transferRecvTxWithTxStorage TransferRecvTxWithTxStorage
	relayService                proofapi.ProofApiServiceClient
	sourceChainID               string
	sourceClientID              string
	destinationChainID          string
	destinationClientID         string
}

func NewBatchRecvPacketProcessor(
	bridgeClientmanager BridgeClientManager,
	transferRecvTxWithTxStorage TransferRecvTxWithTxStorage,
	relayService proofapi.ProofApiServiceClient,
	sourceChainID string,
	sourceClientID string,
	destinationChainID string,
	destinationClientID string,
) BatchRecvPacketProcessor {
	return BatchRecvPacketProcessor{
		bridgeClientManager:         bridgeClientmanager,
		transferRecvTxWithTxStorage: transferRecvTxWithTxStorage,
		relayService:                relayService,
		sourceChainID:               sourceChainID,
		sourceClientID:              sourceClientID,
		destinationChainID:          destinationChainID,
		destinationClientID:         destinationClientID,
	}
}

func (p BatchRecvPacketProcessor) Process(ctx context.Context, transfers []*IBCV2Transfer) ([]*IBCV2Transfer, error) {
	// get tx bytes and packet sequence number for all transfers in batch
	txIDs := make([][]byte, 0, len(transfers))
	sequences := make([]uint64, 0, len(transfers))
	txSet := make(map[string]struct{})
	for _, transfer := range transfers {
		if _, ok := txSet[transfer.GetSourceTxHash()]; ok {
			sequences = append(sequences, uint64(transfer.GetPacketSequenceNumber()))
			continue
		}

		txID, err := hex.DecodeString(strings.TrimPrefix(transfer.GetSourceTxHash(), "0x"))
		if err != nil {
			err = fmt.Errorf("decoding hex source tx hash %s to bytes: %w", transfer.GetSourceTxHash(), err)
			transfer.ProcessingError = err
			continue
		}

		txIDs = append(txIDs, txID)
		txSet[transfer.GetSourceTxHash()] = struct{}{}
		sequences = append(sequences, uint64(transfer.GetPacketSequenceNumber()))
	}

	logger := lmt.Logger(ctx).With(zap.Int("num_transfers", len(sequences)), zap.Any("sequences", sequences))
	logger.Info("processing batch recv submission")

	// request for a relay with each unique txIDs and all packet sequence
	// numbers in the batch from the processors source client to the processors
	// destination client
	relayByTxReqStartTs := time.Now()
	req := &proofapi.RelayByTxRequest{
		SrcChain:           p.sourceChainID,
		DstChain:           p.destinationChainID,
		SourceTxIds:        txIDs,
		SrcClientId:        p.sourceClientID,
		DstClientId:        p.destinationClientID,
		SrcPacketSequences: sequences,
	}
	resp, err := p.relayService.RelayByTx(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("getting recv tx bytes from relay service for batch of %d transfers: %w", len(sequences), err)
	}

	dur := time.Since(relayByTxReqStartTs)
	logger.Info(
		fmt.Sprintf("ibc relayer returned %d recv tx bytes in %s", len(resp.GetTx()), dur),
		zap.Duration("duration", dur),
		zap.Int("num_tx_bytes", len(resp.GetTx())),
	)

	recvTxBytes := resp.GetTx()
	to := resp.GetAddress()

	destinationChainClient, err := p.bridgeClientManager.GetClient(ctx, p.destinationChainID)
	if err != nil {
		return nil, fmt.Errorf("getting client for transfer destination chain %s: %w", p.destinationChainID, err)
	}

	if destinationChainClient.ChainType() == config.ChainTypeEVM {
		// for evm chains, we must wait until the chain has caught up to the
		// current time before estimating the gas of the tx during delivery or
		// else it will revert
		waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		if err = destinationChainClient.WaitForChain(waitCtx); err != nil {
			return nil, fmt.Errorf("waiting for chain: %w", err)
		}
	}

	// submit the recv on the destination chain
	recvTx, err := destinationChainClient.DeliverTx(ctx, recvTxBytes, to)
	if err != nil {
		metrics.RecordTransactionSubmitted(ctx, false, p.destinationChainID)
		return nil, fmt.Errorf("signing and submitting recv tx bytes for batch of %d transfers: %w", len(sequences), err)
	}
	metrics.RecordTransactionSubmitted(ctx, true, p.destinationChainID)

	logger.Info("delivered batch recv tx", zap.String("tx_hash", recvTx.Hash))

	submittedTransferCount := 0
	err = p.transferRecvTxWithTxStorage.ExecTx(ctx, func(q *db.Queries) error {
		for _, transfer := range transfers {
			if transfer.ProcessingError != nil {
				continue
			}
			submittedTransferCount++

			update := db.UpdateTransferRecvTxParams{
				RecvTxHash:           pgtype.Text{Valid: true, String: recvTx.Hash},
				RecvTxTime:           pgtype.Timestamp{Valid: true, Time: recvTx.Timestamp.UTC()},
				RecvTxRelayerAddress: pgtype.Text{Valid: true, String: recvTx.RelayerAddress},
				SourceChainID:        transfer.GetSourceChainID(),
				PacketSourceClientID: transfer.GetPacketSourceClientID(),
				PacketSequenceNumber: int32(transfer.GetPacketSequenceNumber()), //nolint:gosec // G115 bounded conversion
			}
			if err = q.UpdateTransferRecvTx(ctx, update); err != nil {
				return fmt.Errorf(
					"updating transfer from source chain %s packet sequence %d and source client %s with recv hash %s: %w",
					transfer.GetSourceChainID(),
					transfer.GetPacketSequenceNumber(),
					transfer.GetPacketSourceClientID(),
					recvTx.Hash,
					err,
				)
			}
		}

		if submittedTransferCount == 0 {
			return nil
		}

		insert := db.InsertIBCV2RelayerTxSubmissionParams{
			TxHash:         recvTx.Hash,
			ChainID:        p.destinationChainID,
			TxType:         db.Ibcv2RelayerTxSubmissionTypeRECVPACKET,
			RelayerAddress: recvTx.RelayerAddress,
			SubmittedAt:    pgtype.Timestamptz{Valid: true, Time: recvTx.Timestamp},
			Status:         db.Ibcv2RelayerTxSubmissionStatusPENDING,
		}
		submissionID, err := q.InsertIBCV2RelayerTxSubmission(ctx, insert)
		if err != nil {
			return fmt.Errorf("inserting recv tx submission %s: %w", recvTx.Hash, err)
		}
		if err = insertTransferTxSubmissionMappings(ctx, q, submissionID, transfers); err != nil {
			return fmt.Errorf("inserting recv tx submission mappings for %s: %w", recvTx.Hash, err)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("updating batch transfers recv txs: %w", err)
	}

	for _, transfer := range transfers {
		if transfer.ProcessingError != nil {
			continue
		}

		transfer.RecvTxHash = &recvTx.Hash
		transfer.RecvTxTime = &recvTx.Timestamp
		transfer.RecvTxRelayerAddress = &recvTx.RelayerAddress
	}
	return transfers, nil
}

func (BatchRecvPacketProcessor) Cancel(transfers []*IBCV2Transfer, err error) {
	// log error, mark packet as failed if fatal error and we cannot retry, if
	// not fatal error, do nothing so it will be retried by this stage
	for _, transfer := range transfers {
		transfer.GetLogger().Error("error submitting batch recv", zap.Error(err))
	}
}

// ShouldProcess determines when this processor should be run.
func (BatchRecvPacketProcessor) ShouldProcess(transfer *IBCV2Transfer) bool {
	// the data we are going to populate, no need to run if this is already
	// present
	_, hasRecvTxHash := transfer.GetRecvTxHash()
	return !hasRecvTxHash && !transfer.IsTimedOut()
}

func (BatchRecvPacketProcessor) State() db.Ibcv2RelayStatus {
	return db.Ibcv2RelayStatusDELIVERRECVPACKET
}
