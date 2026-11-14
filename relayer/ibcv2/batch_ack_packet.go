package ibcv2

import (
	"context"
	"encoding/hex"
	"errors"
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

type TransferAckTxWithTxStorage interface {
	TransferAckTxStorage
	ExecTx(ctx context.Context, fn func(q *db.Queries) error) error
}

type BatchAckPacketProcessor struct {
	bridgeClientManager        BridgeClientManager
	transferAckTxWithTxStorage TransferAckTxWithTxStorage
	relayService               proofapi.ProofApiServiceClient
	sourceChainID              string
	sourceClientID             string
	destinationChainID         string
	destinationClientID        string
	shouldRelaySuccessAcks     bool
	shouldRelayErrorAcks       bool
}

func NewBatchAckPacketProcessor(
	bridgeClientmanager BridgeClientManager,
	transferAckTxWithTxStorage TransferAckTxWithTxStorage,
	relayService proofapi.ProofApiServiceClient,
	sourceChainID string,
	sourceClientID string,
	destinationChainID string,
	destinationClientID string,
	shouldRelaySuccessAcks bool,
	shouldRelayErrorAcks bool,
) BatchAckPacketProcessor {
	return BatchAckPacketProcessor{
		bridgeClientManager:        bridgeClientmanager,
		transferAckTxWithTxStorage: transferAckTxWithTxStorage,
		relayService:               relayService,
		sourceChainID:              sourceChainID,
		sourceClientID:             sourceClientID,
		destinationChainID:         destinationChainID,
		destinationClientID:        destinationClientID,
		shouldRelaySuccessAcks:     shouldRelaySuccessAcks,
		shouldRelayErrorAcks:       shouldRelayErrorAcks,
	}
}

// Process uses the ibc ibcv2 proof relayer to get the tx bytes that should be
// submitted on the source chain for the ack packet
func (p BatchAckPacketProcessor) Process(ctx context.Context, transfers []*IBCV2Transfer) ([]*IBCV2Transfer, error) {
	// get tx bytes and packet sequence number for all transfers in batch
	txIDs := make([][]byte, 0, len(transfers))
	sequences := make([]uint64, 0, len(transfers))
	txSet := make(map[string]struct{})
	for _, transfer := range transfers {
		writeAckTxHash, ok := transfer.GetWriteAckTxHash()
		if !ok {
			transfer.ProcessingError = errors.New("trying to deliver ack packet, no write ack tx hash found")
			continue
		}
		if _, ok := txSet[writeAckTxHash]; ok {
			sequences = append(sequences, uint64(transfer.GetPacketSequenceNumber()))
			continue
		}

		txID, err := hex.DecodeString(strings.TrimPrefix(writeAckTxHash, "0x"))
		if err != nil {
			err = fmt.Errorf("decoding hex write ack tx hash %s to bytes: %w", writeAckTxHash, err)
			transfer.ProcessingError = err
			continue
		}

		txIDs = append(txIDs, txID)
		sequences = append(sequences, uint64(transfer.GetPacketSequenceNumber()))
		txSet[writeAckTxHash] = struct{}{}
	}

	logger := lmt.Logger(ctx).With(zap.Int("num_tranfers", len(sequences)), zap.Any("sequences", sequences))
	logger.Info("processing batch ack submission")

	relayByTxReqStartTs := time.Now()
	req := &proofapi.RelayByTxRequest{
		SrcChain:           p.destinationChainID,
		DstChain:           p.sourceChainID,
		SourceTxIds:        txIDs,
		DstClientId:        p.sourceClientID,
		SrcClientId:        p.destinationClientID,
		DstPacketSequences: sequences,
	}
	resp, err := p.relayService.RelayByTx(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("getting ack tx bytes from relay service for batch of %d transfers: %w", len(sequences), err)
	}

	dur := time.Since(relayByTxReqStartTs)
	logger.Info(
		fmt.Sprintf("ibc relayer returned %d ack tx bytes in %s", len(resp.GetTx()), dur),
		zap.Duration("duration", dur),
		zap.Int("num_tx_bytes", len(resp.GetTx())),
	)

	ackTxBytes := resp.GetTx()
	to := resp.GetAddress()

	sourceChainClient, err := p.bridgeClientManager.GetClient(ctx, p.sourceChainID)
	if err != nil {
		return nil, fmt.Errorf("getting client for transfer source chain %s: %w", p.sourceChainID, err)
	}

	if sourceChainClient.ChainType() == config.ChainTypeEVM {
		// for evm chains, we must wait until the chain has caught up to the
		// current time before estimating the gas of the tx during delivery or
		// else it will revert
		waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		if err = sourceChainClient.WaitForChain(waitCtx); err != nil {
			return nil, fmt.Errorf("waiting for chain: %w", err)
		}
	}

	// submit the ack on the source chain
	ackTx, err := sourceChainClient.DeliverTx(ctx, ackTxBytes, to)
	if err != nil {
		metrics.RecordTransactionSubmitted(ctx, false, p.sourceChainID)
		return nil, fmt.Errorf("signing and submitting ack tx bytes for batch of %d transfers: %w", len(sequences), err)
	}
	metrics.RecordTransactionSubmitted(ctx, true, p.sourceChainID)

	logger.Info("delivered batch ack tx", zap.String("tx_hash", ackTx.Hash))

	submittedTransferCount := 0
	err = p.transferAckTxWithTxStorage.ExecTx(ctx, func(q *db.Queries) error {
		for _, transfer := range transfers {
			if transfer.ProcessingError != nil {
				continue
			}
			submittedTransferCount++

			update := db.UpdateTransferAckTxParams{
				AckTxHash:            pgtype.Text{Valid: true, String: ackTx.Hash},
				AckTxTime:            pgtype.Timestamp{Valid: true, Time: ackTx.Timestamp.UTC()},
				AckTxRelayerAddress:  pgtype.Text{Valid: true, String: ackTx.RelayerAddress},
				SourceChainID:        transfer.GetSourceChainID(),
				PacketSourceClientID: transfer.GetPacketSourceClientID(),
				PacketSequenceNumber: int32(transfer.GetPacketSequenceNumber()), //nolint:gosec // G115 bounded conversion
			}
			if err = q.UpdateTransferAckTx(ctx, update); err != nil {
				return fmt.Errorf(
					"updating transfer from source chain %s packet sequence %d and source client %s with ack hash %s: %w",
					transfer.GetSourceChainID(),
					transfer.GetPacketSequenceNumber(),
					transfer.GetPacketSourceClientID(),
					ackTx.Hash,
					err,
				)
			}
		}

		if submittedTransferCount == 0 {
			return nil
		}

		insert := db.InsertIBCV2RelayerTxSubmissionParams{
			TxHash:         ackTx.Hash,
			ChainID:        p.sourceChainID,
			TxType:         db.Ibcv2RelayerTxSubmissionTypeACKPACKET,
			RelayerAddress: ackTx.RelayerAddress,
			SubmittedAt:    pgtype.Timestamptz{Valid: true, Time: ackTx.Timestamp},
			Status:         db.Ibcv2RelayerTxSubmissionStatusPENDING,
		}
		submissionID, err := q.InsertIBCV2RelayerTxSubmission(ctx, insert)
		if err != nil {
			return fmt.Errorf("inserting ack tx submission %s: %w", ackTx.Hash, err)
		}
		if err = insertTransferTxSubmissionMappings(ctx, q, submissionID, transfers); err != nil {
			return fmt.Errorf("inserting ack tx submission mappings for %s: %w", ackTx.Hash, err)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("updating batch transfers ack txs: %w", err)
	}

	for _, transfer := range transfers {
		if transfer.ProcessingError != nil {
			continue
		}

		transfer.AckTxHash = &ackTx.Hash
		transfer.AckTxTime = &ackTx.Timestamp
	}
	return transfers, nil
}

func (BatchAckPacketProcessor) Cancel(transfers []*IBCV2Transfer, err error) {
	// log error, mark packet as failed if fatal error and we cannot retry, if
	// not fatal error, do nothing so it will be retried by this stage
	for _, transfer := range transfers {
		writeAckTxHash, _ := transfer.GetWriteAckTxHash()
		transfer.GetLogger().Error(
			"error submitting batch ack",
			zap.String("write_ack_tx_hash", writeAckTxHash),
			zap.Error(err),
		)
	}
}

// ShouldProcess determines when this processor should be run.
func (p BatchAckPacketProcessor) ShouldProcess(transfer *IBCV2Transfer) bool {
	// the info we need in the transfer in order to run this step
	_, hasWriteAckTxHash := transfer.GetWriteAckTxHash()
	if !hasWriteAckTxHash {
		return false
	}

	writeAckStatus, hasWriteAckStatus := transfer.GetWriteAckStatus()
	if !hasWriteAckStatus {
		// err on the side of relaying the packet instead of not processing
		// here
		transfer.GetLogger().Warn("this is a bug! transfer does not have a write ack status when delivering ack packet")
		return false
	}

	isErrorAck := writeAckStatus == db.Ibcv2WriteAckStatusERROR || writeAckStatus == db.Ibcv2WriteAckStatusUNKNOWN
	isSuccessAck := writeAckStatus == db.Ibcv2WriteAckStatusSUCCESS
	if (isErrorAck && !p.shouldRelayErrorAcks) || (isSuccessAck && !p.shouldRelaySuccessAcks) {
		return false
	}

	// the data we are going to populate with this step, no need to run if this
	// info is already present
	_, hasAckTxHash := transfer.GetAckTxHash()

	// dont need to process if the transfer already has a timeout submitted for
	// it. however we are not checking if the transfer is past its timeout
	// timestamp, since if it is we should still run if a write ack tx hash exists.
	_, hasTimeoutTxHash := transfer.GetTimeoutTxHash()

	return !hasAckTxHash && !hasTimeoutTxHash
}

func (BatchAckPacketProcessor) State() db.Ibcv2RelayStatus {
	return db.Ibcv2RelayStatusDELIVERACKPACKET
}
