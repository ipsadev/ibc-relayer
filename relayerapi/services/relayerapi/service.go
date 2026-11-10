package relayerapi

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/cosmos/ibc-relayer/db/gen/db"
	protorelayerapi "github.com/cosmos/ibc-relayer/proto/gen/relayerapi"
	"github.com/cosmos/ibc-relayer/relayer/ibcv2"
	bridgeibcv2 "github.com/cosmos/ibc-relayer/shared/bridges/ibcv2"
	"github.com/cosmos/ibc-relayer/shared/config"
	"github.com/cosmos/ibc-relayer/shared/lmt"
	"github.com/cosmos/ibc-relayer/shared/metrics"
	"github.com/cosmos/ibc-relayer/shared/utils"
)

var errAlreadySubmitted = errors.New("tx already submitted")

const (
	ErrCodeDuplicateKey = "23505"
)

//nolint:revive // RelayerAPIService is the canonical name across the codebase
type RelayerAPIService struct {
	protorelayerapi.UnsafeRelayerApiServiceServer
	config              config.ConfigReader
	db                  RelayerAPIQueries
	bridgeClientManager ibcv2.BridgeClientManager
}

//nolint:revive // RelayerAPIQueries is the canonical name across the codebase
type RelayerAPIQueries interface {
	GetTransfersBySourceTx(ctx context.Context, arg db.GetTransfersBySourceTxParams) ([]db.Ibcv2Transfer, error)
	GetRelayRequest(ctx context.Context, arg db.GetRelayRequestParams) (db.Ibcv2RelayRequest, error)
	ExecTx(ctx context.Context, fn func(q *db.Queries) error) error
}

func NewRelayerAPIService(
	ctx context.Context,
	queries RelayerAPIQueries,
	bridgeClientManager ibcv2.BridgeClientManager,
) *RelayerAPIService {
	return &RelayerAPIService{
		db:                  queries,
		config:              config.GetConfigReader(ctx),
		bridgeClientManager: bridgeClientManager,
	}
}

func (s *RelayerAPIService) Status(
	ctx context.Context,
	request *protorelayerapi.StatusRequest,
) (*protorelayerapi.StatusResponse, error) {
	if request.GetTxHash() == "" || request.GetChainId() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "tx_hash and chain_id are required")
	}

	// First check if this tx was ever submitted to the relay endpoint
	_, err := s.db.GetRelayRequest(ctx, db.GetRelayRequestParams{
		SourceChainID: request.GetChainId(),
		SourceTxHash:  request.GetTxHash(),
	})
	if err != nil {
		// If no relay request found, return NOT_FOUND
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Errorf(codes.NotFound, "transaction not submitted to relayer")
		}
		return nil, status.Errorf(codes.Internal, "failed to query relay request")
	}

	transfers, err := s.db.GetTransfersBySourceTx(ctx, db.GetTransfersBySourceTxParams{
		SourceChainID: request.GetChainId(),
		SourceTxHash:  request.GetTxHash(),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to query transfers")
	}

	statuses := make([]*protorelayerapi.PacketStatus, 0, len(transfers))
	for _, t := range transfers {
		statuses = append(statuses, &protorelayerapi.PacketStatus{
			State:          mapDBStatusToProto(t.Status),
			SequenceNumber: uint64(t.PacketSequenceNumber), //nolint:gosec // G115 bounded conversion
			SourceClientId: t.PacketSourceClientID,
			SendTx:         &protorelayerapi.TransactionInfo{TxHash: t.SourceTxHash, ChainId: t.SourceChainID},
			RecvTx:         toTxInfo(t.RecvTxHash, t.DestinationChainID),
			AckTx:          toTxInfo(t.AckTxHash, t.SourceChainID),
			TimeoutTx:      toTxInfo(t.TimeoutTxHash, t.SourceChainID),
		})
	}
	return &protorelayerapi.StatusResponse{PacketStatuses: statuses}, nil
}

func (s *RelayerAPIService) Relay(
	ctx context.Context,
	request *protorelayerapi.RelayRequest,
) (*protorelayerapi.RelayResponse, error) {
	if request.GetTxHash() == "" || request.GetChainId() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "tx_hash and chain_id are required")
	}

	sourceChainID := request.GetChainId()
	txHash := request.GetTxHash()

	lmt.Logger(ctx).Debug("received relay request",
		zap.String("tx_hash", txHash),
		zap.String("chain_id", sourceChainID),
	)

	client, err := s.bridgeClientManager.GetClient(ctx, sourceChainID)
	if err != nil {
		metrics.FromContext(ctx).AddRelayRequest(metrics.IBCV2BridgeType, uint32(codes.InvalidArgument))
		return nil, status.Errorf(codes.InvalidArgument, "unsupported chain: %s", sourceChainID)
	}

	execStatus, err := client.TxExecutionStatus(ctx, txHash)
	if errors.Is(err, bridgeibcv2.ErrTxNotFound) {
		metrics.FromContext(ctx).AddRelayRequest(metrics.IBCV2BridgeType, uint32(codes.NotFound))
		return nil, status.Errorf(codes.NotFound, "tx %s not found on chain %s", txHash, sourceChainID)
	}
	if err != nil {
		metrics.FromContext(ctx).AddRelayRequest(metrics.IBCV2BridgeType, uint32(codes.Unavailable))
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	if execStatus.Status != bridgeibcv2.TxExecutionStatusSuccess {
		metrics.FromContext(ctx).AddRelayRequest(metrics.IBCV2BridgeType, uint32(codes.FailedPrecondition))
		return nil, status.Errorf(codes.FailedPrecondition, "tx %s on chain %s did not succeed on chain: %s", txHash, sourceChainID, execStatus.ErrorMessage)
	}

	if client.ChainType() == config.ChainTypeEVM {
		transactionSender, err := client.GetTransactionSender(ctx, txHash)
		if err != nil {
			return nil, fmt.Errorf("checking OFAC Blacklist - error getting transaction sender: %w", err)
		}
		if _, isBlacklisted := utils.OFACAddressMap[strings.ToLower(transactionSender)]; isBlacklisted {
			return nil, status.Error(codes.InvalidArgument, "Transaction sender is blacklisted - cannot relay")
		}
	}

	packets, err := client.SendPacketsFromTx(ctx, sourceChainID, txHash)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get packets from tx: %v", err)
	}

	lmt.Logger(ctx).Info("found packets in transaction",
		zap.String("tx_hash", txHash),
		zap.Int("packet_count", len(packets)),
	)

	sourceFee, sourceFeeDenom, hasSourceFee := s.sourceTransferFee(ctx, client, sourceChainID, txHash)

	txErr := s.db.ExecTx(ctx, func(q *db.Queries) error {
		if err := q.InsertRelayRequest(ctx, db.InsertRelayRequestParams{
			SourceChainID: sourceChainID,
			SourceTxHash:  txHash,
		}); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == ErrCodeDuplicateKey {
				return errAlreadySubmitted
			}
			return fmt.Errorf("inserting relay request: %w", err)
		}

		for _, packet := range packets {
			destChainID, err := s.config.GetClientCounterpartyChainID(sourceChainID, packet.SourceClient)
			if err != nil {
				lmt.Logger(ctx).Warn("skipping packet with unknown destination",
					zap.String("source_client", packet.SourceClient),
					zap.Error(err),
				)
				continue
			}

			insert := db.InsertIBCV2TransferParams{
				SourceChainID:             sourceChainID,
				DestinationChainID:        destChainID,
				SourceTxHash:              txHash,
				SourceTxTime:              pgtype.Timestamp{Valid: true, Time: packet.Timestamp.UTC()},
				PacketSequenceNumber:      int32(packet.Sequence), //nolint:gosec // G115 bounded conversion
				PacketSourceClientID:      packet.SourceClient,
				PacketDestinationClientID: packet.DestinationClient,
				PacketTimeoutTimestamp:    pgtype.Timestamp{Valid: true, Time: packet.TimeoutTimestamp.UTC()},
			}

			if err := q.InsertIBCV2Transfer(ctx, insert); err != nil {
				return fmt.Errorf("inserting packet sequence %d: %w", packet.Sequence, err)
			}

			if err := s.storeTransferMetadata(ctx, q, sourceChainID, packet); err != nil {
				return fmt.Errorf("storing metadata for packet sequence %d: %w", packet.Sequence, err)
			}

			if hasSourceFee {
				if err := storeSourceFee(ctx, q, sourceChainID, packet, sourceFee, sourceFeeDenom); err != nil {
					return fmt.Errorf("storing source fee for packet sequence %d: %w", packet.Sequence, err)
				}
			}

			if packet.Transfer != nil {
				if err := storeFastFill(ctx, q, sourceChainID, packet, packet.Transfer.FastFill); err != nil {
					return fmt.Errorf("storing fast fill for packet sequence %d: %w", packet.Sequence, err)
				}
			}

			lmt.Logger(ctx).Info("submitted packet to be relayed",
				zap.Uint64("sequence", packet.Sequence),
				zap.String("source_client", packet.SourceClient),
			)
		}
		return nil
	})

	if errors.Is(txErr, errAlreadySubmitted) {
		metrics.FromContext(ctx).AddRelayRequest(metrics.IBCV2BridgeType, uint32(codes.OK))
		return &protorelayerapi.RelayResponse{}, nil
	}
	if txErr != nil {
		metrics.FromContext(ctx).AddRelayRequest(metrics.IBCV2BridgeType, uint32(codes.Internal))
		return nil, status.Errorf(codes.Internal, "failed to persist relay submission: %v", txErr)
	}

	metrics.FromContext(ctx).AddRelayRequest(metrics.IBCV2BridgeType, uint32(codes.OK))
	return &protorelayerapi.RelayResponse{}, nil
}

func mapDBStatusToProto(s db.Ibcv2RelayStatus) protorelayerapi.TransferState {
	switch s {
	case db.Ibcv2RelayStatusCOMPLETEWITHACK,
		db.Ibcv2RelayStatusCOMPLETEWITHTIMEOUT,
		db.Ibcv2RelayStatusCOMPLETEWITHWRITEACKSUCCESS,
		db.Ibcv2RelayStatusCOMPLETEWITHWRITEACKERROR:
		return protorelayerapi.TransferState_TRANSFER_STATE_COMPLETE
	case db.Ibcv2RelayStatusFAILED:
		return protorelayerapi.TransferState_TRANSFER_STATE_FAILED
	default:
		return protorelayerapi.TransferState_TRANSFER_STATE_PENDING
	}
}

// Converts  nullable DB field to proto TransactionInfo
func toTxInfo(hash pgtype.Text, chainID string) *protorelayerapi.TransactionInfo {
	if !hash.Valid {
		return nil
	}
	return &protorelayerapi.TransactionInfo{TxHash: hash.String, ChainId: chainID}
}

func (s *RelayerAPIService) storeTransferMetadata(ctx context.Context, q *db.Queries, sourceChainID string, packet *bridgeibcv2.PacketInfo) error {
	transfer := packet.Transfer
	if transfer == nil || transfer.Application == "" {
		return nil
	}

	params := db.UpdateTransferMetadataParams{
		SourceChainID:        sourceChainID,
		PacketSourceClientID: packet.SourceClient,
		PacketSequenceNumber: int32(packet.Sequence), //nolint:gosec // G115 bounded conversion
		Application:          pgtype.Text{String: transfer.Application, Valid: true},
	}

	if transfer.Sender != "" {
		params.Sender = pgtype.Text{String: transfer.Sender, Valid: true}
	}
	if transfer.Receiver != "" {
		params.Receiver = pgtype.Text{String: transfer.Receiver, Valid: true}
	}
	if transfer.Denom != "" {
		params.Denom = pgtype.Text{String: transfer.Denom, Valid: true}
		params.TokenOrigin = pgtype.Text{String: transfer.Denom, Valid: true}

		if decimals, ok := s.tokenDecimals(sourceChainID, transfer.Denom, transfer.Decimals); ok {
			params.TokenOriginDecimals = pgtype.Int4{Int32: int32(decimals), Valid: true}
		}
	}
	if transfer.Amount != "" {
		var amount pgtype.Numeric
		if err := amount.Scan(transfer.Amount); err != nil {
			return fmt.Errorf("parsing transfer amount %q: %w", transfer.Amount, err)
		}
		params.TransferAmount = amount
	}

	return q.UpdateTransferMetadata(ctx, params)
}

func (s *RelayerAPIService) tokenDecimals(chainID string, denom string, onChain *uint8) (uint8, bool) {
	if chain, err := s.config.GetChainConfig(chainID); err == nil {
		if decimals, ok := chain.TokenDecimals(denom); ok {
			return decimals, true
		}
	}

	if onChain != nil {
		return *onChain, true
	}

	return 0, false
}

func (s *RelayerAPIService) sourceTransferFee(
	ctx context.Context,
	client bridgeibcv2.BridgeClient,
	sourceChainID string,
	txHash string,
) (*big.Int, string, bool) {
	chain, err := s.config.GetChainConfig(sourceChainID)
	if err != nil || chain.GasTokenSymbol == "" {
		return nil, "", false
	}

	fee, err := client.TxFee(ctx, txHash)
	if err != nil || fee == nil {
		return nil, "", false
	}

	return fee, chain.GasTokenSymbol, true
}

func storeSourceFee(
	ctx context.Context,
	q *db.Queries,
	sourceChainID string,
	packet *bridgeibcv2.PacketInfo,
	fee *big.Int,
	denom string,
) error {
	return q.UpdateTransferSourceFee(ctx, db.UpdateTransferSourceFeeParams{
		FeeOrigin:            pgtype.Numeric{Int: fee, Valid: true},
		FeeOriginDenom:       pgtype.Text{String: denom, Valid: true},
		SourceChainID:        sourceChainID,
		PacketSourceClientID: packet.SourceClient,
		PacketSequenceNumber: int32(packet.Sequence), //nolint:gosec // G115 bounded conversion
	})
}

func storeFastFill(
	ctx context.Context,
	q *db.Queries,
	sourceChainID string,
	packet *bridgeibcv2.PacketInfo,
	fastFill bool,
) error {
	return q.UpdateTransferFastFill(ctx, db.UpdateTransferFastFillParams{
		FastFill:             pgtype.Bool{Bool: fastFill, Valid: true},
		SourceChainID:        sourceChainID,
		PacketSourceClientID: packet.SourceClient,
		PacketSequenceNumber: int32(packet.Sequence), //nolint:gosec // G115 bounded conversion
	})
}
