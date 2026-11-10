package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/cosmos/ibc-relayer/db/gen/db"
	"github.com/cosmos/ibc-relayer/relayer/ibcv2"
	bridgeibcv2 "github.com/cosmos/ibc-relayer/shared/bridges/ibcv2"
	"github.com/cosmos/ibc-relayer/shared/config"
	"github.com/cosmos/ibc-relayer/shared/lmt"
)

func runBackfill(ctx context.Context, cfg config.ConfigReader, manager ibcv2.BridgeClientManager, pool *pgxpool.Pool) error {
	queries := db.New(pool)

	rows, err := queries.ListTransfersToBackfill(ctx)
	if err != nil {
		return fmt.Errorf("listing transfers to backfill: %w", err)
	}

	log := lmt.Logger(ctx)
	log.Info("backfilling transfer metadata and fees", zap.Int("count", len(rows)))

	done := 0
	for _, row := range rows {
		if err := backfillTransfer(ctx, cfg, manager, queries, row); err != nil {
			log.Warn("could not fully backfill transfer",
				zap.Int32("id", row.ID),
				zap.String("source_tx_hash", row.SourceTxHash),
				zap.Error(err),
			)
			continue
		}
		done++
	}

	log.Info("backfill finished", zap.Int("updated", done), zap.Int("total", len(rows)))
	return nil
}

func backfillTransfer(ctx context.Context, cfg config.ConfigReader, manager ibcv2.BridgeClientManager, queries *db.Queries, row db.ListTransfersToBackfillRow) error {
	sourceClient, err := manager.GetClient(ctx, row.SourceChainID)
	if err != nil {
		return fmt.Errorf("source client for %s: %w", row.SourceChainID, err)
	}

	if fee, err := sourceClient.TxFee(ctx, row.SourceTxHash); err == nil && fee != nil {
		if chain, err := cfg.GetChainConfig(row.SourceChainID); err == nil && chain.GasTokenSymbol != "" {
			if err := queries.UpdateTransferSourceFee(ctx, db.UpdateTransferSourceFeeParams{
				FeeOrigin:            pgtype.Numeric{Int: fee, Valid: true},
				FeeOriginDenom:       pgtype.Text{String: chain.GasTokenSymbol, Valid: true},
				SourceChainID:        row.SourceChainID,
				PacketSourceClientID: row.PacketSourceClientID,
				PacketSequenceNumber: row.PacketSequenceNumber,
			}); err != nil {
				return fmt.Errorf("updating source fee: %w", err)
			}
		}
	}

	if packets, err := sourceClient.SendPacketsFromTx(ctx, row.SourceChainID, row.SourceTxHash); err == nil {
		if packet := matchPacket(packets, row.PacketSourceClientID, row.PacketSequenceNumber); packet != nil && packet.Transfer != nil {
			if err := backfillMetadata(ctx, cfg, queries, row, packet.Transfer); err != nil {
				return err
			}
		}
	}

	if row.RecvTxHash.Valid && row.RecvTxHash.String != "" {
		if destinationClient, err := manager.GetClient(ctx, row.DestinationChainID); err == nil {
			if fee, err := destinationClient.TxFee(ctx, row.RecvTxHash.String); err == nil && fee != nil {
				if chain, err := cfg.GetChainConfig(row.DestinationChainID); err == nil && chain.GasTokenSymbol != "" {
					if err := queries.UpdateTransferDestinationFee(ctx, db.UpdateTransferDestinationFeeParams{
						FeeDestination:       pgtype.Numeric{Int: fee, Valid: true},
						FeeDestinationDenom:  pgtype.Text{String: chain.GasTokenSymbol, Valid: true},
						SourceChainID:        row.SourceChainID,
						PacketSourceClientID: row.PacketSourceClientID,
						PacketSequenceNumber: row.PacketSequenceNumber,
					}); err != nil {
						return fmt.Errorf("updating destination fee: %w", err)
					}
				}
			}
		}
	}

	return nil
}

func backfillMetadata(ctx context.Context, cfg config.ConfigReader, queries *db.Queries, row db.ListTransfersToBackfillRow, transfer *bridgeibcv2.TransferPayload) error {
	if transfer.Application == "" {
		return nil
	}

	params := db.UpdateTransferMetadataParams{
		SourceChainID:        row.SourceChainID,
		PacketSourceClientID: row.PacketSourceClientID,
		PacketSequenceNumber: row.PacketSequenceNumber,
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

		if decimals, ok := backfillTokenDecimals(cfg, row.SourceChainID, transfer.Denom, transfer.Decimals); ok {
			params.TokenOriginDecimals = pgtype.Int4{Int32: int32(decimals), Valid: true}
		}
	}
	if transfer.Amount != "" {
		var amount pgtype.Numeric
		if err := amount.Scan(transfer.Amount); err != nil {
			return fmt.Errorf("parsing amount %q: %w", transfer.Amount, err)
		}
		params.TransferAmount = amount
	}

	if err := queries.UpdateTransferMetadata(ctx, params); err != nil {
		return fmt.Errorf("updating metadata: %w", err)
	}

	if err := queries.UpdateTransferFastFill(ctx, db.UpdateTransferFastFillParams{
		FastFill:             pgtype.Bool{Bool: transfer.FastFill, Valid: true},
		SourceChainID:        row.SourceChainID,
		PacketSourceClientID: row.PacketSourceClientID,
		PacketSequenceNumber: row.PacketSequenceNumber,
	}); err != nil {
		return fmt.Errorf("updating fast fill: %w", err)
	}

	if transfer.Denom != "" {
		if chain, err := cfg.GetChainConfig(row.SourceChainID); err == nil {
			if destination, ok := chain.TokenDestination(transfer.Denom, row.DestinationChainID); ok {
				if err := queries.UpdateTransferDestinationToken(ctx, db.UpdateTransferDestinationTokenParams{
					TokenDestination:         pgtype.Text{String: destination.Denom, Valid: destination.Denom != ""},
					TokenDestinationDecimals: pgtype.Int4{Int32: int32(destination.Decimals), Valid: true},
					SourceChainID:            row.SourceChainID,
					PacketSourceClientID:     row.PacketSourceClientID,
					PacketSequenceNumber:     row.PacketSequenceNumber,
				}); err != nil {
					return fmt.Errorf("updating destination token: %w", err)
				}
			}
		}
	}

	return nil
}

func backfillTokenDecimals(cfg config.ConfigReader, chainID string, denom string, onChain *uint8) (uint8, bool) {
	if chain, err := cfg.GetChainConfig(chainID); err == nil {
		if decimals, ok := chain.TokenDecimals(denom); ok {
			return decimals, true
		}
	}

	if onChain != nil {
		return *onChain, true
	}

	return 0, false
}

func matchPacket(packets []*bridgeibcv2.PacketInfo, clientID string, sequence int32) *bridgeibcv2.PacketInfo {
	for _, packet := range packets {
		if packet.SourceClient == clientID && int32(packet.Sequence) == sequence { //nolint:gosec // G115 bounded conversion
			return packet
		}
	}

	return nil
}

func isBackfillSubcommand() bool {
	return len(os.Args) > 1 && os.Args[1] == "backfill"
}
