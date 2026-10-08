package ibcv2

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/zap"

	"github.com/cosmos/ibc-relayer/db/gen/db"
	bridge "github.com/cosmos/ibc-relayer/shared/bridges/ibcv2"
	"github.com/cosmos/ibc-relayer/shared/config"
	"github.com/cosmos/ibc-relayer/shared/lmt"
	"github.com/cosmos/ibc-relayer/shared/metrics"
)

type SubmittedTxCostStorage interface {
	GetUnresolvedIBCV2RelayerTxSubmissions(ctx context.Context, limit int32) ([]db.Ibcv2RelayerTxSubmission, error)
	UpdateIBCV2RelayerTxSubmissionTracking(ctx context.Context, arg db.UpdateIBCV2RelayerTxSubmissionTrackingParams) error
	ExpirePendingIBCV2RelayerTxSubmission(ctx context.Context, id int32) error
}

type SubmittedTxCostTracker struct {
	storage             SubmittedTxCostStorage
	bridgeClientManager BridgeClientManager
	priceClient         PriceClient
	pollInterval        time.Duration
	batchSize           int32
	attemptExpiry       time.Duration
}

func NewSubmittedTxCostTracker(
	storage SubmittedTxCostStorage,
	bridgeClientManager BridgeClientManager,
	priceClient PriceClient,
	pollInterval time.Duration,
	batchSize int32,
	attemptExpiry time.Duration,
) *SubmittedTxCostTracker {
	if pollInterval <= 0 {
		pollInterval = 5 * time.Second
	}
	if batchSize <= 0 {
		batchSize = 100
	}

	return &SubmittedTxCostTracker{
		storage:             storage,
		bridgeClientManager: bridgeClientManager,
		priceClient:         priceClient,
		pollInterval:        pollInterval,
		batchSize:           batchSize,
		attemptExpiry:       attemptExpiry,
	}
}

func (t *SubmittedTxCostTracker) Run(ctx context.Context) error {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			ticker.Stop()

			if err := t.Track(ctx); err != nil {
				lmt.Logger(ctx).Error("error tracking submitted tx costs", zap.Error(err))
			}

			ticker.Reset(t.pollInterval)
		}
	}
}

func (t *SubmittedTxCostTracker) Track(ctx context.Context) error {
	submissions, err := t.storage.GetUnresolvedIBCV2RelayerTxSubmissions(ctx, t.batchSize)
	if err != nil {
		return fmt.Errorf("getting unresolved submitted txs: %w", err)
	}

	for _, submission := range submissions {
		if err := t.trackSubmission(ctx, submission); err != nil {
			lmt.Logger(ctx).Debug(
				"submitted tx gas cost not ready yet",
				zap.Error(err),
				zap.Int32("submission_id", submission.ID),
				zap.String("tx_hash", submission.TxHash),
				zap.String("chain_id", submission.ChainID),
			)
		}
	}

	return nil
}

// trackSubmission resolves a single ATTEMPTED submission to its final state.
//
// The method first checks whether the submission has exceeded attemptExpiry
// (measured from SubmittedAt). If so it marks the row EXPIRED and returns —
// this is a backstop for submissions whose tx was dropped or whose chain
// became unreachable, preventing them from being polled indefinitely.
//
// Otherwise it queries the chain for the tx's execution status and fee, then
// writes the resolved state (SUCCESS/FAILED, gas cost, error message) in a
// single update. Any error returned here is non-fatal: the caller logs it
// and moves on to the next submission; the unresolved row will be retried on
// the next poll cycle.
//
// A nil gasCostAmount (when TxFee returns nil) is normalised to 0 by
// resolveGasCosts, so the DB row always receives a valid numeric. When
// gasCostAmount is non-nil but bigIntToNumeric fails the submission is left
// unresolved for the next cycle rather than writing a partial update.
func (t *SubmittedTxCostTracker) trackSubmission(ctx context.Context, submission db.Ibcv2RelayerTxSubmission) error {
	isExpired := submission.Status == db.Ibcv2RelayerTxSubmissionStatusPENDING && submission.SubmittedAt.Valid && submission.SubmittedAt.Time.Add(t.attemptExpiry).Before(time.Now().UTC())
	if isExpired {
		if err := t.storage.ExpirePendingIBCV2RelayerTxSubmission(ctx, submission.ID); err != nil {
			return fmt.Errorf("marking submission %d expired: %w", submission.ID, err)
		}
		return nil
	}

	client, err := t.bridgeClientManager.GetClient(ctx, submission.ChainID)
	if err != nil {
		return fmt.Errorf("getting bridge client for chain %s: %w", submission.ChainID, err)
	}

	executionStatus, err := client.TxExecutionStatus(ctx, submission.TxHash)
	if err != nil {
		return fmt.Errorf("getting execution status for tx %s on chain %s: %w", submission.TxHash, submission.ChainID, err)
	}

	gasCostAmount, gasCostUSD, err := t.resolveGasCosts(ctx, submission, client)
	if err != nil {
		return fmt.Errorf("resolving gas costs for tx %s on chain %s: %w", submission.TxHash, submission.ChainID, err)
	}

	var gasCostNumeric pgtype.Numeric
	if gasCostAmount != nil {
		gasCostNumeric, err = bigIntToNumeric(gasCostAmount)
		if err != nil {
			return fmt.Errorf("converting gas cost to numeric for submission %d: %w", submission.ID, err)
		}
	}

	update := db.UpdateIBCV2RelayerTxSubmissionTrackingParams{
		GasCostAmount: gasCostNumeric,
		GasCostUsd:    gasCostUSD,
		Status:        db.Ibcv2RelayerTxSubmissionStatus(executionStatus.Status),
		ExecutionError: pgtype.Text{
			Valid:  executionStatus.ErrorMessage != "",
			String: executionStatus.ErrorMessage,
		},
		ID: submission.ID,
	}
	if err := t.storage.UpdateIBCV2RelayerTxSubmissionTracking(ctx, update); err != nil {
		return fmt.Errorf("updating gas costs for submission %d: %w", submission.ID, err)
	}

	t.recordResolvedSubmissionMetrics(ctx, submission, executionStatus.Status, gasCostAmount)

	return nil
}

// resolveGasCosts returns the native gas cost and its USD equivalent for a
// submitted tx.
//
// Return value semantics:
//   - (*big.Int, Numeric, nil) — success. The big.Int is always non-nil (a nil
//     TxFee result is normalised to 0). The Numeric may be invalid (Valid=false)
//     when a USD conversion is unavailable; this is intentional so the caller
//     can still persist the native amount.
//   - (nil, {}, error) — TxFee failed; the submission should not be resolved
//     yet and will be retried on the next poll cycle.
//
// USD conversion failures are treated as non-fatal: the method logs a warning
// and returns the native amount with an invalid Numeric for USD, allowing the
// submission to be resolved without a price. This avoids blocking resolution
// when CoinGecko is down or a gas token has no configured coingecko ID.
func (t *SubmittedTxCostTracker) resolveGasCosts(
	ctx context.Context,
	submission db.Ibcv2RelayerTxSubmission,
	client bridge.BridgeClient,
) (*big.Int, pgtype.Numeric, error) {
	gasCostAmount, err := client.TxFee(ctx, submission.TxHash)
	if err != nil {
		return nil, pgtype.Numeric{}, fmt.Errorf("unable to get native gas cost: %w", err)
	}
	if gasCostAmount == nil {
		gasCostAmount = big.NewInt(0)
	}

	gasCostUSD, err := t.getGasCostUSD(ctx, submission.ChainID, gasCostAmount)
	if err != nil {
		lmt.Logger(ctx).Warn("unable to get usd gas cost, continuing without",
			zap.Error(err),
			zap.String("execution_chain_id", submission.ChainID),
			zap.String("tx_hash", submission.TxHash),
		)
		return gasCostAmount, pgtype.Numeric{}, nil
	}

	return gasCostAmount, gasCostUSD, nil
}

func (*SubmittedTxCostTracker) recordResolvedSubmissionMetrics(ctx context.Context, submission db.Ibcv2RelayerTxSubmission, executionStatus string, gasCostAmount *big.Int) {
	chainConfig, _ := chainConfigOrFallback(ctx, submission.ChainID)

	metrics.FromContext(ctx).AddTransactionConfirmed(
		executionStatus == "SUCCESS",
		submission.ChainID,
		chainConfig.ChainName,
		string(chainConfig.Environment),
	)

	if gasCostAmount == nil {
		return
	}

	metrics.FromContext(ctx).AddTransactionGasCost(
		submission.ChainID,
		chainConfig.ChainName,
		string(chainConfig.Environment),
		*gasCostAmount,
		chainConfig.GasTokenDecimals,
	)
}

// getGasCostUSD converts a native gas cost amount to its USD value.
//
// Returns (Valid=true, Int=0) when amount is zero — there is nothing to price.
//
// Returns (Valid=false) with no error in two cases where a USD price cannot be
// determined but this is expected and should not block submission resolution:
//   - priceClient is nil (no price provider configured, e.g. in e2e tests).
//   - The chain's gas token has no CoinGecko ID or zero decimals configured.
//
// In both cases the caller receives an invalid Numeric which is written to the
// DB as NULL for gas_cost_usd, distinguishing "no price available" from "price
// is $0".
func (t *SubmittedTxCostTracker) getGasCostUSD(ctx context.Context, chainID string, amount *big.Int) (pgtype.Numeric, error) {
	if amount.Sign() == 0 {
		return pgtype.Numeric{Valid: true, Int: big.NewInt(0)}, nil
	}
	if t.priceClient == nil {
		return pgtype.Numeric{}, nil
	}

	chainConfig, err := config.GetConfigReader(ctx).GetChainConfig(chainID)
	if err != nil {
		return pgtype.Numeric{}, fmt.Errorf("getting chain config for chain %s: %w", chainID, err)
	}
	if chainConfig.GasTokenCoingeckoID == nil || *chainConfig.GasTokenCoingeckoID == "" || chainConfig.GasTokenDecimals == 0 {
		return pgtype.Numeric{}, nil
	}

	return t.priceClient.GetCoinUsdValue(ctx, *chainConfig.GasTokenCoingeckoID, chainConfig.GasTokenDecimals, amount)
}

func bigIntToNumeric(value *big.Int) (pgtype.Numeric, error) {
	numeric := pgtype.Numeric{Valid: true}
	if err := numeric.Scan(value.String()); err != nil {
		return pgtype.Numeric{}, err
	}
	return numeric, nil
}
