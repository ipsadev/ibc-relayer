package ibcv2

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/stellar/go-stellar-sdk/clients/rpcclient"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/txnbuild"

	"github.com/cosmos/ibc-relayer/shared/config"
	"github.com/cosmos/ibc-relayer/shared/signing"
)

// ErrStellarNotImplemented marks a BridgeClient method that the Stellar client
// does not answer yet.
var ErrStellarNotImplemented = errors.New("not implemented for stellar")

// StellarRPC is the subset of the Soroban RPC surface the bridge client uses.
type StellarRPC interface {
	GetLatestLedger(ctx context.Context) (protocol.GetLatestLedgerResponse, error)
	GetTransaction(ctx context.Context, req protocol.GetTransactionRequest) (protocol.GetTransactionResponse, error)
	GetLedgers(ctx context.Context, req protocol.GetLedgersRequest) (protocol.GetLedgersResponse, error)
	GetEvents(ctx context.Context, req protocol.GetEventsRequest) (protocol.GetEventsResponse, error)
	LoadAccount(ctx context.Context, address string) (txnbuild.Account, error)
	SimulateTransaction(ctx context.Context, req protocol.SimulateTransactionRequest) (protocol.SimulateTransactionResponse, error)
	SendTransaction(ctx context.Context, req protocol.SendTransactionRequest) (protocol.SendTransactionResponse, error)
}

// StellarBridgeClient relays IBC v2 packets on Stellar. Proofs and headers come
// from the gateway; this client only submits and answers chain queries.
type StellarBridgeClient struct {
	chainID        string
	cfg            *config.StellarConfig
	rpc            StellarRPC
	signer         *signing.LocalStellarSigner
	submissionLock *sync.Mutex
}

// NewStellarBridgeClient builds a Stellar bridge client for a configured chain.
func NewStellarBridgeClient(
	chainID string,
	cfg *config.StellarConfig,
	rpc StellarRPC,
	signer *signing.LocalStellarSigner,
) (*StellarBridgeClient, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("stellar chain %s: %w", chainID, err)
	}
	if rpc == nil {
		rpc = rpcclient.NewClient(cfg.RPC, nil)
	}
	return &StellarBridgeClient{
		chainID:        chainID,
		cfg:            cfg,
		rpc:            rpc,
		signer:         signer,
		submissionLock: new(sync.Mutex),
	}, nil
}

func (*StellarBridgeClient) ChainType() config.ChainType {
	return config.ChainTypeStellar
}

func (c *StellarBridgeClient) WaitForChain(ctx context.Context) error {
	_, err := c.rpc.GetLatestLedger(ctx)
	return err
}

func (c *StellarBridgeClient) LatestOnChainTimestamp(ctx context.Context) (time.Time, error) {
	latest, err := c.rpc.GetLatestLedger(ctx)
	if err != nil {
		return time.Time{}, fmt.Errorf("getting the latest ledger: %w", err)
	}
	return time.Unix(latest.LedgerCloseTime, 0).UTC(), nil
}

func (c *StellarBridgeClient) TimestampAtHeight(ctx context.Context, height uint64) (time.Time, error) {
	sequence, err := ledgerSequence(height)
	if err != nil {
		return time.Time{}, err
	}

	ledgers, err := c.rpc.GetLedgers(ctx, protocol.GetLedgersRequest{
		StartLedger: sequence,
		Pagination:  &protocol.LedgerPaginationOptions{Limit: 1},
	})
	if err != nil {
		return time.Time{}, fmt.Errorf("getting ledger %d: %w", height, err)
	}
	if len(ledgers.Ledgers) == 0 {
		return time.Time{}, fmt.Errorf("ledger %d not found", height)
	}

	return time.Unix(ledgers.Ledgers[0].LedgerCloseTime, 0).UTC(), nil
}

// IsTxFinalized reports whether the transaction's ledger has closed. Stellar
// closes a ledger by externalizing one value under SCP, so a closed ledger is
// final and the offset the other chain types need does not apply.
func (c *StellarBridgeClient) IsTxFinalized(ctx context.Context, txHash string, offset *uint64) (bool, error) {
	tx, err := c.rpc.GetTransaction(ctx, protocol.GetTransactionRequest{Hash: txHash})
	if err != nil {
		return false, fmt.Errorf("getting transaction %s: %w", txHash, err)
	}

	switch tx.Status {
	case protocol.TransactionStatusNotFound:
		return false, ErrTxNotFound
	case protocol.TransactionStatusFailed, protocol.TransactionStatusSuccess:
	default:
		return false, nil
	}

	if offset == nil || *offset == 0 {
		return true, nil
	}
	return uint64(tx.LatestLedger)-uint64(tx.Ledger) >= *offset, nil
}

func (c *StellarBridgeClient) IsTimestampFinalized(ctx context.Context, timestamp time.Time, _ *uint64) (bool, error) {
	latest, err := c.LatestOnChainTimestamp(ctx)
	if err != nil {
		return false, err
	}
	return !latest.Before(timestamp), nil
}

func (c *StellarBridgeClient) WaitForTx(ctx context.Context, hash string) error {
	_, err := c.rpc.GetTransaction(ctx, protocol.GetTransactionRequest{Hash: hash})
	return err
}

func (c *StellarBridgeClient) TxExecutionStatus(ctx context.Context, txHash string) (TxExecutionStatus, error) {
	tx, err := c.rpc.GetTransaction(ctx, protocol.GetTransactionRequest{Hash: txHash})
	if err != nil {
		return TxExecutionStatus{}, fmt.Errorf("getting transaction %s: %w", txHash, err)
	}

	switch tx.Status {
	case protocol.TransactionStatusSuccess:
		return TxExecutionStatus{Status: TxExecutionStatusSuccess}, nil
	case protocol.TransactionStatusFailed:
		return TxExecutionStatus{
			Status:       TxExecutionStatusFailed,
			ErrorMessage: tx.ResultXDR,
		}, nil
	default:
		return TxExecutionStatus{}, ErrTxNotFound
	}
}

func (c *StellarBridgeClient) ShouldRetryTx(
	ctx context.Context,
	txHash string,
	expiry time.Duration,
	sentTs time.Time,
) (bool, error) {
	tx, err := c.rpc.GetTransaction(ctx, protocol.GetTransactionRequest{Hash: txHash})
	if err != nil {
		return false, fmt.Errorf("getting transaction %s: %w", txHash, err)
	}

	if tx.Status != protocol.TransactionStatusNotFound {
		return false, nil
	}

	latest, err := c.LatestOnChainTimestamp(ctx)
	if err != nil {
		return false, err
	}
	if latest.Sub(sentTs) < expiry {
		return false, ErrTxNotFound
	}
	return true, nil
}

func (c *StellarBridgeClient) SignerGasTokenBalance(context.Context) (*big.Int, error) {
	return nil, ErrStellarNotImplemented
}

func (c *StellarBridgeClient) SendTransfer(
	context.Context, string, string, string, string, *big.Int, string, time.Duration,
) (string, error) {
	return "", ErrStellarNotImplemented
}

func (c *StellarBridgeClient) IFTTransfer(
	context.Context, string, string, string, *big.Int, time.Duration,
) (string, error) {
	return "", ErrStellarNotImplemented
}

func ledgerSequence(height uint64) (uint32, error) {
	if height > uint64(^uint32(0)) {
		return 0, fmt.Errorf("ledger %d does not fit in a stellar sequence", height)
	}
	return uint32(height), nil
}
