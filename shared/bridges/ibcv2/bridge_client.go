package ibcv2

import (
	"context"
	"crypto/sha256"
	"errors"
	"math/big"
	"time"

	"github.com/cosmos/ibc-relayer/db/gen/db"
	"github.com/cosmos/ibc-relayer/shared/config"
)

type BridgeTx struct {
	Hash           string
	Timestamp      time.Time
	RelayerAddress string
}

type TxExecutionStatus struct {
	Status       string
	ErrorMessage string
}

const (
	TxExecutionStatusSuccess = "SUCCESS"
	TxExecutionStatusFailed  = "FAILED"
)

type PacketInfo struct {
	Sequence          uint64
	SourceClient      string
	DestinationClient string
	TimeoutTimestamp  time.Time
	Timestamp         time.Time
	Transfer          *TransferPayload
}

type ClientState struct {
	WasmClientState       *WasmClientState
	TendermintClientState *TendermintClientState
}

type WasmClientState struct {
	LatestExecutionBlockNumber uint64 `json:"latest_execution_block_number"`
}

type TendermintClientState struct {
	TrustingPeriod time.Duration
	LatestHeight   uint64
}

var (
	ErrTxNotFound                = errors.New("tx not found")
	ErrWriteAckDecoding          = errors.New("could not decode write ack")
	ErrWriteAckNotFoundForPacket = errors.New("write ack for packet not found in tx")
	ErrNoLaterBlocks             = errors.New("no blocks with a timestamp later than the provided timestamp")

	ErrorAcknowledgement = sha256.Sum256([]byte("UNIVERSAL_ERROR_ACKNOWLEDGEMENT"))
)

type BridgeClient interface {
	IsPacketReceived(ctx context.Context, clientID string, sequence uint64) (bool, error)
	IsPacketCommitted(ctx context.Context, clientID string, sequence uint64) (bool, error)
	FindRecvTx(ctx context.Context, sourceClientID string, destClientID string, sequence uint64, timeoutTimestamp time.Time) (*BridgeTx, error)
	FindAckTx(ctx context.Context, sourceClientID string, destClientID string, sequence uint64) (*BridgeTx, error)
	FindTimeoutTx(ctx context.Context, sourceClientID string, destClientID string, sequence uint64) (*BridgeTx, error)
	DeliverTx(ctx context.Context, tx []byte, address string) (*BridgeTx, error)

	// PacketWriteAckStatus returns the ack status of the packet within a write
	// ack tx hash. The status is either SUCCESS, ERROR, or UNKNOWN if the
	// status cannot be determined.
	//
	// If the tx with hash cannot be found, ErrTxNotFound is returned. If write
	// ack event for the specified packet is not found,
	// ErrWriteAckNotFoundForPacket is returned. If the write ack data cannot
	// be decoded into the expected success or error types, ErrWriteAckDecoding
	// is returned.
	PacketWriteAckStatus(ctx context.Context, hash string, sequence uint64, sourceClientID string, destClientID string) (db.Ibcv2WriteAckStatus, error)
	SendPacketsFromTx(ctx context.Context, sourceChainID string, txHash string) ([]*PacketInfo, error)

	// ShouldRetryTx returns if a tx hash has timed out before successfully
	// landing on chain and should be retried. If the tx is not yet found on
	// chain, but it may be available in the future and shouldnt yet be
	// retried, (false, ErrTxNotFound) will be returned.
	ShouldRetryTx(ctx context.Context, txHash string, expiry time.Duration, sentTs time.Time) (bool, error)
	WaitForChain(ctx context.Context) error
	// IsTxFinalized checks if a transaction is finalized. If offset is nil, the
	// chain's native finality mechanism is used (e.g., the 'finalized' block tag
	// for EVM). If offset is non-nil, the transaction is considered finalized if
	// (latest_block - tx_block) >= *offset.
	IsTxFinalized(ctx context.Context, txHash string, offset *uint64) (bool, error)
	// IsTimestampFinalized checks if there is a finalized block with timestamp >= the
	// given timestamp. This is used for timeout finality checks where we need to verify
	// the destination chain has finalized past the packet's timeout timestamp.
	// If offset is nil, uses the chain's native finality mechanism. If offset is non-nil,
	// uses (latest_block - offset) as the finalized block.
	IsTimestampFinalized(ctx context.Context, timestamp time.Time, offset *uint64) (bool, error)
	ChainType() config.ChainType

	LatestOnChainTimestamp(ctx context.Context) (time.Time, error)
	TimestampAtHeight(ctx context.Context, height uint64) (time.Time, error)

	SignerGasTokenBalance(ctx context.Context) (*big.Int, error)

	TxFee(ctx context.Context, txHash string) (*big.Int, error)
	TxExecutionStatus(ctx context.Context, txHash string) (TxExecutionStatus, error)

	SendTransfer(ctx context.Context, ics20Address string, clientID string, denom string, receiver string, amount *big.Int, memo string, timeout time.Duration) (string, error)
	IFTTransfer(ctx context.Context, iftAddress string, clientID string, receiver string, amount *big.Int, timeout time.Duration) (string, error)

	ClientState(ctx context.Context, clientID string) (ClientState, error)

	WaitForTx(ctx context.Context, hash string) error

	GetTransactionSender(ctx context.Context, hash string) (string, error)
}
