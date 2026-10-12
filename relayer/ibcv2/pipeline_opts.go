package ibcv2

import (
	"context"
	"fmt"
	"time"

	"github.com/cosmos/ibc-relayer/shared/config"
	"github.com/cosmos/ibc-relayer/shared/lmt"
)

const (
	DefaultShouldRelaySuccessAcks  = false
	DefaultShouldRelayErrorAcks    = true
	DefaultAckBatchSize            = 50
	DefaultAckBatchTimeout         = 10 * time.Second
	DefaultAckBatchConcurrency     = 10
	DefaultRecvBatchSize           = 50
	DefaultRecvBatchTimeout        = 10 * time.Second
	DefaultRecvBatchConcurrency    = 10
	DefaultTimeoutBatchSize        = 50
	DefaultTimeoutBatchTimeout     = 1 * time.Minute
	DefaultTimeoutBatchConcurrency = 10
)

type PipelineOptions struct {
	ShouldRelaySuccessAcks bool
	ShouldRelayErrorAcks   bool

	AckBatchSize        int
	AckBatchTimeout     time.Duration
	AckBatchConcurrency int

	RecvBatchSize        int
	RecvBatchTimeout     time.Duration
	RecvBatchConcurrency int

	TimeoutBatchSize        int
	TimeoutBatchTimeout     time.Duration
	TimeoutBatchConcurrency int

	SourceChainGasTokenCoingeckoID      string
	SourceChainGasTokenDecimals         uint8
	DestinationChainGasTokenCoingeckoID string
	DestinationChainGasTokenDecimals    uint8

	// SourceFinalityOffset is the number of blocks to wait after a transaction
	// on the source chain before considering it finalized. If nil, the relayer
	// uses the chain's native finality mechanism.
	SourceFinalityOffset *uint64
	// DestinationFinalityOffset is the number of blocks to wait after a
	// transaction on the destination chain before considering it finalized.
	// If nil, the relayer uses the chain's native finality mechanism.
	DestinationFinalityOffset *uint64
}

func NewDefaultPipelineOpts() *PipelineOptions {
	return &PipelineOptions{
		ShouldRelaySuccessAcks:              false,
		ShouldRelayErrorAcks:                true,
		AckBatchSize:                        DefaultAckBatchSize,
		AckBatchTimeout:                     DefaultAckBatchTimeout,
		AckBatchConcurrency:                 DefaultAckBatchConcurrency,
		RecvBatchSize:                       DefaultRecvBatchSize,
		RecvBatchTimeout:                    DefaultRecvBatchTimeout,
		RecvBatchConcurrency:                DefaultRecvBatchSize,
		TimeoutBatchSize:                    DefaultTimeoutBatchSize,
		TimeoutBatchTimeout:                 DefaultTimeoutBatchTimeout,
		TimeoutBatchConcurrency:             DefaultTimeoutBatchConcurrency,
		SourceChainGasTokenCoingeckoID:      "",
		SourceChainGasTokenDecimals:         0,
		DestinationChainGasTokenCoingeckoID: "",
		DestinationChainGasTokenDecimals:    0,
	}
}

func NewPipelineOpts(ctx context.Context, transfer *IBCV2Transfer) (*PipelineOptions, error) {
	var opts PipelineOptions

	sourceChainConfig, err := config.GetConfigReader(ctx).GetChainConfig(transfer.GetSourceChainID())
	if err != nil {
		return nil, fmt.Errorf("getting config for chain %s: %w", transfer.GetSourceChainID(), err)
	}

	destinationChainConfig, err := config.GetConfigReader(ctx).GetChainConfig(transfer.GetDestinationChainID())
	if err != nil {
		return nil, fmt.Errorf("getting config for chain %s: %w", transfer.GetDestinationChainID(), err)
	}

	sourceChainIBCV2Config := sourceChainConfig.IBCV2
	if sourceChainIBCV2Config == nil {
		return nil, fmt.Errorf("no ibcv2 config for source chain %s", transfer.GetSourceChainID())
	}

	destinationChainIBCV2Config := destinationChainConfig.IBCV2
	if destinationChainIBCV2Config == nil {
		return nil, fmt.Errorf("no ibcv2 config for destination chain %s", transfer.GetDestinationChainID())
	}

	shouldRelaySuccessAcks := DefaultShouldRelaySuccessAcks
	if sourceChainIBCV2Config != nil {
		shouldRelaySuccessAcks = sourceChainIBCV2Config.ShouldRelaySuccessAcks
	}
	opts.ShouldRelaySuccessAcks = shouldRelaySuccessAcks

	shouldRelayErrorAcks := DefaultShouldRelayErrorAcks
	if sourceChainIBCV2Config != nil {
		shouldRelayErrorAcks = sourceChainIBCV2Config.ShouldRelayErrorAcks
	}
	opts.ShouldRelayErrorAcks = shouldRelayErrorAcks

	ackBatchSize := DefaultAckBatchSize
	if sourceChainIBCV2Config.AckBatchSize != 0 {
		ackBatchSize = sourceChainIBCV2Config.AckBatchSize
	}
	opts.AckBatchSize = ackBatchSize

	ackBatchTimeout := DefaultAckBatchTimeout
	if sourceChainIBCV2Config.AckBatchTimeout != 0 {
		ackBatchTimeout = sourceChainIBCV2Config.AckBatchTimeout
	}
	opts.AckBatchTimeout = ackBatchTimeout

	ackBatchConcurrency := DefaultAckBatchConcurrency
	if sourceChainIBCV2Config.AckBatchConcurrency != 0 {
		ackBatchConcurrency = sourceChainIBCV2Config.AckBatchConcurrency
	}
	opts.AckBatchConcurrency = ackBatchConcurrency

	recvBatchSize := DefaultRecvBatchSize
	if destinationChainIBCV2Config.RecvBatchSize != 0 {
		recvBatchSize = destinationChainIBCV2Config.RecvBatchSize
	}
	opts.RecvBatchSize = recvBatchSize

	recvBatchTimeout := DefaultRecvBatchTimeout
	if destinationChainIBCV2Config.RecvBatchTimeout != 0 {
		recvBatchTimeout = destinationChainIBCV2Config.RecvBatchTimeout
	}
	opts.RecvBatchTimeout = recvBatchTimeout

	recvBatchConcurrency := DefaultRecvBatchConcurrency
	if destinationChainIBCV2Config.RecvBatchConcurrency != 0 {
		recvBatchConcurrency = destinationChainIBCV2Config.RecvBatchConcurrency
	}
	opts.RecvBatchConcurrency = recvBatchConcurrency

	timeoutBatchSize := DefaultTimeoutBatchSize
	if sourceChainIBCV2Config.TimeoutBatchSize != 0 {
		timeoutBatchSize = sourceChainIBCV2Config.TimeoutBatchSize
	}
	opts.TimeoutBatchSize = timeoutBatchSize

	timeoutBatchTimeout := DefaultTimeoutBatchTimeout
	if sourceChainIBCV2Config.TimeoutBatchTimeout != 0 {
		timeoutBatchTimeout = sourceChainIBCV2Config.TimeoutBatchTimeout
	}
	opts.TimeoutBatchTimeout = timeoutBatchTimeout

	timeoutBatchConcurrency := DefaultTimeoutBatchConcurrency
	if sourceChainIBCV2Config.TimeoutBatchConcurrency != 0 {
		timeoutBatchConcurrency = sourceChainIBCV2Config.TimeoutBatchConcurrency
	}
	opts.TimeoutBatchConcurrency = timeoutBatchConcurrency

	if sourceChainConfig.GasTokenCoingeckoID == nil {
		lmt.Logger(ctx).Debug("source chain has no gas token coingecko id configured, no ack tx or timeout tx gas cost info will be calculated")
	} else {
		opts.SourceChainGasTokenCoingeckoID = *sourceChainConfig.GasTokenCoingeckoID
	}

	if destinationChainConfig.GasTokenCoingeckoID == nil {
		lmt.Logger(ctx).Debug("destination chain has no gas token coingecko id configured, no recv tx gas cost info will be calculated")
	} else {
		opts.DestinationChainGasTokenCoingeckoID = *destinationChainConfig.GasTokenCoingeckoID
	}

	if sourceChainConfig.GasTokenDecimals == 0 {
		lmt.Logger(ctx).Warn("source chain has no gas token decimals configured, no ack tx or timeout tx gas cost info will be calculated")
	} else {
		opts.SourceChainGasTokenDecimals = sourceChainConfig.GasTokenDecimals
	}

	if destinationChainConfig.GasTokenDecimals == 0 {
		lmt.Logger(ctx).Warn("destination chain has no gas token decimals configured, no recv tx gas cost info will be calculated")
	} else {
		opts.DestinationChainGasTokenDecimals = destinationChainConfig.GasTokenDecimals
	}

	// Set finality offsets from config (nil means use native finality)
	opts.SourceFinalityOffset = sourceChainIBCV2Config.FinalityOffset
	opts.DestinationFinalityOffset = destinationChainIBCV2Config.FinalityOffset

	return &opts, nil
}

func NewSmallPipelineOpts() *PipelineOptions {
	return &PipelineOptions{
		ShouldRelaySuccessAcks:              false,
		ShouldRelayErrorAcks:                true,
		AckBatchSize:                        3,
		AckBatchTimeout:                     250 * time.Millisecond,
		AckBatchConcurrency:                 2,
		RecvBatchSize:                       3,
		RecvBatchTimeout:                    250 * time.Millisecond,
		RecvBatchConcurrency:                2,
		TimeoutBatchSize:                    3,
		TimeoutBatchTimeout:                 250 * time.Millisecond,
		TimeoutBatchConcurrency:             2,
		SourceChainGasTokenCoingeckoID:      "",
		SourceChainGasTokenDecimals:         0,
		DestinationChainGasTokenCoingeckoID: "",
		DestinationChainGasTokenDecimals:    0,
	}
}
