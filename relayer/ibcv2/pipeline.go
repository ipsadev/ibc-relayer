package ibcv2

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"

	"github.com/deliveryhero/pipeline/v2"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/zap"

	"github.com/cosmos/ibc-relayer/proto/gen/proofapi"
	"github.com/cosmos/ibc-relayer/shared/clients/coingecko"
	"github.com/cosmos/ibc-relayer/shared/lmt"
	"github.com/cosmos/ibc-relayer/shared/metrics"
)

//nolint:revive // IBCV2Pipeline is the canonical name across the codebase
type IBCV2Pipeline interface {
	// Push pushes a transfer onto the pipelines input to be relayed, and
	// returns if the transfer was successfully accepted to be relayed or not
	Push(ctx context.Context, transfer *IBCV2Transfer) bool
	Poll() (*IBCV2Transfer, error)
	Close()
}

var _ IBCV2Pipeline = (*Pipeline[*IBCV2Transfer, *IBCV2Transfer])(nil)

// Pipeline represents a pipeline that is input an Input and outputs an Output.
// The user (caller) of the pipeline is responsible for closing the input
// channel, but not the output channel. The output channel will be closed once
// the context is cancelled and all items in the pipeline are finished
// processing.
type Pipeline[Input any, Output any] struct {
	// Input is used to send input to the pipeline. The user of the Pipeline is
	// responsible to close the Input channel.
	Input chan<- Input

	// Output receives processed Outputs from the pipeline. This will be closed
	// once the context used to create the pipeline is cancelled.
	Output <-chan Output
}

// Storage is a wrapper interface around all storage interfaces used throughout
// the pipeline
type Storage interface {
	TransferRecvTxWithTxStorage
	TransferRecvTxStorage
	TransferClearRecvTxStorage
	TransferRecvTxGasCostStorage
	TransferWriteAckTxStorage
	TransferAckTxWithTxStorage
	TransferAckTxStorage
	TransferClearAckTxStorage
	TransferAckTxGasCostStorage
	TransferTimeoutTxStorage
	TransferClearTimeoutTxStorage
	TransferTimeoutTxGasCostStorage
	TransferStateStorage
	TransferSendTxFinalityStorage
	TransferWriteAckFinalityStorage
}

type PriceClient interface {
	coingecko.PriceClient
	GetCoinUsdValue(ctx context.Context, coingeckoID string, decimals uint8, amount *big.Int) (pgtype.Numeric, error)
}

// NewPipeline creates a pipeline that performs the full ibcv2 relaying
// life cycle on transfers sent to its Input channel.
//
// Each pipeline instance is for a unique (source chain, source client, dest
// chain, dest client) pair, even though the relaying logic for all pipeline
// steps is the same, no matter the chain or client. They must be unique due to
// batching. When attempting to batch multiple packets together to submit in a
// single recv tx, the pipeline assumes that all packets are bound for the same
// dest chain + dest client (this is the same for acks, and timeouts).
func NewPipeline(
	ctx context.Context,
	storage Storage,
	bridgeClientManager BridgeClientManager,
	relayService proofapi.ProofApiServiceClient,
	priceClient PriceClient,
	sourceChainID string,
	sourceClientID string,
	destinationChainID string,
	destinationClientID string,
	pipelineOpts *PipelineOptions,
) *Pipeline[*IBCV2Transfer, *IBCV2Transfer] {
	// inputBufferSize is a high number we should never hit so we do not block
	// on inputting to the pipeline
	const inputBufferSize = 10000
	const concurrency = 10

	logger := lmt.Logger(ctx).With(
		zap.String("source_chain_id", sourceChainID),
		zap.String("source_client_id", sourceClientID),
		zap.String("destination_chain_id", destinationChainID),
		zap.String("destination_client_id", destinationClientID),
		zap.Bool("should_relay_success_acks", pipelineOpts.ShouldRelaySuccessAcks),
		zap.Bool("should_relay_error_acks", pipelineOpts.ShouldRelayErrorAcks),
		zap.Int("max_recv_batch_size", pipelineOpts.RecvBatchSize),
		zap.Duration("recv_batch_timeout", pipelineOpts.RecvBatchTimeout),
		zap.Int("max_recv_batch_concurrency", pipelineOpts.RecvBatchConcurrency),
		zap.Int("max_ack_batch_size", pipelineOpts.AckBatchSize),
		zap.Duration("ack_batch_timeout", pipelineOpts.AckBatchTimeout),
		zap.Int("max_ack_batch_concurrency", pipelineOpts.AckBatchConcurrency),
	)
	ctx = lmt.WithLogger(ctx, logger)

	input := make(chan *IBCV2Transfer, inputBufferSize)

	// pull transfers off the input channel one at a time into pipeline
	output := pipeline.Emitter(ctx, func() *IBCV2Transfer {
		return <-input
	})

	// check if the recv packet has already been delivered on the destination
	// chain, if it is, find and populate the tx hash for the recv delivery
	checkRecvPacketDeliveryProcessor := NewCheckRecvPacketDeliveryProcessor(bridgeClientManager, storage)
	output = pipeline.ProcessConcurrently(ctx, concurrency, NewIBCV2ProcessorMW(storage, checkRecvPacketDeliveryProcessor), output)

	// check if the packet commitment is found on the source chain, if it is
	// not, find and populate the either the ack tx hash or timeout tx hash. we
	// need this step here since this tx may have already received a timeout
	checkInitialPacketCommitmentProcessor := NewCheckPacketCommitmentProcessor(bridgeClientManager, storage)
	output = pipeline.ProcessConcurrently(ctx, concurrency, NewIBCV2ProcessorMW(storage, checkInitialPacketCommitmentProcessor), output)

	// check if the send packet is finalized on the source chain
	checkSendPacketFinalityProcessor := NewCheckSendFinalityProcessor(bridgeClientManager, sourceChainID, storage, pipelineOpts.SourceFinalityOffset)
	output = pipeline.ProcessConcurrently(ctx, concurrency, NewIBCV2ProcessorMW(storage, checkSendPacketFinalityProcessor), output)

	// if we need to timeout a packet, check to ensure that the packet timeout
	// timestamp is finalized on the destination chain
	checkTimeoutTimestampFinalityProcessor := NewCheckTimeoutFinalityProcessor(bridgeClientManager, pipelineOpts.DestinationFinalityOffset)
	output = pipeline.ProcessConcurrently(ctx, concurrency, NewIBCV2ProcessorMW(storage, checkTimeoutTimestampFinalityProcessor), output)

	// timeout a packet if it is past its timeout time stamp
	batchTimeoutPacketProcessor := NewBatchTimeoutPacketProcessor(bridgeClientManager, storage, relayService, sourceChainID, sourceClientID, destinationChainID, destinationClientID)
	output = ConditionallyBatchProcess(metrics.ContextWithRelayType(ctx, metrics.IBCV2SendToTimeoutRelayType), pipelineOpts.TimeoutBatchConcurrency, pipelineOpts.TimeoutBatchSize, pipelineOpts.TimeoutBatchTimeout, output, NewIBCV2BatchProcessorMW(storage, batchTimeoutPacketProcessor))

	// check if the timeout packet needs to be retried. if it does, remove the tx
	// hash from the db an error the transfer so the timeout is retried on the next
	// run
	retryTimeoutPacketProcessor := NewRetryTimeoutPacketProcessor(bridgeClientManager, storage, sourceChainID)
	output = pipeline.ProcessConcurrently(ctx, concurrency, NewIBCV2ProcessorMW(storage, retryTimeoutPacketProcessor), output)

	// record the timeout tx gas cost at current gas prices, if the pipeline
	// has all of the necessary info to do so
	if pipelineOpts.SourceChainGasTokenCoingeckoID != "" && pipelineOpts.SourceChainGasTokenDecimals != 0 && priceClient != nil {
		timeoutTxGasCalculator := NewTimeoutTxGasCalculatorProcessor(bridgeClientManager, storage, priceClient, pipelineOpts.SourceChainGasTokenCoingeckoID, pipelineOpts.SourceChainGasTokenDecimals)
		output = pipeline.ProcessConcurrently(ctx, concurrency, NewIBCV2ProcessorMW(storage, timeoutTxGasCalculator), output)
	}

	// if the recv packet has not been delivered, place the transfer into a
	// batch and wait until either batchTimeout or until batchSize transfers
	// accumulate, then deliver a single recv tx for all transfers in the batch
	// on the destination chain
	batchRecvPacketProcessor := NewBatchRecvPacketProcessor(bridgeClientManager, storage, relayService, sourceChainID, sourceClientID, destinationChainID, destinationClientID)
	output = ConditionallyBatchProcess(metrics.ContextWithRelayType(ctx, metrics.IBCV2SendToRecvRelayType), pipelineOpts.RecvBatchConcurrency, pipelineOpts.RecvBatchSize, pipelineOpts.RecvBatchTimeout, output, NewIBCV2BatchProcessorMW(storage, batchRecvPacketProcessor))

	// check if the recv packet needs to be retried. if it does, remove the tx
	// hash from the db an error the transfer so the recv is retried on the next
	// run
	retryRecvPacketProcessor := NewRetryRecvPacketProcessor(bridgeClientManager, storage, destinationChainID)
	output = pipeline.ProcessConcurrently(ctx, concurrency, NewIBCV2ProcessorMW(storage, retryRecvPacketProcessor), output)

	// record the recv tx gas cost at current gas prices, if the pipeline has
	// all of the necessary info to do so
	if pipelineOpts.DestinationChainGasTokenSymbol != "" || (pipelineOpts.DestinationChainGasTokenCoingeckoID != "" && pipelineOpts.DestinationChainGasTokenDecimals != 0 && priceClient != nil) {
		recvTxGasCalculator := NewRecvTxGasCalculatorProcessor(bridgeClientManager, storage, priceClient, pipelineOpts.DestinationChainGasTokenCoingeckoID, pipelineOpts.DestinationChainGasTokenDecimals, pipelineOpts.DestinationChainGasTokenSymbol)
		output = pipeline.ProcessConcurrently(ctx, concurrency, NewIBCV2ProcessorMW(storage, recvTxGasCalculator), output)
	}

	// wait on the destination chain to produce a write ack packet
	waitForWriteAckPacketProcessor := NewWaitFoWriteAckPacketProcessor(bridgeClientManager, storage)
	output = pipeline.ProcessConcurrently(ctx, concurrency, NewIBCV2ProcessorMW(storage, waitForWriteAckPacketProcessor), output)

	// check if the write ack is finalized on the destination chain
	checkWriteAckFinalityProcessor := NewCheckWriteAckFinalityProcessor(bridgeClientManager, destinationChainID, pipelineOpts.ShouldRelaySuccessAcks, pipelineOpts.ShouldRelayErrorAcks, storage, pipelineOpts.DestinationFinalityOffset)
	output = pipeline.ProcessConcurrently(ctx, concurrency, NewIBCV2ProcessorMW(storage, checkWriteAckFinalityProcessor), output)

	// check if the packet commitment has been found on the source chain, if it
	// is, find and populate the either the ack tx hash or timeout tx hash
	checkPostReceivePacketCommitmentProcessor := NewCheckPacketCommitmentProcessor(bridgeClientManager, storage)
	output = pipeline.ProcessConcurrently(ctx, concurrency, NewIBCV2ProcessorMW(storage, checkPostReceivePacketCommitmentProcessor), output)

	// if the ack packet has not been delivered, place the transfer into a
	// batch and wait until either batchTimeout or until batchSize transfers
	// accumulate, then deliver a single ack tx for all transfers in the batch
	// on the source chain
	batchAckPacketProcessor := NewBatchAckPacketProcessor(bridgeClientManager, storage, relayService, sourceChainID, sourceClientID, destinationChainID, destinationClientID, pipelineOpts.ShouldRelaySuccessAcks, pipelineOpts.ShouldRelayErrorAcks)
	output = ConditionallyBatchProcess(metrics.ContextWithRelayType(ctx, metrics.IBCV2RecvToAckRelayType), pipelineOpts.AckBatchConcurrency, pipelineOpts.AckBatchSize, pipelineOpts.AckBatchTimeout, output, NewIBCV2BatchProcessorMW(storage, batchAckPacketProcessor))

	// check if the ack packet needs to be retried. if it does, remove the tx
	// hash from the db an error the transfer so the ack is retried on the next
	// run
	retryAckPacketProcessor := NewRetryAckPacketProcessor(bridgeClientManager, storage, sourceChainID)
	output = pipeline.ProcessConcurrently(ctx, concurrency, NewIBCV2ProcessorMW(storage, retryAckPacketProcessor), output)

	// record the ack tx gas cost at current gas prices, if the pipeline has
	// all of the necessary info to do so
	if pipelineOpts.SourceChainGasTokenCoingeckoID != "" && pipelineOpts.SourceChainGasTokenDecimals != 0 && priceClient != nil {
		ackTxGasCalculator := NewAckTxGasCalculatorProcessor(bridgeClientManager, storage, priceClient, pipelineOpts.SourceChainGasTokenCoingeckoID, pipelineOpts.SourceChainGasTokenDecimals)
		output = pipeline.ProcessConcurrently(ctx, concurrency, NewIBCV2ProcessorMW(storage, ackTxGasCalculator), output)
	}

	output = pipeline.ProcessConcurrently(ctx, concurrency, NewStateFinisherProcessor(storage, pipelineOpts.ShouldRelaySuccessAcks, pipelineOpts.ShouldRelayErrorAcks), output)

	return &Pipeline[*IBCV2Transfer, *IBCV2Transfer]{Input: input, Output: output}
}

func (p Pipeline[Input, Output]) Push(_ context.Context, i Input) bool {
	p.Input <- i
	return true
}

func (p Pipeline[Input, Output]) Poll() (Output, error) {
	t, ok := <-p.Output
	if !ok {
		return t, errors.New("closed")
	}
	return t, nil
}

func (p Pipeline[Input, Output]) Close() {
	close(p.Input)
}

// IBCV2PipelineManager creates and gives out pipelines for transfers. Each pipeline
// instance is unique to a (source chain, source client, dest chain, dest
// client) combination.
//
//nolint:revive // IBCV2PipelineManager is the canonical name across the codebase
type IBCV2PipelineManager struct {
	storage             Storage
	bridgeClientManager BridgeClientManager
	relayService        proofapi.ProofApiServiceClient
	priceClient         PriceClient
	pipelines           map[pipelineKey]IBCV2Pipeline
}

func NewIBCV2PipelineManager(
	storage Storage,
	bridgeClientManager BridgeClientManager,
	relayService proofapi.ProofApiServiceClient,
	priceClient PriceClient,
) *IBCV2PipelineManager {
	return &IBCV2PipelineManager{
		storage:             storage,
		bridgeClientManager: bridgeClientManager,
		relayService:        relayService,
		priceClient:         priceClient,
		pipelines:           make(map[pipelineKey]IBCV2Pipeline),
	}
}

func (m *IBCV2PipelineManager) Pipeline(ctx context.Context, transfer *IBCV2Transfer) (IBCV2Pipeline, error) {
	key := newPipelineKey(transfer)
	if existing, ok := m.pipelines[key]; ok {
		return existing, nil
	}

	created, err := m.newPipelineForTransfer(ctx, transfer)
	if err != nil {
		return nil, fmt.Errorf("creating new pipeline for transfer from %s to %s: %w", transfer.GetSourceChainID(), transfer.GetDestinationChainID(), err)
	}

	m.pipelines[key] = created
	return created, nil
}

func (m *IBCV2PipelineManager) newPipelineForTransfer(ctx context.Context, transfer *IBCV2Transfer) (IBCV2Pipeline, error) {
	key := newPipelineKey(transfer)

	opts, err := NewPipelineOpts(ctx, transfer)
	if err != nil {
		return nil, fmt.Errorf("creating pipeline options: %w", err)
	}

	ctx = metrics.ContextWithSourceChainID(ctx, transfer.GetSourceChainID())
	ctx = metrics.ContextWithSourceClientID(ctx, transfer.GetPacketSourceClientID())
	ctx = metrics.ContextWithDestChainID(ctx, transfer.GetDestinationChainID())
	ctx = metrics.ContextWithDestClientID(ctx, transfer.GetPacketDestinationClientID())
	ctx = metrics.ContextWithBridgeType(ctx, metrics.IBCV2BridgeType)

	return NewPipelineDeduper(NewPipeline(
		ctx,
		m.storage,
		m.bridgeClientManager,
		m.relayService,
		m.priceClient,
		key.SourceChainID,
		key.SourceClientID,
		key.DestinationChainID,
		key.DestinationClientID,
		opts,
	)), nil
}

func (m *IBCV2PipelineManager) Close() {
	for _, p := range m.pipelines {
		p.Close()
	}
}

type pipelineKey struct {
	SourceChainID       string
	SourceClientID      string
	DestinationChainID  string
	DestinationClientID string
}

func newPipelineKey(transfer *IBCV2Transfer) pipelineKey {
	return pipelineKey{
		SourceChainID:       transfer.GetSourceChainID(),
		SourceClientID:      transfer.GetPacketSourceClientID(),
		DestinationChainID:  transfer.GetDestinationChainID(),
		DestinationClientID: transfer.GetPacketDestinationClientID(),
	}
}

var _ IBCV2Pipeline = (*PipelineDeduper)(nil)

// PipelineDeduper is a wrapper around a pipeline that should be used to input
// values into the pipeline. If a value is pushed onto a pipeline that has
// already been input into the pipeline and it has not been output by the
// pipeline yet, then that input will be ignored.
type PipelineDeduper struct {
	pipeline       IBCV2Pipeline
	inPipeline     map[transferKey]struct{}
	inPipelineLock *sync.RWMutex
	once           *sync.Once
	done           chan struct{}
}

func NewPipelineDeduper(p IBCV2Pipeline) *PipelineDeduper {
	return &PipelineDeduper{
		pipeline:       p,
		inPipeline:     make(map[transferKey]struct{}),
		inPipelineLock: new(sync.RWMutex),
		once:           new(sync.Once),
		done:           make(chan struct{}),
	}
}

func (d *PipelineDeduper) Push(ctx context.Context, transfer *IBCV2Transfer) bool {
	d.once.Do(func() {
		// if this is the first time we are pushing a transfer onto this
		// pipeline, spawn a goroutine to watch the output channel to delete
		// entries from the inPipeline map
		go func() {
			defer func() {
				d.done <- struct{}{}
			}()

			for {
				output, err := d.pipeline.Poll()
				if err != nil {
					return
				}
				if output == nil {
					return
				}
				d.inPipelineLock.Lock()
				delete(d.inPipeline, newTransferKey(output))
				d.inPipelineLock.Unlock()
			}
		}()
	})

	d.inPipelineLock.Lock()
	defer d.inPipelineLock.Unlock()

	key := newTransferKey(transfer)

	// check if transfer already is in the pipeline
	if _, ok := d.inPipeline[key]; ok {
		return false
	}

	// mark transfer as in pipeline and push
	d.inPipeline[key] = struct{}{}
	return d.pipeline.Push(ctx, transfer)
}

// Poll does nothing for a pipeline deduper, it overrides the output and consumes it infernally
//
//nolint:nilnil // intentional: deduper consumes output internally, Poll is a no-op
func (*PipelineDeduper) Poll() (*IBCV2Transfer, error) {
	return nil, nil
}

func (d *PipelineDeduper) Close() {
	d.pipeline.Close()
	<-d.done
}

type transferKey struct {
	SourceChainID        string
	PacketSequenceNumber uint32
	PacketSourceClientID string
}

func newTransferKey(transfer *IBCV2Transfer) transferKey {
	return transferKey{
		SourceChainID:        transfer.GetSourceChainID(),
		PacketSequenceNumber: transfer.GetPacketSequenceNumber(),
		PacketSourceClientID: transfer.GetPacketSourceClientID(),
	}
}
