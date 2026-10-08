package ibcv2

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rpc"
	"go.uber.org/zap"

	"github.com/cosmos/ibc-relayer/db/gen/db"
	"github.com/cosmos/ibc-relayer/shared/config"
	"github.com/cosmos/ibc-relayer/shared/contracts/erc20"
	"github.com/cosmos/ibc-relayer/shared/contracts/ics20_transfer"
	"github.com/cosmos/ibc-relayer/shared/contracts/ics26_router"
	"github.com/cosmos/ibc-relayer/shared/contracts/ift"
	"github.com/cosmos/ibc-relayer/shared/contracts/sp1_ics07_tendermint"
	"github.com/cosmos/ibc-relayer/shared/lmt"
	"github.com/cosmos/ibc-relayer/shared/metrics"
	"github.com/cosmos/ibc-relayer/shared/signing"
	signingevm "github.com/cosmos/ibc-relayer/shared/signing/evm"
)

const maxPendingAndConfirmedNonceDifference = 5

type EVMClient interface {
	bind.DeployBackend
	bind.ContractBackend

	NonceAt(ctx context.Context, account common.Address, blockNumber *big.Int) (uint64, error)
	BalanceAt(ctx context.Context, account common.Address, blockNumber *big.Int) (*big.Int, error)
	TransactionByHash(ctx context.Context, hash common.Hash) (tx *types.Transaction, isPending bool, err error)
	Close()
}

type EVMBridgeClient struct {
	chainID string
	client  EVMClient

	ics26Router   *ics26_router.Contract
	routerAddress common.Address

	signer             signing.Signer
	signerAddress      common.Address
	txSubmissionLock   *sync.Mutex
	lastSubmissionTime time.Time
	txSubmissionDelay  time.Duration

	txOnce              *OnceWithKey[*types.Receipt]
	latestTimestampOnce *OnceWithKey[time.Time]

	gasFeeCapMultiplier *float64
	gasTipCapMultiplier *float64

	logLookbackBlocks *uint64
}

func (c *EVMBridgeClient) SetLogLookbackBlocks(blocks *uint64) {
	c.logLookbackBlocks = blocks
}

func (c *EVMBridgeClient) logQueryStart(ctx context.Context) (*big.Int, error) {
	if c.logLookbackBlocks == nil {
		return nil, nil
	}

	head, err := c.client.HeaderByNumber(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("reading the latest header to bound the log query: %w", err)
	}

	latest := head.Number.Uint64()
	if latest <= *c.logLookbackBlocks {
		return big.NewInt(0), nil
	}

	return new(big.Int).SetUint64(latest - *c.logLookbackBlocks), nil
}

func NewEVMBridgeClient(
	ctx context.Context,
	chainID string,
	ics26RouterContractAddress string,
	client EVMClient,
	signer signing.Signer,
	gasFeeCapMultiplier *float64,
	gasTipCapMultiplier *float64,
	txSubmissionDelay *time.Duration,
) (*EVMBridgeClient, error) {
	delay := 2 * time.Second
	if txSubmissionDelay != nil {
		delay = *txSubmissionDelay
	}

	routerAddress := common.HexToAddress(ics26RouterContractAddress)
	ics26Router, err := ics26_router.NewContract(routerAddress, client)
	if err != nil {
		return nil, fmt.Errorf("creating contract binding for ics26 router at %s: %w", routerAddress, err)
	}

	signerAddressBytes := signer.Address(ctx)
	if len(signerAddressBytes) != common.AddressLength {
		return nil, fmt.Errorf("invalid signer address length: expected %d bytes, got %d bytes (failed to get address from remote signer)", common.AddressLength, len(signerAddressBytes))
	}

	return &EVMBridgeClient{
		chainID:             chainID,
		client:              client,
		ics26Router:         ics26Router,
		routerAddress:       routerAddress,
		signer:              signer,
		signerAddress:       common.Address(signerAddressBytes),
		txSubmissionLock:    new(sync.Mutex),
		txSubmissionDelay:   delay,
		txOnce:              NewOnceWithKey[*types.Receipt](time.Minute, 5*time.Second),
		latestTimestampOnce: NewOnceWithKey[time.Time](12*time.Second, 200*time.Millisecond),
		gasFeeCapMultiplier: gasFeeCapMultiplier,
		gasTipCapMultiplier: gasTipCapMultiplier,
	}, nil
}

// IsPacketReceived checks to see if a packet receipt commitment exists for a
// packet sequence and client ID.
func (c *EVMBridgeClient) IsPacketReceived(ctx context.Context, packetDestinationClientID string, sequence uint64) (bool, error) {
	packetReceiptCommitmentPathCalldata, err := encodePacked(
		packetDestinationClientID,
		uint8(2),
		sdk.Uint64ToBigEndian(sequence),
	)
	if err != nil {
		return false, fmt.Errorf("encoding packed client id '%s' '2' and sequence '%d': %w", packetDestinationClientID, sequence, err)
	}

	hashedPath := crypto.Keccak256Hash(packetReceiptCommitmentPathCalldata)
	commitment, err := c.ics26Router.GetCommitment(&bind.CallOpts{Context: ctx}, hashedPath)
	if err != nil {
		return false, fmt.Errorf("getting commitment for packet with destination client id %s and sequence %d and hashed path %s: %w", packetReceiptCommitmentPathCalldata, sequence, hashedPath, err)
	}

	unwritten, _ := hex.DecodeString("0000000000000000000000000000000000000000")
	// if the commitment is not unwritten, then it has been received
	return commitment != common.BytesToHash(unwritten), nil
}

// IsPacketCommitted checks to see if a packet commitment exists for a packet
// sequence and client ID.
//
// When a packet is sent, a packet commitment is created on the source chain.
// When it is ack'd or timed out on the source chain, that packet commitment is
// deleted. Thus, if this function returns true, it means that the packet has
// been sent and not timed out or ack'd yet. If it returns false, this means
// that the packet either has been ack'd or timed out, or it does not exist.
func (c *EVMBridgeClient) IsPacketCommitted(ctx context.Context, packetSourceClientID string, sequence uint64) (bool, error) {
	packetCommitmentPathCalldata, err := encodePacked(
		packetSourceClientID,
		uint8(1),
		sdk.Uint64ToBigEndian(sequence),
	)
	if err != nil {
		return false, fmt.Errorf("encoding packed client id '%s' '1' and sequence '%d': %w", packetSourceClientID, sequence, err)
	}

	hashedPath := crypto.Keccak256Hash(packetCommitmentPathCalldata)
	commitment, err := c.ics26Router.GetCommitment(&bind.CallOpts{Context: ctx}, hashedPath)
	if err != nil {
		return false, fmt.Errorf("getting commitment for packet with source client id %s and sequence %d and hashed path %s: %w", packetSourceClientID, sequence, hashedPath, err)
	}

	// when a commitment does not exist, GetCommitment will return an
	// uninitialized storage value, which is 32 bytes of 0's
	unwritten, _ := hex.DecodeString("0000000000000000000000000000000000000000")
	return commitment != common.BytesToHash(unwritten), nil
}

func (c *EVMBridgeClient) FindRecvTx(
	ctx context.Context,
	sourceClientID string,
	destinationClientID string,
	sequence uint64,
	timeoutTimestamp time.Time,
) (*BridgeTx, error) {
	routerABI, err := ics26_router.ContractMetaData.GetAbi()
	if err != nil {
		return nil, fmt.Errorf("getting ics26 router abi: %w", err)
	}

	// we look for the write ack event here instead of the RecvPacket because
	// there is no RecvPacket to save on gas costs so when receiving a packet
	// there is no duplicate event emitted since these would hold the same
	// information. The ibc team has said there is no plans to implement async
	// acks on the evm side, so the WriteAcknowledgement will always be emitted
	// in the receive tx.
	const writeAckEventName = "WriteAcknowledgement"
	query := [][]any{{routerABI.Events[writeAckEventName].ID}, {destinationClientID}, {sequence}}
	topics, err := abi.MakeTopics(query...)
	if err != nil {
		return nil, fmt.Errorf("creating topics for filter logs query: %w", err)
	}

	fromBlock, err := c.logQueryStart(ctx)
	if err != nil {
		return nil, err
	}

	filter := ethereum.FilterQuery{
		FromBlock: fromBlock,
		Addresses: []common.Address{c.routerAddress},
		Topics:    topics,
	}
	logs, err := c.client.FilterLogs(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("filtering for write ack logs: %w", err)
	}
	if len(logs) == 0 {
		return nil, ErrTxNotFound
	}
	if len(logs) != 1 {
		return nil, fmt.Errorf("expected to receive 1 log for write ack with sequence %d and dest client ID %s, instead got %d", sequence, destinationClientID, len(logs))
	}

	writeAck, err := c.ics26Router.ParseWriteAcknowledgement(logs[0])
	if err != nil {
		return nil, fmt.Errorf("parsing write ack: %w", err)
	}

	// get the block header to get the timestamp of the tx
	blockNumber := writeAck.Raw.BlockNumber
	header, err := c.client.HeaderByNumber(ctx, big.NewInt(int64(blockNumber))) //nolint:gosec // G115 bounded conversion
	if err != nil {
		return nil, fmt.Errorf("getting block header at height %d: %w", blockNumber, err)
	}

	signer, err := c.txSigner(ctx, writeAck.Raw.TxHash)
	if err != nil {
		// if fetching the signer fails, we would rather miss this info than
		// potentially hold up a client finding a recv tx because of this, so
		// instead of returning an error, we will simply log and continue
		// without this info
		lmt.Logger(ctx).Error("found recv tx but could not determine the tx's signer, ignoring error and continuing", zap.Error(err), zap.String("tx_hash", writeAck.Raw.TxHash.String()))
	}

	return &BridgeTx{
		Hash:           writeAck.Raw.TxHash.String(),
		Timestamp:      time.Unix(int64(header.Time), 0), //nolint:gosec // G115 bounded conversion
		RelayerAddress: signer,
	}, nil
}

func (c *EVMBridgeClient) FindAckTx(
	ctx context.Context,
	sourceClientID string,
	_ string,
	sequence uint64,
) (*BridgeTx, error) {
	routerABI, err := ics26_router.ContractMetaData.GetAbi()
	if err != nil {
		return nil, fmt.Errorf("getting ics26 router abi: %w", err)
	}

	const ackEventName = "AckPacket"
	query := [][]any{{routerABI.Events[ackEventName].ID}, {sourceClientID}, {sequence}}
	topics, err := abi.MakeTopics(query...)
	if err != nil {
		return nil, fmt.Errorf("creating topics for filter logs query: %w", err)
	}

	fromBlock, err := c.logQueryStart(ctx)
	if err != nil {
		return nil, err
	}

	filter := ethereum.FilterQuery{
		FromBlock: fromBlock,
		Addresses: []common.Address{c.routerAddress},
		Topics:    topics,
	}
	logs, err := c.client.FilterLogs(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("filtering ack logs: %w", err)
	}
	if len(logs) == 0 {
		return nil, ErrTxNotFound
	}
	if len(logs) != 1 {
		return nil, fmt.Errorf("expected to receive 1 log for ack packet with sequence %d and source client ID %s, instead got %d", sequence, sourceClientID, len(logs))
	}

	ackPacket, err := c.ics26Router.ParseAckPacket(logs[0])
	if err != nil {
		return nil, fmt.Errorf("parsing ack packet log: %w", err)
	}

	// get the block header to get the timestamp of the tx
	blockNumber := ackPacket.Raw.BlockNumber
	header, err := c.client.HeaderByNumber(ctx, big.NewInt(int64(blockNumber))) //nolint:gosec // G115 bounded conversion
	if err != nil {
		return nil, fmt.Errorf("getting block header at height %d: %w", blockNumber, err)
	}

	signer, err := c.txSigner(ctx, ackPacket.Raw.TxHash)
	if err != nil {
		// if fetching the signer fails, we would rather miss this info than
		// potentially hold up a client finding a recv tx because of this, so
		// instead of returning an error, we will simply log and continue
		// without this info
		lmt.Logger(ctx).Error("found ack tx but could not determine the tx's signer, ignoring error and continuing", zap.Error(err), zap.String("tx_hash", ackPacket.Raw.TxHash.String()))
	}

	return &BridgeTx{
		Hash:           ackPacket.Raw.TxHash.String(),
		Timestamp:      time.Unix(int64(header.Time), 0), //nolint:gosec // G115 bounded conversion
		RelayerAddress: signer,
	}, nil
}

func (c *EVMBridgeClient) FindTimeoutTx(
	ctx context.Context,
	sourceClientID string,
	_ string,
	sequence uint64,
) (*BridgeTx, error) {
	routerABI, err := ics26_router.ContractMetaData.GetAbi()
	if err != nil {
		return nil, fmt.Errorf("getting ics26 router abi: %w", err)
	}

	const timeoutEventName = "TimeoutPacket"
	query := [][]any{{routerABI.Events[timeoutEventName].ID}, {sourceClientID}, {sequence}}
	topics, err := abi.MakeTopics(query...)
	if err != nil {
		return nil, fmt.Errorf("creating topics for filter logs query: %w", err)
	}

	fromBlock, err := c.logQueryStart(ctx)
	if err != nil {
		return nil, err
	}

	filter := ethereum.FilterQuery{
		FromBlock: fromBlock,
		Addresses: []common.Address{c.routerAddress},
		Topics:    topics,
	}
	logs, err := c.client.FilterLogs(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("filtering for timeout logs: %w", err)
	}
	if len(logs) == 0 {
		return nil, ErrTxNotFound
	}
	if len(logs) != 1 {
		return nil, fmt.Errorf("expected to receive 1 log for timeout with sequence %d and source client ID %s, instead got %d", sequence, sourceClientID, len(logs))
	}

	timeoutPacket, err := c.ics26Router.ParseTimeoutPacket(logs[0])
	if err != nil {
		return nil, fmt.Errorf("parsing timeout packet: %w", err)
	}

	// get the block header to get the timestamp of the tx
	blockNumber := timeoutPacket.Raw.BlockNumber
	header, err := c.client.HeaderByNumber(ctx, big.NewInt(int64(blockNumber))) //nolint:gosec // G115 bounded conversion
	if err != nil {
		return nil, fmt.Errorf("getting block header at height %d: %w", blockNumber, err)
	}

	signer, err := c.txSigner(ctx, timeoutPacket.Raw.TxHash)
	if err != nil {
		// if fetching the signer fails, we would rather miss this info than
		// potentially hold up a client finding a recv tx because of this, so
		// instead of returning an error, we will simply log and continue
		// without this info
		lmt.Logger(ctx).Error("found timeout tx but could not determine the tx's signer, ignoring error and continuing", zap.Error(err), zap.String("tx_hash", timeoutPacket.Raw.TxHash.String()))
	}

	return &BridgeTx{
		Hash:           timeoutPacket.Raw.TxHash.String(),
		Timestamp:      time.Unix(int64(header.Time), 0), //nolint:gosec // G115 bounded conversion
		RelayerAddress: signer,
	}, nil
}

func (c *EVMBridgeClient) DeliverTx(ctx context.Context, bz []byte, address string) (*BridgeTx, error) {
	var err error
	c.txSubmissionLock.Lock()
	defer func() {
		if err == nil {
			c.lastSubmissionTime = time.Now()
		}
		c.txSubmissionLock.Unlock()
	}()
	select {
	case <-time.After(time.Until(c.lastSubmissionTime.Add(c.txSubmissionDelay))):
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	hash, err := c.signAndSubmitData(ctx, bz, address)
	if err != nil {
		return nil, fmt.Errorf("signing and submitting data to %s: %w", address, err)
	}

	return &BridgeTx{
		Hash:           hash.String(),
		Timestamp:      time.Now().UTC(),
		RelayerAddress: c.signerAddress.String(),
	}, nil
}

func (c *EVMBridgeClient) PacketWriteAckStatus(
	ctx context.Context,
	hash string,
	sequence uint64,
	sourceClientID string,
	destClientID string,
) (db.Ibcv2WriteAckStatus, error) {
	routerABI, err := ics26_router.ContractMetaData.GetAbi()
	if err != nil {
		return db.Ibcv2WriteAckStatusUNKNOWN, fmt.Errorf("getting ics26 router abi: %w", err)
	}

	hashBytes := common.HexToHash(hash)

	// only fetch the tx receipt one per tx hash even if called many times
	// concurrently. if the call returns an error, cache it for 5s and then
	// let another caller try again.
	receipt, err := c.txOnce.Do(hash, func() (*types.Receipt, error) {
		return c.client.TransactionReceipt(ctx, hashBytes)
	})
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return db.Ibcv2WriteAckStatusUNKNOWN, ErrTxNotFound
		}
		return db.Ibcv2WriteAckStatusUNKNOWN, fmt.Errorf("getting transaction receipt: %w", err)
	}

	const writeAckEventName = "WriteAcknowledgement"
	for _, log := range receipt.Logs {
		if log == nil {
			continue
		}
		if len(log.Topics) == 0 {
			continue
		}
		if routerABI.Events[writeAckEventName].ID != log.Topics[0] {
			continue
		}

		writeAck, err := c.ics26Router.ParseWriteAcknowledgement(*log)
		if err != nil {
			return db.Ibcv2WriteAckStatusUNKNOWN, fmt.Errorf("parsing write packet event from log %v: %w", *log, err)
		}

		if writeAck.Packet.Sequence == sequence && writeAck.Packet.SourceClient == sourceClientID && writeAck.Packet.DestClient == destClientID {
			if len(writeAck.Acknowledgements) == 1 {
				if bytes.Equal(writeAck.Acknowledgements[0], ErrorAcknowledgement[:]) {
					return db.Ibcv2WriteAckStatusERROR, nil
				}
			}
			return db.Ibcv2WriteAckStatusSUCCESS, nil
		}
	}

	return db.Ibcv2WriteAckStatusUNKNOWN, ErrWriteAckNotFoundForPacket
}

func (c *EVMBridgeClient) SendPacketsFromTx(ctx context.Context, sourceChainID string, txHash string) ([]*PacketInfo, error) {
	ics26RouterABI, err := ics26_router.ContractMetaData.GetAbi()
	if err != nil {
		return nil, fmt.Errorf("getting contract ics26RouterABI for ics_26 router: %w", err)
	}

	tx := common.HexToHash(txHash)
	receipt, err := c.client.TransactionReceipt(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("getting transaction receipt for tx %s on chain %s: %w", txHash, c.chainID, err)
	}

	packets := make([]*PacketInfo, 0, len(receipt.Logs))
	const sendPacketEvent = "SendPacket"
	for _, log := range receipt.Logs {
		if log == nil {
			continue
		}
		if len(log.Topics) == 0 {
			continue
		}
		if ics26RouterABI.Events[sendPacketEvent].ID != log.Topics[0] {
			continue
		}
		sendPacket, err := c.ics26Router.ParseSendPacket(*log)
		if err != nil {
			return nil, fmt.Errorf("parsing send packet event from tx %s logs on chain %s: %w", txHash, c.chainID, err)
		}
		packets = append(packets, &PacketInfo{
			Sequence:          sendPacket.Packet.Sequence,
			SourceClient:      sendPacket.Packet.SourceClient,
			DestinationClient: sendPacket.Packet.DestClient,
			TimeoutTimestamp:  time.Unix(int64(sendPacket.Packet.TimeoutTimestamp), 0), //nolint:gosec // G115 bounded conversion
		})
	}
	if len(packets) == 0 {
		return nil, nil
	}

	// query for the block header only after we have found send packets in this
	// tx this just saves us a call if the tx does not have any send packets in
	// it
	header, err := c.client.HeaderByNumber(ctx, receipt.BlockNumber)
	if err != nil {
		return nil, fmt.Errorf("getting block header for height %s of tx %s o chain %s: %w", receipt.BlockNumber.String(), txHash, c.chainID, err)
	}

	for _, packet := range packets {
		packet.Timestamp = time.Unix(int64(header.Time), 0) //nolint:gosec // G115 bounded conversion
	}

	return packets, nil
}

func (c *EVMBridgeClient) ShouldRetryTx(ctx context.Context, txHash string, expiry time.Duration, sentTs time.Time) (bool, error) {
	receipt, err := c.txOnce.Do(txHash, func() (*types.Receipt, error) { return c.client.TransactionReceipt(ctx, common.HexToHash(txHash)) })
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			// tx not found on chain, check if we have waited longer than the
			// expiry of on chain time, and if we have, the tx should be
			// retried

			header, err := c.client.HeaderByNumber(ctx, nil)
			if err != nil {
				return false, fmt.Errorf("getting latest block header: %w", err)
			}
			headerTs := time.Unix(int64(header.Time), 0).UTC() //nolint:gosec // G115 bounded conversion

			expiresAt := sentTs.UTC().Add(expiry)
			isExpired := expiresAt.Before(headerTs)
			if isExpired {
				// we have waited longer than the expiry and the tx is still
				// not found, retry it
				return true, nil
			}

			// tx not found but we have not timed out waiting for the tx yet,
			// return that the tx should not be retried yet, but return a
			// sentinel error to inform the user of the situation
			return false, ErrTxNotFound
		}
		// error fetching tx on chain, node may be down or some other issue so
		// do not retry yet
		return false, fmt.Errorf("could not get transaction receipt for tx with hash %s: %w", txHash, err)
	}
	if receipt.Status != 1 {
		// tx was unsuccessful, needs to be retried
		return true, nil
	}

	// tx exists and was successful, no need to retry
	return false, nil
}

func (c *EVMBridgeClient) WaitForChain(ctx context.Context) error {
	const initialTick = time.Millisecond
	const tick = time.Second
	ticker := time.NewTicker(initialTick)

	now := time.Now()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			latest, err := c.client.HeaderByNumber(ctx, nil)
			if err != nil {
				return fmt.Errorf("getting latest block header: %w", err)
			}

			latestTime := time.Unix(int64(latest.Time), 0) //nolint:gosec // G115 bounded conversion
			if latestTime.After(now) {
				return nil
			}

			if time.Since(now) > time.Second*30 {
				// if the chain hasnt reached the current height in 30s, something
				// may be wrong and we should start logging warnings
				lmt.Logger(ctx).Warn(
					"latest header time is behind current time, waiting... (already been waiting for >30s)",
					zap.Time("chain_time", latestTime),
					zap.Time("wait_until", now),
				)
			} else {
				lmt.Logger(ctx).Debug(
					"latest header time is behind current time, waiting...",
					zap.Time("chain_time", latestTime),
					zap.Time("wait_until", now),
				)
			}

			ticker.Reset(tick)
		}
	}
}

func (c *EVMBridgeClient) IsTxFinalized(ctx context.Context, txHash string, offset *uint64) (bool, error) {
	hash := common.HexToHash(txHash)
	receipt, err := c.client.TransactionReceipt(ctx, hash)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return false, ErrTxNotFound
		}
		return false, fmt.Errorf("getting receipt for tx %s: %w", txHash, err)
	}

	// If offset is nil, use native finality mechanism
	if offset == nil {
		finalizedHeader, err := c.client.HeaderByNumber(ctx, big.NewInt(rpc.FinalizedBlockNumber.Int64()))
		if err != nil {
			return false, fmt.Errorf("getting finalized block height: %w", err)
		}
		return big.NewInt(receipt.BlockNumber.Int64()).Cmp(finalizedHeader.Number) <= 0, nil
	}

	// If offset is specified, use block offset mechanism
	latestHeader, err := c.client.HeaderByNumber(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("getting latest block height: %w", err)
	}

	txBlockNumber := receipt.BlockNumber.Uint64()
	latestBlockNumber := latestHeader.Number.Uint64()

	return latestBlockNumber >= txBlockNumber && (latestBlockNumber-txBlockNumber) >= *offset, nil
}

func (c *EVMBridgeClient) IsTimestampFinalized(ctx context.Context, timestamp time.Time, offset *uint64) (bool, error) {
	var header *types.Header
	var err error

	if offset == nil {
		// Use native finality mechanism
		header, err = c.client.HeaderByNumber(ctx, big.NewInt(rpc.FinalizedBlockNumber.Int64()))
		if err != nil {
			return false, fmt.Errorf("getting finalized block header: %w", err)
		}
	} else {
		// Use offset-based finality (same as attestors)
		latestHeader, err := c.client.HeaderByNumber(ctx, nil)
		if err != nil {
			return false, fmt.Errorf("getting latest block height: %w", err)
		}
		if latestHeader.Number.Uint64() < *offset {
			return false, nil // Not enough blocks yet
		}
		finalizedHeight := latestHeader.Number.Uint64() - *offset
		header, err = c.client.HeaderByNumber(ctx, big.NewInt(int64(finalizedHeight))) //nolint:gosec // G115 bounded conversion
		if err != nil {
			return false, fmt.Errorf("getting block header at height %d: %w", finalizedHeight, err)
		}
	}

	// Check if finalized block's timestamp >= target timestamp
	blockTime := time.Unix(int64(header.Time), 0) //nolint:gosec // G115 bounded conversion
	return !blockTime.Before(timestamp), nil
}

func (*EVMBridgeClient) ChainType() config.ChainType {
	return config.ChainTypeEVM
}

func encodePacked(values ...any) ([]byte, error) {
	var bz bytes.Buffer

	for _, value := range values {
		switch v := value.(type) {
		case []byte:
			_, _ = bz.Write(v)
		case byte:
			_, _ = bz.Write([]byte{v})
		case string:
			_, _ = bz.WriteString(v)
		default:
			return nil, fmt.Errorf("unsupported value type %T", v)
		}
	}

	return bz.Bytes(), nil
}

func (c *EVMBridgeClient) signAndSubmitData(ctx context.Context, bz []byte, address string) (common.Hash, error) {
	tx, err := c.newTx(ctx, bz, address)
	if err != nil {
		return common.Hash{}, fmt.Errorf("creating tx to sign and submit: %w", err)
	}

	signerFn := signingevm.EthereumSignerToBindSignerFn(ctx, c.signer, c.chainID)
	signedTx, err := signerFn(c.signerAddress, tx)
	if err != nil {
		return common.Hash{}, fmt.Errorf("signing tx with address %s: %w", c.signerAddress.String(), err)
	}

	if err = c.client.SendTransaction(ctx, signedTx); err != nil {
		return common.Hash{}, fmt.Errorf("sending signed transaction: %w", err)
	}

	return signedTx.Hash(), nil
}

// selectNonceWithFallback returns confirmedNonce when pendingNonce is far enough
// ahead to indicate a stuck-tx situation in the mempool; otherwise it returns
// pendingNonce. When the fallback triggers, it logs a warning and increments
// the nonce-replaced metric.
func selectNonceWithFallback(ctx context.Context, chainID string, pendingNonce, confirmedNonce uint64) uint64 {
	if pendingNonce > confirmedNonce && pendingNonce-confirmedNonce > maxPendingAndConfirmedNonceDifference {
		lmt.Logger(ctx).Warn(
			"pending nonce too far ahead of confirmed nonce, falling back to confirmed nonce",
			zap.String("chain_id", chainID),
			zap.Uint64("pending_nonce", pendingNonce),
			zap.Uint64("confirmed_nonce", confirmedNonce),
		)
		metrics.FromContext(ctx).AddTransactionNonceReplaced(chainID)
		return confirmedNonce
	}
	return pendingNonce
}

func (c *EVMBridgeClient) newTx(ctx context.Context, bz []byte, address string) (*types.Transaction, error) {
	to := common.HexToAddress(address)

	head, err := c.client.HeaderByNumber(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("getting latest block header: %w", err)
	}

	gasTipCap, err := c.client.SuggestGasTipCap(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting suggested gas price for chain %s: %w", c.chainID, err)
	}
	gasFeeCap := new(big.Int).Add(gasTipCap, new(big.Int).Mul(head.BaseFee, big.NewInt(2)))

	code, err := c.client.PendingCodeAt(ctx, to)
	if err != nil {
		return nil, fmt.Errorf("getting pending code at for address %s: %w", to.String(), err)
	}
	if len(code) == 0 {
		return nil, fmt.Errorf("code no length response when getting pending code for address %s: %w", to.String(), err)
	}

	msg := ethereum.CallMsg{From: c.signerAddress, To: &to, Data: bz}
	gasLimit, err := c.client.EstimateGas(ctx, msg)
	if err != nil {
		return nil, fmt.Errorf("estimating gas to deliver msg to %s: %w", to.String(), err)
	}

	pendingNonce, err := c.client.PendingNonceAt(ctx, c.signerAddress)
	if err != nil {
		return nil, fmt.Errorf("getting pending nonce for address %s on chain %s: %w", c.signerAddress.String(), c.chainID, err)
	}
	confirmedNonce, err := c.client.NonceAt(ctx, c.signerAddress, nil)
	if err != nil {
		return nil, fmt.Errorf("getting confirmed nonce for address %s on chain %s: %w", c.signerAddress.String(), c.chainID, err)
	}
	nonce := selectNonceWithFallback(ctx, c.chainID, pendingNonce, confirmedNonce)

	inner := &types.DynamicFeeTx{
		To:        &to,
		Nonce:     nonce,
		GasFeeCap: c.adjustGasFeeCap(gasFeeCap),
		GasTipCap: c.adjustGasTipCap(gasTipCap),
		Gas:       gasLimit,
		Data:      bz,
	}
	return types.NewTx(inner), nil
}

func (c *EVMBridgeClient) adjustGasFeeCap(gasFeeCap *big.Int) *big.Int {
	if gasFeeCap == nil {
		return nil
	}
	if c.gasFeeCapMultiplier == nil {
		return gasFeeCap
	}

	feeCapFloat := new(big.Float).SetInt(gasFeeCap)
	adjustedFeeCap, _ := new(big.Float).Mul(feeCapFloat, big.NewFloat(*c.gasFeeCapMultiplier)).Int(nil)

	return adjustedFeeCap
}

func (c *EVMBridgeClient) adjustGasTipCap(gasTipCap *big.Int) *big.Int {
	if gasTipCap == nil {
		return nil
	}
	if c.gasTipCapMultiplier == nil {
		return gasTipCap
	}
	tipCapFloat := new(big.Float).SetInt(gasTipCap)
	adjustedTipCap, _ := new(big.Float).Mul(tipCapFloat, big.NewFloat(*c.gasTipCapMultiplier)).Int(nil)

	return adjustedTipCap
}

func (c *EVMBridgeClient) LatestOnChainTimestamp(ctx context.Context) (time.Time, error) {
	return c.latestTimestampOnce.Do("key", func() (time.Time, error) {
		header, err := c.client.HeaderByNumber(ctx, nil)
		if err != nil {
			return time.Time{}, fmt.Errorf("getting latest header on chain: %w", err)
		}
		return time.Unix(int64(header.Time), 0), nil //nolint:gosec // G115 bounded conversion
	})
}

func (c *EVMBridgeClient) SignerGasTokenBalance(ctx context.Context) (*big.Int, error) {
	return c.client.BalanceAt(ctx, c.signerAddress, nil)
}

func (c *EVMBridgeClient) TxFee(ctx context.Context, txHash string) (*big.Int, error) {
	receipt, err := c.txOnce.Do(txHash, func() (*types.Receipt, error) {
		return c.client.TransactionReceipt(ctx, common.HexToHash(txHash))
	})
	if err != nil {
		return nil, fmt.Errorf("getting transaction receipt for tx hash %s: %w", txHash, err)
	}
	return new(big.Int).Mul(receipt.EffectiveGasPrice, new(big.Int).SetUint64(receipt.GasUsed)), nil
}

func (c *EVMBridgeClient) TxExecutionStatus(ctx context.Context, txHash string) (TxExecutionStatus, error) {
	receipt, err := c.txOnce.Do(txHash, func() (*types.Receipt, error) {
		return c.client.TransactionReceipt(ctx, common.HexToHash(txHash))
	})
	if err != nil {
		if errors.Is(err, ethereum.NotFound) {
			return TxExecutionStatus{}, fmt.Errorf("%w: %w", ErrTxNotFound, err)
		}
		return TxExecutionStatus{}, fmt.Errorf("getting transaction receipt for tx hash %s: %w", txHash, err)
	}

	if receipt.Status == 1 {
		return TxExecutionStatus{Status: TxExecutionStatusSuccess}, nil
	}

	return TxExecutionStatus{
		Status:       TxExecutionStatusFailed,
		ErrorMessage: "evm transaction execution failed",
	}, nil
}

func (c *EVMBridgeClient) txSigner(ctx context.Context, hash common.Hash) (string, error) {
	chainID, ok := new(big.Int).SetString(c.chainID, 10)
	if !ok {
		return "", fmt.Errorf("could not convert chain id %s to *big.Int", c.chainID)
	}

	tx, _, err := c.client.TransactionByHash(ctx, hash)
	if err != nil {
		return "", fmt.Errorf("getting transaction for hash %s: %w", hash.String(), err)
	}

	sender, err := types.Sender(types.NewPragueSigner(chainID), tx)
	if err != nil {
		return "", fmt.Errorf("getting transaction %s sender: %w", hash.String(), err)
	}

	return sender.String(), nil
}

func (c *EVMBridgeClient) SendTransfer(
	ctx context.Context,
	ics20Address string,
	clientID string,
	denom string,
	receiver string,
	amount *big.Int,
	memo string,
	timeout time.Duration,
) (string, error) {
	signerFn := signingevm.EthereumSignerToBindSignerFn(ctx, c.signer, c.chainID)
	opts := &bind.TransactOpts{
		From:    c.signerAddress,
		Signer:  signerFn,
		Context: ctx,
	}

	transferAddress := common.HexToAddress(ics20Address)
	ics20Contract, err := ics20_transfer.NewContract(transferAddress, c.client)
	if err != nil {
		return "", fmt.Errorf("creating ics20 transfer contract binding at %s: %w", transferAddress, err)
	}

	erc20Contract, err := erc20.NewContract(common.HexToAddress(denom), c.client)
	if err != nil {
		return "", fmt.Errorf("creating erc20 contract for denom %s: %w", denom, err)
	}

	currentApproval, err := erc20Contract.Allowance(nil, c.signerAddress, transferAddress)
	if err != nil {
		return "", fmt.Errorf("getting current allowance for ics20 transfer %s to spend %s from wallet %s: %w", transferAddress.String(), denom, c.signerAddress.String(), err)
	}

	if currentApproval.Cmp(amount) < 0 {
		tx, err := erc20Contract.Approve(opts, transferAddress, amount)
		if err != nil {
			return "", fmt.Errorf("approving %s to be spent from wallet %s by ics20 transfer %s: %w", amount.String(), c.signerAddress.String(), transferAddress.String(), err)
		}
		if err = c.WaitForTx(ctx, tx.Hash().String()); err != nil {
			return "", fmt.Errorf("waiting for receipt of erc20 approval tx %s: %w", tx.Hash().String(), err)
		}
	}

	transferPort := "transfer"
	msg := ics20_transfer.IICS20TransferMsgsSendTransferMsg{
		Denom:            common.HexToAddress(denom),
		Amount:           amount,
		Receiver:         receiver,
		SourceClient:     clientID,
		DestPort:         transferPort,
		TimeoutTimestamp: uint64(time.Now().Add(timeout).UTC().Unix()), //nolint:gosec // G115 bounded conversion
		Memo:             memo,
	}
	tx, err := ics20Contract.SendTransfer(opts, msg)
	if err != nil {
		return "", fmt.Errorf("sending transfer from %s of %s %s to %s from client %s: %w", c.signerAddress, amount, denom, receiver, clientID, err)
	}

	if err = c.WaitForTx(ctx, tx.Hash().String()); err != nil {
		return "", fmt.Errorf("waiting for receipt of send transfer tx %s: %w", tx.Hash().String(), err)
	}
	return tx.Hash().String(), nil
}

func (c *EVMBridgeClient) IFTTransfer(
	ctx context.Context,
	iftAddress string,
	clientID string,
	receiver string,
	amount *big.Int,
	timeout time.Duration,
) (string, error) {
	signerFn := signingevm.EthereumSignerToBindSignerFn(ctx, c.signer, c.chainID)
	opts := &bind.TransactOpts{
		From:    c.signerAddress,
		Signer:  signerFn,
		Context: ctx,
	}

	iftContract, err := ift.NewContract(common.HexToAddress(iftAddress), c.client)
	if err != nil {
		return "", fmt.Errorf("creating ift contract binding at %s: %w", iftAddress, err)
	}

	timeoutTimestamp := uint64(time.Now().Add(timeout).UTC().Unix()) //nolint:gosec // G115 bounded conversion
	tx, err := iftContract.IftTransfer(opts, clientID, receiver, amount, timeoutTimestamp)
	if err != nil {
		return "", fmt.Errorf("sending ift transfer from %s of %s to %s from client %s: %w", c.signerAddress, amount, receiver, clientID, err)
	}

	if err = c.WaitForTx(ctx, tx.Hash().String()); err != nil {
		return "", fmt.Errorf("waiting for receipt of ift transfer tx %s: %w", tx.Hash().String(), err)
	}
	return tx.Hash().String(), nil
}

func (c *EVMBridgeClient) TimestampAtHeight(ctx context.Context, height uint64) (time.Time, error) {
	header, err := c.client.HeaderByNumber(ctx, big.NewInt(int64(height))) //nolint:gosec // G115 bounded conversion
	if err != nil {
		return time.Time{}, fmt.Errorf("getting block header at height %d: %w", height, err)
	}
	return time.Unix(int64(header.Time), 0).UTC(), nil //nolint:gosec // G115 bounded conversion
}

func (c *EVMBridgeClient) ClientState(ctx context.Context, clientID string) (ClientState, error) {
	addr, err := c.ics26Router.GetClient(&bind.CallOpts{Context: ctx}, clientID)
	if err != nil {
		return ClientState{}, fmt.Errorf("getting client %s: %w", clientID, err)
	}
	if addr == common.BytesToAddress(make([]byte, 32)) {
		return ClientState{}, fmt.Errorf("could not find client %s: %w", clientID, err)
	}

	lightClient, err := sp1_ics07_tendermint.NewContract(addr, c.client)
	if err != nil {
		return ClientState{}, fmt.Errorf("creating new instance of sp1 ics07 tendermint contract at address %s: %w", addr.String(), err)
	}

	decodedState, err := lightClient.ClientState(&bind.CallOpts{Context: ctx})
	if err != nil {
		return ClientState{}, fmt.Errorf("getting tendermint client state at address %s: %w", addr, err)
	}

	return ClientState{
		TendermintClientState: &TendermintClientState{
			TrustingPeriod: time.Duration(decodedState.TrustingPeriod) * time.Second,
			LatestHeight:   decodedState.LatestHeight.RevisionHeight,
		},
	}, nil
}

func (c *EVMBridgeClient) WaitForTx(ctx context.Context, hash string) error {
	const tick = time.Second * 3
	ticker := time.NewTicker(tick)

	start := time.Now()
	timeout := time.Minute
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			ticker.Stop()

			receipt, err := c.client.TransactionReceipt(ctx, common.HexToHash(hash))
			if err != nil {
				ticker.Reset(tick)
				continue
			}
			if receipt != nil {
				return nil
			}

			if time.Since(start) > timeout {
				return fmt.Errorf("timeout after waiting %s for transaction receipt", timeout.String())
			}

			ticker.Reset(tick)
		}
	}
}

func (c *EVMBridgeClient) GetTransactionSender(ctx context.Context, hash string) (string, error) {
	return c.txSigner(ctx, common.HexToHash(hash))
}
