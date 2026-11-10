package ibcv2

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"

	sdkmath "cosmossdk.io/math"
	abci "github.com/cometbft/cometbft/abci/types"
	cometbytes "github.com/cometbft/cometbft/libs/bytes"
	tmrpc "github.com/cometbft/cometbft/rpc/client"
	coretypes "github.com/cometbft/cometbft/rpc/core/types"
	"github.com/cosmos/cosmos-sdk/client"
	"github.com/cosmos/cosmos-sdk/codec"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	"github.com/cosmos/cosmos-sdk/std"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/bech32"
	"github.com/cosmos/cosmos-sdk/types/msgservice"
	txtypes "github.com/cosmos/cosmos-sdk/types/tx"
	"github.com/cosmos/cosmos-sdk/x/auth/tx"
	authtypes "github.com/cosmos/cosmos-sdk/x/auth/types"
	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"
	"github.com/cosmos/gogoproto/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/cosmos/ibc-relayer/db/gen/db"
	ibcchannelv2types "github.com/cosmos/ibc-relayer/proto/gen/ibc/core/channel/v2"
	ibcclientv1types "github.com/cosmos/ibc-relayer/proto/gen/ibc/core/client/v1"
	attestationsv1types "github.com/cosmos/ibc-relayer/proto/gen/ibc/lightclients/attestations/v1"
	tendermintv1types "github.com/cosmos/ibc-relayer/proto/gen/ibc/lightclients/tendermint/v1"
	wasmv1types "github.com/cosmos/ibc-relayer/proto/gen/ibc/lightclients/wasm/v1"
	"github.com/cosmos/ibc-relayer/shared/config"
	"github.com/cosmos/ibc-relayer/shared/lmt"
	"github.com/cosmos/ibc-relayer/shared/signing"
)

const (
	attributeKeyPacketSourceClient     = "packet_source_client"
	attributeKeyPacketDestClient       = "packet_dest_client"
	attributeKeyPacketSequence         = "packet_sequence"
	attributeKeyPacketTimeoutTimestamp = "packet_timeout_timestamp"

	eventTypeRecvPacket    = "recv_packet"
	eventTypeAckPacket     = "acknowledge_packet"
	eventTypeTimeoutPacket = "timeout_packet"
	eventTypeWriteAck      = "write_acknowledgement"
	eventTypeSendPacket    = "send_packet"
)

func newInterfaceRegistry() codectypes.InterfaceRegistry {
	reg := codectypes.NewInterfaceRegistry()
	std.RegisterInterfaces(reg)
	authtypes.RegisterInterfaces(reg)

	msgs := []proto.Message{
		&ibcchannelv2types.MsgSendPacket{},
		&ibcchannelv2types.MsgRecvPacket{},
		&ibcchannelv2types.MsgTimeout{},
		&ibcchannelv2types.MsgAcknowledgement{},
		&ibcclientv1types.MsgUpdateClient{},
		&attestationsv1types.ClientState{},
		&attestationsv1types.ConsensusState{},
		&attestationsv1types.AttestationProof{},
		&tendermintv1types.ClientState{},
		&tendermintv1types.Misbehaviour{},
		&tendermintv1types.ConsensusState{},
		&tendermintv1types.Header{},
		&tendermintv1types.Fraction{},
		&wasmv1types.ClientState{},
		&wasmv1types.ConsensusState{},
		&wasmv1types.ClientMessage{},
	}

	reg.RegisterImplementations((*sdk.Msg)(nil), msgs...)
	msgservice.RegisterMsgServiceDesc(reg, &ibcchannelv2types.Msg_serviceDesc)

	return reg
}

type CosmosBridgeClient struct {
	chainID string
	signer  signing.Signer
	prefix  string

	gasPrice  float64
	feeDenom  string
	feeAmount uint64

	conn  grpc.ClientConnInterface
	tmrpc tmrpc.Client

	txConfig client.TxConfig
	cdc      *codec.ProtoCodec
	reg      codectypes.InterfaceRegistry

	txOnce             *OnceWithKey[*coretypes.ResultTx]
	txSubmissionLock   *sync.Mutex
	lastSubmissionTime time.Time
	txSubmissionDelay  time.Duration

	latestTimestampOnce *OnceWithKey[time.Time]
}

func NewCosmosBridgeClient(
	chainID string,
	signer signing.Signer,
	prefix string,
	gasPrice float64,
	feeDenom string,
	feeAmount uint64,
	conn grpc.ClientConnInterface,
	rpcClient tmrpc.Client,
	txSubmissionDelay *time.Duration,
) *CosmosBridgeClient {
	delay := 2 * time.Second
	if txSubmissionDelay != nil {
		delay = *txSubmissionDelay
	}

	reg := newInterfaceRegistry()
	cdc := codec.NewProtoCodec(reg)

	return &CosmosBridgeClient{
		chainID:             chainID,
		signer:              signer,
		prefix:              prefix,
		gasPrice:            gasPrice,
		feeDenom:            feeDenom,
		feeAmount:           feeAmount,
		conn:                conn,
		tmrpc:               rpcClient,
		txConfig:            tx.NewTxConfig(cdc, tx.DefaultSignModes),
		cdc:                 cdc,
		reg:                 reg,
		txOnce:              NewOnceWithKey[*coretypes.ResultTx](time.Minute, 5*time.Second),
		txSubmissionLock:    new(sync.Mutex),
		txSubmissionDelay:   delay,
		latestTimestampOnce: NewOnceWithKey[time.Time](6*time.Second, 200*time.Millisecond),
	}
}

func (c *CosmosBridgeClient) IsPacketReceived(ctx context.Context, destinationClientID string, sequence uint64) (bool, error) {
	in := &ibcchannelv2types.QueryPacketReceiptRequest{
		ClientId: destinationClientID,
		Sequence: sequence,
	}
	resp, err := ibcchannelv2types.NewQueryClient(c.conn).PacketReceipt(ctx, in)
	if err != nil {
		return false, fmt.Errorf("querying for packet receipt with sequence number %d to destination client %s: %w", sequence, destinationClientID, err)
	}

	return resp.Received, nil
}

func (c *CosmosBridgeClient) IsPacketCommitted(ctx context.Context, sourceClientID string, sequence uint64) (bool, error) {
	in := &ibcchannelv2types.QueryPacketCommitmentRequest{
		ClientId: sourceClientID,
		Sequence: sequence,
	}
	_, err := ibcchannelv2types.NewQueryClient(c.conn).PacketCommitment(ctx, in)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			if st.Code() == codes.NotFound {
				return false, nil
			}
		}
		return false, fmt.Errorf("querying for packet commitment with sequence number %d to source client %s: %w", sequence, sourceClientID, err)
	}

	return true, nil
}

func (c *CosmosBridgeClient) FindRecvTx(
	ctx context.Context,
	sourceClientID string,
	destClientID string,
	sequence uint64,
	timeoutTimestamp time.Time,
) (*BridgeTx, error) {
	query := strings.Join([]string{
		fmt.Sprintf("%s.%s='%s'", eventTypeRecvPacket, attributeKeyPacketSourceClient, sourceClientID),
		fmt.Sprintf("%s.%s='%s'", eventTypeRecvPacket, attributeKeyPacketDestClient, destClientID),
		fmt.Sprintf("%s.%s=%d", eventTypeRecvPacket, attributeKeyPacketSequence, sequence),
		fmt.Sprintf("%s.%s=%d", eventTypeRecvPacket, attributeKeyPacketTimeoutTimestamp, timeoutTimestamp.Unix()),
	}, " AND ")

	searchResult, err := c.tmrpc.TxSearch(ctx, query, false, nil, nil, "desc")
	if err != nil {
		return nil, fmt.Errorf(
			"searching for recv tx from source client %s to dest client %s with sequence %d and timeout tx %d: %w",
			sourceClientID,
			destClientID,
			sequence,
			timeoutTimestamp.UnixNano(),
			err,
		)
	}
	if searchResult.TotalCount == 0 {
		return nil, ErrTxNotFound
	}
	if searchResult.TotalCount != 1 {
		return nil, fmt.Errorf("expected only 1 tx, but got %d", searchResult.TotalCount)
	}

	recvTx := searchResult.Txs[0]

	// fetch header at block height to get timestamp
	headerResult, err := c.tmrpc.Header(ctx, &recvTx.Height)
	if err != nil {
		return nil, fmt.Errorf("fetching block header at recv tx height %d: %w", recvTx.Height, err)
	}
	if headerResult.Header == nil {
		return nil, fmt.Errorf("fetched block header at recv tx height %d without err but header was nil", recvTx.Height)
	}

	signer, err := c.txSigner(recvTx)
	if err != nil {
		// the above could fail for many reasons (some other relayer included
		// some crazy message types in the recv we are unable to decode, weird
		// signature type, etc), however we should not return an error to the
		// client since, we have most of the relevant info that is required,
		// besides the signer address which is not critical
		lmt.Logger(ctx).Error("found recv tx but could not determine the tx's signer, ignoring error and continuing", zap.Error(err), zap.String("tx_hash", recvTx.Hash.String()))
	}

	return &BridgeTx{
		Hash:           recvTx.Hash.String(),
		Timestamp:      headerResult.Header.Time,
		RelayerAddress: signer,
	}, nil
}

func (c *CosmosBridgeClient) FindAckTx(
	ctx context.Context,
	sourceClientID string,
	destClientID string,
	sequence uint64,
) (*BridgeTx, error) {
	query := strings.Join([]string{
		fmt.Sprintf("%s.%s='%s'", eventTypeAckPacket, attributeKeyPacketSourceClient, sourceClientID),
		fmt.Sprintf("%s.%s='%s'", eventTypeAckPacket, attributeKeyPacketDestClient, destClientID),
		fmt.Sprintf("%s.%s=%d", eventTypeAckPacket, attributeKeyPacketSequence, sequence),
	}, " AND ")

	searchResult, err := c.tmrpc.TxSearch(ctx, query, false, nil, nil, "desc")
	if err != nil {
		return nil, fmt.Errorf(
			"searching for ack tx from source client %s to dest client %s with sequence %d: %w",
			sourceClientID,
			destClientID,
			sequence,
			err,
		)
	}
	if searchResult.TotalCount == 0 {
		return nil, ErrTxNotFound
	}
	if searchResult.TotalCount != 1 {
		return nil, fmt.Errorf("expected only 1 tx, but got %d", searchResult.TotalCount)
	}

	ackTx := searchResult.Txs[0]

	// fetch header at block height to get timestamp
	headerResult, err := c.tmrpc.Header(ctx, &ackTx.Height)
	if err != nil {
		return nil, fmt.Errorf("fetching block header at ack tx height %d: %w", ackTx.Height, err)
	}
	if headerResult.Header == nil {
		return nil, fmt.Errorf("fetched block header at ack tx height %d without err but header was nil", ackTx.Height)
	}

	signer, err := c.txSigner(ackTx)
	if err != nil {
		// the above could fail for many reasons (some other relayer included
		// some crazy message types in the ack we are unable to decode, weird
		// signature type, etc), however we should not return an error to the
		// client since, we have most of the relevant info that is required,
		// besides the signer address which is not critical
		lmt.Logger(ctx).Error("found ack tx but could not determine the tx's signer, ignoring error and continuing", zap.Error(err), zap.String("tx_hash", ackTx.Hash.String()))
	}

	return &BridgeTx{
		Hash:           ackTx.Hash.String(),
		Timestamp:      headerResult.Header.Time,
		RelayerAddress: signer,
	}, nil
}

func (c *CosmosBridgeClient) FindTimeoutTx(
	ctx context.Context,
	sourceClientID string,
	destClientID string,
	sequence uint64,
) (*BridgeTx, error) {
	query := strings.Join([]string{
		fmt.Sprintf("%s.%s='%s'", eventTypeTimeoutPacket, attributeKeyPacketSourceClient, sourceClientID),
		fmt.Sprintf("%s.%s='%s'", eventTypeTimeoutPacket, attributeKeyPacketDestClient, destClientID),
		fmt.Sprintf("%s.%s=%d", eventTypeTimeoutPacket, attributeKeyPacketSequence, sequence),
	}, " AND ")

	searchResult, err := c.tmrpc.TxSearch(ctx, query, false, nil, nil, "desc")
	if err != nil {
		return nil, fmt.Errorf(
			"searching for timeout tx from source client %s to dest client %s with sequence %d: %w",
			sourceClientID,
			destClientID,
			sequence,
			err,
		)
	}
	if searchResult.TotalCount == 0 {
		return nil, ErrTxNotFound
	}
	if searchResult.TotalCount != 1 {
		return nil, fmt.Errorf("expected only 1 tx, but got %d", searchResult.TotalCount)
	}

	timeoutTx := searchResult.Txs[0]

	// fetch header at block height to get timestamp
	headerResult, err := c.tmrpc.Header(ctx, &timeoutTx.Height)
	if err != nil {
		return nil, fmt.Errorf("fetching block header at timeout tx height %d: %w", timeoutTx.Height, err)
	}
	if headerResult.Header == nil {
		return nil, fmt.Errorf("fetched block header at timeout tx height %d without err but header was nil", timeoutTx.Height)
	}

	signer, err := c.txSigner(timeoutTx)
	if err != nil {
		// the above could fail for many reasons (some other relayer included
		// some crazy message types in the timeout we are unable to decode, weird
		// signature type, etc), however we should not return an error to the
		// client since, we have most of the relevant info that is required,
		// besides the signer address which is not critical
		lmt.Logger(ctx).Error("found timeout tx but could not determine the tx's signer, ignoring error and continuing", zap.Error(err), zap.String("tx_hash", timeoutTx.Hash.String()))
	}

	return &BridgeTx{
		Hash:           timeoutTx.Hash.String(),
		Timestamp:      headerResult.Header.Time,
		RelayerAddress: signer,
	}, nil
}

func (c *CosmosBridgeClient) DeliverTx(ctx context.Context, bz []byte, _ string) (*BridgeTx, error) {
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

	bech32Address, err := bech32.ConvertAndEncode(c.prefix, c.signer.Address(ctx))
	if err != nil {
		return nil, fmt.Errorf("converting and encoding signer address %s with prefix %s to bech32: %w", c.signer.Address(ctx), c.prefix, err)
	}

	hash, err := c.signAndSubmit(ctx, bz)
	if err != nil {
		return nil, fmt.Errorf("signing and submitting recv tx: %w", err)
	}

	return &BridgeTx{
		Hash:           hash.String(),
		Timestamp:      time.Now().UTC(),
		RelayerAddress: bech32Address,
	}, nil
}

func (c *CosmosBridgeClient) PacketWriteAckStatus(
	ctx context.Context,
	hash string,
	sequence uint64,
	sourceClientID string,
	destClientID string,
) (db.Ibcv2WriteAckStatus, error) {
	hashBytes, err := hex.DecodeString(hash)
	if err != nil {
		return db.Ibcv2WriteAckStatusUNKNOWN, fmt.Errorf("hex decoding tx hash string %s: %w", hash, err)
	}

	// we use the once wrapper so that this function is only called once for a
	// particular value of hash bytes even for many invocations
	txResult, err := c.txOnce.Do(hash, func() (*coretypes.ResultTx, error) {
		return c.tmrpc.Tx(ctx, hashBytes, false)
	})
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return db.Ibcv2WriteAckStatusUNKNOWN, ErrTxNotFound
		}
		return db.Ibcv2WriteAckStatusUNKNOWN, fmt.Errorf("getting tx %s's result: %w", hash, err)
	}

	for _, event := range txResult.TxResult.GetEvents() {
		if event.GetType() != eventTypeWriteAck {
			continue
		}

		var sequenceMatch, sourceClientMatch, destClientMatch bool
		for _, attribute := range event.GetAttributes() {
			switch attribute.GetKey() {
			case attributeKeyPacketSequence:
				if attribute.GetValue() == strconv.Itoa(int(sequence)) { //nolint:gosec // G115 bounded conversion
					sequenceMatch = true
				}
			case attributeKeyPacketSourceClient:
				if attribute.GetValue() == sourceClientID {
					sourceClientMatch = true
				}
			case attributeKeyPacketDestClient:
				if attribute.GetValue() == destClientID {
					destClientMatch = true
				}
			default:
				continue
			}
		}
		if sequenceMatch && sourceClientMatch && destClientMatch {
			// note that we dont want to parse the write ack until we know that
			// the write ack is for the packet that we are interested in, i.e.
			// outside of the loop iterating over the events. Since we do not
			// want a write ack for a different packet to possibly fail parsing
			// and return an error.
			return parseWriteAckStatusForPacket(event)
		}
	}

	return db.Ibcv2WriteAckStatusUNKNOWN, ErrWriteAckNotFoundForPacket
}

func parseWriteAckStatusForPacket(writeAckEvent abci.Event) (db.Ibcv2WriteAckStatus, error) {
	for _, attribute := range writeAckEvent.GetAttributes() {
		if attribute.GetKey() != "encoded_acknowledgement_hex" {
			continue
		}
		protoAckBytes, err := hex.DecodeString(attribute.GetValue())
		if err != nil {
			return db.Ibcv2WriteAckStatusUNKNOWN, fmt.Errorf("%w:%w", ErrWriteAckDecoding, err)
		}

		var ack ibcchannelv2types.Acknowledgement
		if err = proto.Unmarshal(protoAckBytes, &ack); err != nil {
			return db.Ibcv2WriteAckStatusUNKNOWN, fmt.Errorf("%w:%w", ErrWriteAckDecoding, err)
		}

		if len(ack.GetAppAcknowledgements()) == 1 {
			// if the ack is an error, there will only be a single app
			// acknowledgement that is the universal error ack
			if bytes.Equal(ack.GetAppAcknowledgements()[0], ErrorAcknowledgement[:]) {
				return db.Ibcv2WriteAckStatusERROR, nil
			}
		}
		return db.Ibcv2WriteAckStatusSUCCESS, nil
	}
	return db.Ibcv2WriteAckStatusUNKNOWN, nil
}

func (c *CosmosBridgeClient) SendPacketsFromTx(ctx context.Context, sourceChainID string, txHash string) ([]*PacketInfo, error) {
	hashBytes, err := hex.DecodeString(txHash)
	if err != nil {
		return nil, fmt.Errorf("hex decoding tx hash: %w", err)
	}

	txResult, err := c.tmrpc.Tx(ctx, hashBytes, false)
	if err != nil {
		return nil, fmt.Errorf("getting tx result: %w", err)
	}

	packets := make([]*PacketInfo, 0, len(txResult.TxResult.GetEvents()))
	for _, event := range txResult.TxResult.GetEvents() {
		if event.GetType() != eventTypeSendPacket {
			continue
		}
		var packet PacketInfo
		for _, attribute := range event.GetAttributes() {
			switch attribute.GetKey() {
			case attributeKeyPacketSequence:
				sequence, err := strconv.Atoi(attribute.GetValue())
				if err != nil {
					return nil, fmt.Errorf("converting packet sequence %s to int", attribute.GetValue())
				}
				packet.Sequence = uint64(sequence) //nolint:gosec // G115 bounded conversion
			case attributeKeyPacketSourceClient:
				packet.SourceClient = attribute.GetValue()
			case attributeKeyPacketDestClient:
				packet.DestinationClient = attribute.GetValue()
			case attributeKeyPacketTimeoutTimestamp:
				unixts, err := strconv.Atoi(attribute.GetValue())
				if err != nil {
					return nil, fmt.Errorf("converting attribute timeout ts %s to int", attribute.GetValue())
				}
				packet.TimeoutTimestamp = time.Unix(int64(unixts), 0)
			default:
				continue
			}
		}
		if packet.SourceClient == "" || packet.DestinationClient == "" {
			// v1 send packets do not contain this info, we do not care about
			// them
			continue
		}
		packets = append(packets, &packet)
	}
	if len(packets) == 0 {
		return nil, nil
	}

	// only query for the header if we have actually found packets within the
	// tx, doing this after the loop just saves us a call if there are not send
	// packets found in the tx
	header, err := c.tmrpc.Header(ctx, &txResult.Height)
	if err != nil {
		return nil, fmt.Errorf("getting header for height %d: %w", txResult.Height, err)
	}
	if header.Header == nil {
		return nil, fmt.Errorf("got nil header at height %d", txResult.Height)
	}

	payloadsByClient := c.sendPacketPayloads(txResult.Tx)
	seenByClient := make(map[string]int)

	for _, packet := range packets {
		packet.Timestamp = header.Header.Time

		list := payloadsByClient[packet.SourceClient]
		index := seenByClient[packet.SourceClient]
		if index < len(list) {
			packet.Transfer = list[index]
			seenByClient[packet.SourceClient]++
		}
	}

	return packets, nil
}

func (c *CosmosBridgeClient) sendPacketPayloads(txBytes []byte) map[string][]*TransferPayload {
	result := make(map[string][]*TransferPayload)

	decoded, err := c.txConfig.TxDecoder()(txBytes)
	if err != nil {
		return result
	}

	for _, msg := range decoded.GetMsgs() {
		send, ok := msg.(*ibcchannelv2types.MsgSendPacket)
		if !ok || len(send.Payloads) == 0 {
			continue
		}

		payload := send.Payloads[0]
		result[send.SourceClient] = append(
			result[send.SourceClient],
			DecodeTransferPayload(payload.SourcePort, payload.Encoding, payload.Value),
		)
	}

	return result
}

func (c *CosmosBridgeClient) ShouldRetryTx(ctx context.Context, txHash string, expiry time.Duration, sentTs time.Time) (bool, error) {
	txHashBytes, err := hex.DecodeString(txHash)
	if err != nil {
		return false, fmt.Errorf("decoding hex tx hash %s to bytes: %w", txHash, err)
	}

	resp, err := c.txOnce.Do(txHash, func() (*coretypes.ResultTx, error) { return c.tmrpc.Tx(ctx, txHashBytes, false) })
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			// tx not found on chain, check if we have waited longer than the
			// expiry of on chain time, and if we have, the tx should be
			// retried

			latestHeaderResult, err := c.tmrpc.Header(ctx, nil)
			if err != nil {
				return false, fmt.Errorf("getting latest block header: %w", err)
			}
			if latestHeaderResult.Header == nil {
				return false, errors.New("fetched block header at latest height without err but header was nil")
			}

			expiresAt := sentTs.UTC().Add(expiry)
			isExpired := expiresAt.Before(latestHeaderResult.Header.Time.UTC())
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
		return false, fmt.Errorf("fetching tx %s on chain: %w", txHash, err)
	}
	if resp.TxResult.GetCode() != 0 {
		// tx was unsuccessful, needs to be retried
		return true, nil
	}

	// tx exists and was successful, no need to retry
	return false, nil
}

func (c *CosmosBridgeClient) WaitForChain(ctx context.Context) error {
	const initialTick = time.Millisecond
	const tick = time.Second

	ticker := time.NewTicker(initialTick)

	now := time.Now()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			ticker.Stop()

			latest, err := c.tmrpc.Header(ctx, nil)
			if err != nil {
				return fmt.Errorf("getting latest block header: %w", err)
			}
			if latest.Header == nil {
				return errors.New("latest header was nil")
			}

			if latest.Header.Time.After(now) {
				return nil
			}

			if time.Since(now) > time.Second*30 {
				// if the chain hasnt reached the current height in 30s, something
				// may be wrong and we should start logging warnings
				lmt.Logger(ctx).Warn(
					"latest header time is behind current time, waiting... (already been waiting for >30s)",
					zap.Time("chain_time", latest.Header.Time),
					zap.Time("wait_until", now),
				)
			} else {
				lmt.Logger(ctx).Debug(
					"latest header time is behind current time, waiting...",
					zap.Time("chain_time", latest.Header.Time),
					zap.Time("wait_until", now),
				)
			}

			ticker.Reset(tick)
		}
	}
}

func (*CosmosBridgeClient) IsTxFinalized(ctx context.Context, txHash string, offset *uint64) (bool, error) {
	return true, nil
}

func (c *CosmosBridgeClient) IsTimestampFinalized(ctx context.Context, timestamp time.Time, offset *uint64) (bool, error) {
	// Cosmos chains have instant finality, so we just need to check if the latest
	// block timestamp has passed the target timestamp.
	// Note: We don't use LatestOnChainTimestamp here because it caches the result.
	header, err := c.tmrpc.Header(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("getting latest header: %w", err)
	}
	if header.Header == nil {
		return false, errors.New("got nil header when fetching latest header")
	}
	return header.Header.Time.After(timestamp), nil
}

func (*CosmosBridgeClient) ChainType() config.ChainType {
	return config.ChainTypeCOSMOS
}

func (c *CosmosBridgeClient) signAndSubmit(ctx context.Context, bz []byte) (cometbytes.HexBytes, error) {
	var txBody txtypes.TxBody
	if err := proto.Unmarshal(bz, &txBody); err != nil {
		return nil, fmt.Errorf("unmarshaling tx bytes: %w", err)
	}
	if len(txBody.Messages) == 0 {
		return nil, errors.New("no messages to relay")
	}

	var numClientUpdates int
	updateClientTypeURL := codectypes.MsgTypeURL(&ibcclientv1types.MsgUpdateClient{})

	msgs := make([]sdk.Msg, 0, len(txBody.Messages))
	for _, msg := range txBody.Messages {
		var sdkMsg sdk.Msg
		if err := c.reg.UnpackAny(msg, &sdkMsg); err != nil {
			return nil, fmt.Errorf("unpacking msg into sdk.Msg: %w", err)
		}
		if codectypes.MsgTypeURL(sdkMsg) == updateClientTypeURL {
			numClientUpdates++
		}
		msgs = append(msgs, sdkMsg)
	}

	if numClientUpdates > 1 {
		// if the number of update client messages is > 1, then this tx is going to
		// be too large to submit (http limitation, code 413). to avoid this,
		// we will submit each update client one by one until there are none
		// left. then we will submit the non update client messages
		lmt.Logger(ctx).Warn(
			fmt.Sprintf("found %d update client messages tx, submitting them on chain individually", numClientUpdates),
			zap.Int("num_client_updates", numClientUpdates),
		)
		var numSubmitted int
		var nonClientUpdateMsgs []sdk.Msg
		for _, msg := range msgs {
			if codectypes.MsgTypeURL(msg) != updateClientTypeURL {
				nonClientUpdateMsgs = append(nonClientUpdateMsgs, msg)
				continue
			}

			hash, err := c.signAndSubmitMessages(ctx, msg)
			if err != nil {
				return nil, fmt.Errorf("submitting individual update client message number %d since there were %d included in tx: %w", numSubmitted, numClientUpdates, err)
			}
			numSubmitted++

			lmt.Logger(ctx).Info(
				fmt.Sprintf("submitted individual client update, sleeping for %s to submit next tx", c.txSubmissionDelay),
				zap.String("tx_hash", hash.String()),
			)
			time.Sleep(c.txSubmissionDelay)
		}

		// we have submitted all client update msgs individually, now only
		// submit non update client messages
		lmt.Logger(ctx).Info("submitted all individual client updates", zap.Int("num_client_update", numClientUpdates))
		msgs = nonClientUpdateMsgs
	}

	return c.signAndSubmitMessages(ctx, msgs...)
}

func (c *CosmosBridgeClient) signAndSubmitMessages(ctx context.Context, msgs ...sdk.Msg) (cometbytes.HexBytes, error) {
	bech32Address, err := bech32.ConvertAndEncode(c.prefix, c.signer.Address(ctx))
	if err != nil {
		return nil, fmt.Errorf("converting and encoding signer address %s with prefix %s to bech32: %w", c.signer.Address(ctx), c.prefix, err)
	}

	accountClient := authtypes.NewQueryClient(c.conn)
	account, err := accountClient.AccountInfo(ctx, &authtypes.QueryAccountInfoRequest{Address: bech32Address})
	if err != nil {
		return nil, fmt.Errorf("getting account info for cosmos signer %s on chain %s: %w", bech32Address, c.chainID, err)
	}
	accountInfo := account.GetInfo()
	if accountInfo == nil {
		return nil, fmt.Errorf("got no info for cosmos signer account %s on chain %s: %w", bech32Address, c.chainID, err)
	}

	txBuilder := c.txConfig.NewTxBuilder()
	if err = txBuilder.SetMsgs(msgs...); err != nil {
		return nil, fmt.Errorf("setting message in tx: %w", err)
	}

	gasLimit, err := c.getEstimatedGasLimit(ctx, txBuilder, accountInfo)
	if err != nil {
		return nil, fmt.Errorf("getting estimated gas limit for tx: %w", err)
	}
	txBuilder.SetGasLimit(gasLimit)

	if c.feeDenom != "" {
		fee, err := c.getTxFee(gasLimit)
		if err != nil {
			return nil, fmt.Errorf("getting tx fee: %w", err)
		}
		txBuilder.SetFeeAmount(sdk.NewCoins(fee))
	}

	unsigedTx := signing.NewCosmosTransaction(txBuilder.GetTx(), accountInfo.GetAccountNumber(), accountInfo.Sequence, c.txConfig)
	signedTx, err := c.signer.Sign(ctx, c.chainID, unsigedTx)
	if err != nil {
		return nil, fmt.Errorf("signing tx: %w", err)
	}

	signedTxBytes, err := c.txConfig.TxEncoder()(signedTx.(*signing.CosmosTransaction).Tx)
	if err != nil {
		return nil, fmt.Errorf("encoding signed tx: %w", err)
	}

	result, err := c.tmrpc.BroadcastTxSync(ctx, signedTxBytes)
	if err != nil {
		return nil, fmt.Errorf("broadcasting tx synchronously from signer %s: %w", bech32Address, err)
	}
	if result.Code != 0 {
		return nil, fmt.Errorf("broadcasting cosmos tx from signer %s returned error code %d: %s", bech32Address, result.Code, result.Log)
	}

	return result.Hash, nil
}

func (c *CosmosBridgeClient) getTxFee(gasLimit uint64) (sdk.Coin, error) {
	if c.feeDenom == "" {
		return sdk.Coin{}, fmt.Errorf("fee denom not set for chain %s", c.chainID)
	}
	if c.gasPrice > 0 && c.feeAmount > 0 {
		return sdk.Coin{}, fmt.Errorf("gas price and fee amount cannot both be set for chain %s", c.chainID)
	}

	if c.gasPrice > 0 {
		feeAmount, _ := new(big.Float).Mul(big.NewFloat(c.gasPrice), new(big.Float).SetInt64(int64(gasLimit))).Int(nil) //nolint:gosec // G115 bounded conversion
		feeAmount.Add(feeAmount, big.NewInt(1))
		return sdk.NewCoin(c.feeDenom, sdkmath.NewIntFromBigInt(feeAmount)), nil
	}
	if c.feeAmount > 0 {
		return sdk.NewCoin(c.feeDenom, sdkmath.NewIntFromUint64(c.feeAmount)), nil
	}

	return sdk.NewCoin(c.feeDenom, sdkmath.NewIntFromUint64(0)), nil
}

func (c *CosmosBridgeClient) getEstimatedGasLimit(ctx context.Context, txBuilder client.TxBuilder, accountInfo *authtypes.BaseAccount) (uint64, error) {
	unsigedTx := signing.NewCosmosTransaction(txBuilder.GetTx(), accountInfo.GetAccountNumber(), accountInfo.GetSequence(), c.txConfig)
	signedTx, err := c.signer.Sign(ctx, c.chainID, unsigedTx)
	if err != nil {
		return 0, fmt.Errorf("signing tx: %w", err)
	}

	signedTxBytes, err := c.txConfig.TxEncoder()(signedTx.(*signing.CosmosTransaction).Tx)
	if err != nil {
		return 0, fmt.Errorf("encoding signed tx: %w", err)
	}

	txService := txtypes.NewServiceClient(c.conn)
	res, err := txService.Simulate(ctx, &txtypes.SimulateRequest{TxBytes: signedTxBytes})
	if err != nil {
		return 0, fmt.Errorf("simulating tx to get estimated gas usage: %w", err)
	}
	if res.GetGasInfo() == nil {
		return 0, errors.New("got nil gas info after simulating tx")
	}

	// multiply gas used by a multiplier to give us a buffer on the gas limit
	const gasMultiplier float64 = 1.5
	return uint64(math.Ceil(float64(res.GetGasInfo().GetGasUsed()) * gasMultiplier)), nil
}

func (c *CosmosBridgeClient) LatestOnChainTimestamp(ctx context.Context) (time.Time, error) {
	return c.latestTimestampOnce.Do("key", func() (time.Time, error) {
		header, err := c.tmrpc.Header(ctx, nil)
		if err != nil {
			return time.Time{}, fmt.Errorf("getting latest header: %w", err)
		}
		if header.Header == nil {
			return time.Time{}, errors.New("got nil header when fetching latest header")
		}
		return header.Header.Time, nil
	})
}

func (c *CosmosBridgeClient) SignerGasTokenBalance(ctx context.Context) (*big.Int, error) {
	bech32Address, err := bech32.ConvertAndEncode(c.prefix, c.signer.Address(ctx))
	if err != nil {
		return nil, fmt.Errorf("converting and encoding signer address %s with prefix %s to bech32: %w", c.signer.Address(ctx), c.prefix, err)
	}

	req := &banktypes.QueryBalanceRequest{Address: bech32Address, Denom: c.feeDenom}
	resp, err := banktypes.NewQueryClient(c.conn).Balance(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("getting %s balance for signer on chain %s: %w", c.feeDenom, c.chainID, err)
	}
	if resp.GetBalance() == nil {
		return nil, fmt.Errorf("got nil %s balance for signer on chain %s: %w", c.feeDenom, c.chainID, err)
	}
	return resp.GetBalance().Amount.BigInt(), nil
}

func (c *CosmosBridgeClient) TxFee(ctx context.Context, txHash string) (*big.Int, error) {
	hashBytes, err := hex.DecodeString(txHash)
	if err != nil {
		return nil, fmt.Errorf("hex decoding tx hash string %s: %w", txHash, err)
	}

	result, err := c.txOnce.Do(txHash, func() (*coretypes.ResultTx, error) {
		return c.tmrpc.Tx(ctx, hashBytes, false)
	})
	if err != nil {
		return nil, fmt.Errorf("getting results for tx hash %s: %w", txHash, err)
	}

	decodedTx, err := c.txConfig.TxDecoder()(result.Tx)
	if err != nil {
		return nil, fmt.Errorf("decoding tx result: %w", err)
	}

	feeTx, ok := decodedTx.(sdk.FeeTx)
	if !ok {
		return nil, errors.New("could not convert decoded tx to sdk.FeeTx")
	}

	fee := feeTx.GetFee().AmountOf(c.feeDenom)
	if fee.IsNil() {
		return big.NewInt(0), nil
	}

	return fee.BigInt(), nil
}

func (c *CosmosBridgeClient) TxExecutionStatus(ctx context.Context, txHash string) (TxExecutionStatus, error) {
	hashBytes, err := hex.DecodeString(txHash)
	if err != nil {
		return TxExecutionStatus{}, fmt.Errorf("hex decoding tx hash string %s: %w", txHash, err)
	}

	result, err := c.txOnce.Do(txHash, func() (*coretypes.ResultTx, error) {
		return c.tmrpc.Tx(ctx, hashBytes, false)
	})
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return TxExecutionStatus{}, fmt.Errorf("%w: %w", ErrTxNotFound, err)
		}
		return TxExecutionStatus{}, fmt.Errorf("getting results for tx hash %s: %w", txHash, err)
	}

	if result.TxResult.Code == 0 {
		return TxExecutionStatus{Status: TxExecutionStatusSuccess}, nil
	}

	return TxExecutionStatus{
		Status:       TxExecutionStatusFailed,
		ErrorMessage: result.TxResult.Log,
	}, nil
}

func (c *CosmosBridgeClient) txSigner(result *coretypes.ResultTx) (string, error) {
	decoded, err := c.txConfig.TxDecoder()(result.Tx)
	if err != nil {
		return "", fmt.Errorf("decoding tx result: %w", err)
	}

	for _, msg := range decoded.GetMsgs() {
		switch t := msg.(type) {
		case *ibcchannelv2types.MsgRecvPacket:
			return t.Signer, nil
		case *ibcchannelv2types.MsgAcknowledgement:
			return t.Signer, nil
		case *ibcchannelv2types.MsgTimeout:
			return t.Signer, nil
		default:
			continue
		}
	}
	return "", errors.New("expected recv packet, ack packet, or timeout packet msg but found none")
}

func (*CosmosBridgeClient) SendTransfer(
	ctx context.Context,
	ics20Address string,
	clientID string,
	denom string,
	receiver string,
	amount *big.Int,
	memo string,
	timeout time.Duration,
) (string, error) {
	panic("unimplemented")
}

func (*CosmosBridgeClient) IFTTransfer(
	ctx context.Context,
	iftAddress string,
	clientID string,
	receiver string,
	amount *big.Int,
	timeout time.Duration,
) (string, error) {
	panic("unimplemented")
}

func (c *CosmosBridgeClient) TimestampAtHeight(ctx context.Context, height uint64) (time.Time, error) {
	heightI := int64(height) //nolint:gosec // G115 bounded conversion
	header, err := c.tmrpc.Header(ctx, &heightI)
	if err != nil {
		return time.Time{}, fmt.Errorf("fetching block header at height %d: %w", height, err)
	}
	if header.Header == nil {
		return time.Time{}, fmt.Errorf("fetched block header at height %d without err but header was nil", height)
	}
	return header.Header.Time.UTC(), nil
}

func (c *CosmosBridgeClient) ClientState(ctx context.Context, clientID string) (ClientState, error) {
	req := &ibcclientv1types.QueryClientStateRequest{ClientId: clientID}
	state, err := ibcclientv1types.NewQueryClient(c.conn).ClientState(ctx, req)
	if err != nil {
		return ClientState{}, fmt.Errorf("querying for state of client: %w", err)
	}
	if state.GetClientState() == nil {
		return ClientState{}, errors.New("got nil client state")
	}
	if state.GetClientState().GetValue() == nil {
		return ClientState{}, errors.New("got nil client state value")
	}

	switch state.GetClientState().GetTypeUrl() {
	case "/ibc.lightclients.wasm.v1.ClientState":
		var fullstate wasmv1types.ClientState
		if err = proto.Unmarshal(state.GetClientState().GetValue(), &fullstate); err != nil {
			return ClientState{}, fmt.Errorf("proto decoding full wasm client state (%s): %w", string(state.GetClientState().GetValue()), err)
		}

		var wasmClientState WasmClientState
		if err = json.Unmarshal(fullstate.Data, &wasmClientState); err != nil {
			return ClientState{}, fmt.Errorf("json decoding wasm client state data (%s): %w", string(fullstate.Data), err)
		}

		return ClientState{
			WasmClientState: &wasmClientState,
		}, nil
	default:
		return ClientState{}, fmt.Errorf("unsupported client state type url %s", state.GetClientState().GetTypeUrl())
	}
}

func (c *CosmosBridgeClient) WaitForTx(ctx context.Context, hash string) error {
	hashBytes, err := hex.DecodeString(hash)
	if err != nil {
		return fmt.Errorf("hex decoding tx hash string %s: %w", hash, err)
	}

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

			result, err := c.tmrpc.Tx(ctx, hashBytes, false)
			if err != nil {
				if strings.Contains(err.Error(), "not found") {
					ticker.Reset(tick)
					continue
				}
				return fmt.Errorf("getting tx result: %w", err)
			}
			if result != nil {
				return nil
			}

			if time.Since(start) > timeout {
				return fmt.Errorf("timeout after waiting %s for transaction result", timeout.String())
			}

			ticker.Reset(tick)
		}
	}
}

func (*CosmosBridgeClient) GetTransactionSender(ctx context.Context, hash string) (string, error) {
	return "", errors.New("GetTransactionSender not implemented")
}
