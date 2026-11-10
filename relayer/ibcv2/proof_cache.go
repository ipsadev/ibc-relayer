package ibcv2

import (
	"context"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"

	"github.com/cosmos/ibc-relayer/proto/gen/proofapi"
	"github.com/cosmos/ibc-relayer/shared/lmt"
)

const (
	DefaultProofCacheTTL     = 30 * time.Minute
	DefaultProofCacheMaxUses = 5
)

type cachedRelay struct {
	response *proofapi.RelayByTxResponse
	storedAt time.Time
	uses     int
}

type CachingProofAPIClient struct {
	proofapi.ProofApiServiceClient

	ttl     time.Duration
	maxUses int
	now     func() time.Time

	mu       sync.Mutex
	entries  map[string]*cachedRelay
	inflight singleflight.Group
}

func NewCachingProofAPIClient(
	inner proofapi.ProofApiServiceClient,
	ttl time.Duration,
	maxUses int,
) *CachingProofAPIClient {
	if ttl <= 0 {
		ttl = DefaultProofCacheTTL
	}

	if maxUses <= 0 {
		maxUses = DefaultProofCacheMaxUses
	}

	return &CachingProofAPIClient{
		ProofApiServiceClient: inner,
		ttl:                   ttl,
		maxUses:               maxUses,
		now:                   time.Now,
		entries:               make(map[string]*cachedRelay),
	}
}

func (c *CachingProofAPIClient) RelayByTx(
	ctx context.Context,
	in *proofapi.RelayByTxRequest,
	opts ...grpc.CallOption,
) (*proofapi.RelayByTxResponse, error) {
	key := relayKey(in)

	if response, uses, ok := c.reuse(key); ok {
		lmt.Logger(ctx).Info(
			"reusing a proof already bought for these packets",
			zap.Int("use", uses),
			zap.Int("max_uses", c.maxUses),
		)

		return response, nil
	}

	result, err, _ := c.inflight.Do(key, func() (interface{}, error) {
		if response, _, ok := c.reuse(key); ok {
			return response, nil
		}

		response, err := c.ProofApiServiceClient.RelayByTx(ctx, in, opts...)
		if err != nil {
			return nil, err
		}

		c.store(key, response)

		return response, nil
	})
	if err != nil {
		return nil, err
	}

	return result.(*proofapi.RelayByTxResponse), nil
}

func (c *CachingProofAPIClient) reuse(key string) (*proofapi.RelayByTxResponse, int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.evictExpired()

	entry, ok := c.entries[key]
	if !ok {
		return nil, 0, false
	}

	if entry.uses >= c.maxUses {
		delete(c.entries, key)

		return nil, 0, false
	}

	entry.uses++

	return entry.response, entry.uses, true
}

func (c *CachingProofAPIClient) store(key string, response *proofapi.RelayByTxResponse) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries[key] = &cachedRelay{
		response: response,
		storedAt: c.now(),
		uses:     1,
	}
}

func (c *CachingProofAPIClient) evictExpired() {
	cutoff := c.now().Add(-c.ttl)

	for key, entry := range c.entries {
		if entry.storedAt.Before(cutoff) {
			delete(c.entries, key)
		}
	}
}

func relayKey(in *proofapi.RelayByTxRequest) string {
	txIDs := make([]string, 0, len(in.GetSourceTxIds()))
	for _, id := range in.GetSourceTxIds() {
		txIDs = append(txIDs, hex.EncodeToString(id))
	}
	slices.Sort(txIDs)

	sequences := slices.Clone(in.GetSrcPacketSequences())
	slices.Sort(sequences)

	destinationSequences := slices.Clone(in.GetDstPacketSequences())
	slices.Sort(destinationSequences)

	timeoutIDs := make([]string, 0, len(in.GetTimeoutTxIds()))
	for _, id := range in.GetTimeoutTxIds() {
		timeoutIDs = append(timeoutIDs, hex.EncodeToString(id))
	}
	slices.Sort(timeoutIDs)

	return fmt.Sprintf(
		"%s/%s/%s/%s|tx:%s|seq:%v|dst:%v|timeout:%s",
		in.GetSrcChain(),
		in.GetSrcClientId(),
		in.GetDstChain(),
		in.GetDstClientId(),
		strings.Join(txIDs, ","),
		sequences,
		destinationSequences,
		strings.Join(timeoutIDs, ","),
	)
}
