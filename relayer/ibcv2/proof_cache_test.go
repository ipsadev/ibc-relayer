package ibcv2

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/cosmos/ibc-relayer/proto/gen/proofapi"
)

type countingProofAPI struct {
	proofapi.ProofApiServiceClient

	calls   atomic.Int32
	fail    atomic.Bool
	release chan struct{}
}

func (f *countingProofAPI) RelayByTx(
	_ context.Context,
	in *proofapi.RelayByTxRequest,
	_ ...grpc.CallOption,
) (*proofapi.RelayByTxResponse, error) {
	call := f.calls.Add(1)

	if f.release != nil {
		<-f.release
	}

	if f.fail.Load() {
		return nil, errors.New("the prover network rejected the request")
	}

	return &proofapi.RelayByTxResponse{
		Tx:      []byte{byte(call)},
		Address: in.GetDstChain(),
	}, nil
}

func request(sequences ...uint64) *proofapi.RelayByTxRequest {
	return &proofapi.RelayByTxRequest{
		SrcChain:           "stellar-testnet",
		DstChain:           "11155111",
		SrcClientId:        "sepolia-0",
		DstClientId:        "stellar-testnet-2",
		SourceTxIds:        [][]byte{{0x01, 0x02}},
		SrcPacketSequences: sequences,
	}
}

func TestARetryReusesTheProofAlreadyBought(t *testing.T) {
	inner := &countingProofAPI{}
	cache := NewCachingProofAPIClient(inner, time.Hour, 5)

	first, err := cache.RelayByTx(context.Background(), request(1))
	require.NoError(t, err)

	second, err := cache.RelayByTx(context.Background(), request(1))
	require.NoError(t, err)

	require.Equal(t, int32(1), inner.calls.Load())
	require.Equal(t, first.GetTx(), second.GetTx())
}

func TestAProofIsReplacedAfterItsMaximumUses(t *testing.T) {
	inner := &countingProofAPI{}
	cache := NewCachingProofAPIClient(inner, time.Hour, 3)

	for range 3 {
		_, err := cache.RelayByTx(context.Background(), request(1))
		require.NoError(t, err)
	}
	require.Equal(t, int32(1), inner.calls.Load())

	_, err := cache.RelayByTx(context.Background(), request(1))
	require.NoError(t, err)
	require.Equal(t, int32(2), inner.calls.Load())
}

func TestAProofIsReplacedAfterItsTimeToLive(t *testing.T) {
	inner := &countingProofAPI{}
	cache := NewCachingProofAPIClient(inner, time.Minute, 5)
	now := time.Now()
	cache.now = func() time.Time { return now }

	_, err := cache.RelayByTx(context.Background(), request(1))
	require.NoError(t, err)

	now = now.Add(2 * time.Minute)

	_, err = cache.RelayByTx(context.Background(), request(1))
	require.NoError(t, err)
	require.Equal(t, int32(2), inner.calls.Load())
}

func TestAFailedRequestIsNotCached(t *testing.T) {
	inner := &countingProofAPI{}
	cache := NewCachingProofAPIClient(inner, time.Hour, 5)

	inner.fail.Store(true)
	_, err := cache.RelayByTx(context.Background(), request(1))
	require.Error(t, err)

	inner.fail.Store(false)
	_, err = cache.RelayByTx(context.Background(), request(1))
	require.NoError(t, err)
	require.Equal(t, int32(2), inner.calls.Load())
}

func TestDifferentPacketsGetDifferentProofs(t *testing.T) {
	inner := &countingProofAPI{}
	cache := NewCachingProofAPIClient(inner, time.Hour, 5)

	first, err := cache.RelayByTx(context.Background(), request(1))
	require.NoError(t, err)

	second, err := cache.RelayByTx(context.Background(), request(2))
	require.NoError(t, err)

	require.Equal(t, int32(2), inner.calls.Load())
	require.NotEqual(t, first.GetTx(), second.GetTx())
}

func TestTheOrderOfPacketsInABatchDoesNotMatter(t *testing.T) {
	inner := &countingProofAPI{}
	cache := NewCachingProofAPIClient(inner, time.Hour, 5)

	_, err := cache.RelayByTx(context.Background(), request(1, 2))
	require.NoError(t, err)

	_, err = cache.RelayByTx(context.Background(), request(2, 1))
	require.NoError(t, err)

	require.Equal(t, int32(1), inner.calls.Load())
}

func TestConcurrentIdenticalRequestsBuyOneProof(t *testing.T) {
	inner := &countingProofAPI{release: make(chan struct{})}
	cache := NewCachingProofAPIClient(inner, time.Hour, 10)

	var wg sync.WaitGroup
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := cache.RelayByTx(context.Background(), request(1))
			require.NoError(t, err)
		}()
	}

	time.Sleep(50 * time.Millisecond)
	close(inner.release)
	wg.Wait()

	require.Equal(t, int32(1), inner.calls.Load())
}
