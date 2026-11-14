package ibcv2_test

import (
	"context"
	"math/big"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/cosmos/ibc-relayer/db/gen/db"
	mockibcv2 "github.com/cosmos/ibc-relayer/mocks/relayer/ibcv2"
	mockbridge "github.com/cosmos/ibc-relayer/mocks/shared/bridges/ibcv2"
	"github.com/cosmos/ibc-relayer/relayer/ibcv2"
)

type recvFeeStorageStub struct {
	fee *db.UpdateTransferDestinationFeeParams
	usd *db.UpdateTransferRecvTxGasCostUSDParams
}

func (s *recvFeeStorageStub) UpdateTransferDestinationFee(_ context.Context, arg db.UpdateTransferDestinationFeeParams) error {
	s.fee = &arg
	return nil
}

func (s *recvFeeStorageStub) UpdateTransferRecvTxGasCostUSD(_ context.Context, arg db.UpdateTransferRecvTxGasCostUSDParams) error {
	s.usd = &arg
	return nil
}

func TestRecvTxGasCalculatorRecordsDestinationFeeWithoutPriceClient(t *testing.T) {
	ctx := context.Background()

	transfer := ibcv2.NewIBCV2Transfer(ctx, db.Ibcv2Transfer{
		SourceChainID:        "stellar-testnet",
		DestinationChainID:   "provider",
		PacketSourceClientID: "sepolia-0",
		PacketSequenceNumber: 1,
		RecvTxHash:           pgtype.Text{String: "RECVHASH", Valid: true},
	})

	client := mockbridge.NewMockBridgeClient(t)
	client.EXPECT().TxFee(mock.Anything, "RECVHASH").Return(big.NewInt(4200), nil)

	manager := mockibcv2.NewMockBridgeClientManager(t)
	manager.EXPECT().GetClient(mock.Anything, "provider").Return(client, nil)

	storage := &recvFeeStorageStub{}

	processor := ibcv2.NewRecvTxGasCalculatorProcessor(manager, storage, nil, "", 0, "ATOM")

	require.True(t, processor.ShouldProcess(transfer))

	out, err := processor.Process(ctx, transfer)
	require.NoError(t, err)

	require.NotNil(t, storage.fee)
	require.Nil(t, storage.usd)
	require.Equal(t, "ATOM", storage.fee.FeeDestinationDenom.String)
	require.Equal(t, int64(4200), storage.fee.FeeDestination.Int.Int64())

	require.False(t, processor.ShouldProcess(out))
}
