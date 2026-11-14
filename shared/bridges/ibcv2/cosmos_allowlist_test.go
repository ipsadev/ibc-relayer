package ibcv2

import (
	"testing"

	sdk "github.com/cosmos/cosmos-sdk/types"

	ibcchannelv2types "github.com/cosmos/ibc-relayer/proto/gen/ibc/core/channel/v2"
	ibcclientv1types "github.com/cosmos/ibc-relayer/proto/gen/ibc/core/client/v1"
)

const (
	testRelayerAddress = "cosmos1relayer"
	testForeignAddress = "cosmos1attacker"
)

func TestCheckRelayerMessagesAllowsProofMessagesFromTheRelayer(t *testing.T) {
	msgs := []sdk.Msg{
		&ibcclientv1types.MsgUpdateClient{Signer: testRelayerAddress},
		&ibcchannelv2types.MsgRecvPacket{Signer: testRelayerAddress},
		&ibcchannelv2types.MsgAcknowledgement{Signer: testRelayerAddress},
		&ibcchannelv2types.MsgTimeout{Signer: testRelayerAddress},
	}

	if err := checkRelayerMessages(testRelayerAddress, msgs...); err != nil {
		t.Fatalf("the relayer's own proof messages must be allowed, got %v", err)
	}
}

func TestCheckRelayerMessagesRejectsMsgSendPacket(t *testing.T) {
	if err := checkRelayerMessages(testRelayerAddress, &ibcchannelv2types.MsgSendPacket{}); err == nil {
		t.Fatal("a MsgSendPacket must be refused: signing one would move the relayer's own funds")
	}
}

func TestCheckRelayerMessagesRejectsAForeignSigner(t *testing.T) {
	msg := &ibcchannelv2types.MsgRecvPacket{Signer: testForeignAddress}

	if err := checkRelayerMessages(testRelayerAddress, msg); err == nil {
		t.Fatal("a proof message whose signer is not the relayer must be refused")
	}
}
