package ibcv2

import (
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/cosmos/ibc-relayer/shared/contracts/ics26_router"
)

func routerABIForTest(t *testing.T) *abi.ABI {
	t.Helper()

	routerABI, err := ics26_router.ContractMetaData.GetAbi()
	if err != nil {
		t.Fatalf("getting ics26 router abi: %v", err)
	}
	return routerABI
}

func TestCheckRouterSelectorsAllowsPacketAndClientCalls(t *testing.T) {
	routerABI := routerABIForTest(t)

	for _, name := range []string{"recvPacket", "ackPacket", "timeoutPacket", "updateClient"} {
		if err := checkRouterSelectors(routerABI, routerABI.Methods[name].ID, true); err != nil {
			t.Fatalf("%s must be allowed, got %v", name, err)
		}
	}
}

func TestCheckRouterSelectorsRejectsAnUnrecognizedSelector(t *testing.T) {
	routerABI := routerABIForTest(t)

	if err := checkRouterSelectors(routerABI, []byte{0xde, 0xad, 0xbe, 0xef}, true); err == nil {
		t.Fatal("an unrecognized selector must be refused")
	}
}

func TestCheckRouterSelectorsAllowsAMulticallOfAllowedCalls(t *testing.T) {
	routerABI := routerABIForTest(t)

	data, err := routerABI.Pack("multicall", [][]byte{
		routerABI.Methods["recvPacket"].ID,
		routerABI.Methods["updateClient"].ID,
	})
	if err != nil {
		t.Fatalf("packing multicall: %v", err)
	}

	if err := checkRouterSelectors(routerABI, data, true); err != nil {
		t.Fatalf("a multicall of allowed calls must pass, got %v", err)
	}
}

func TestCheckRouterSelectorsRejectsAMulticallWithADisallowedInnerCall(t *testing.T) {
	routerABI := routerABIForTest(t)

	data, err := routerABI.Pack("multicall", [][]byte{
		routerABI.Methods["recvPacket"].ID,
		{0xde, 0xad, 0xbe, 0xef},
	})
	if err != nil {
		t.Fatalf("packing multicall: %v", err)
	}

	if err := checkRouterSelectors(routerABI, data, true); err == nil {
		t.Fatal("a multicall with a disallowed inner call must be refused")
	}
}

func TestCheckRouterSelectorsRejectsANestedMulticall(t *testing.T) {
	routerABI := routerABIForTest(t)

	data, err := routerABI.Pack("multicall", [][]byte{routerABI.Methods["multicall"].ID})
	if err != nil {
		t.Fatalf("packing multicall: %v", err)
	}

	if err := checkRouterSelectors(routerABI, data, true); err == nil {
		t.Fatal("a multicall nested inside a multicall must be refused")
	}
}

func TestCheckRouterCalldataRejectsANonRouterTarget(t *testing.T) {
	routerABI := routerABIForTest(t)

	client := &EVMBridgeClient{routerAddress: common.HexToAddress("0x1111111111111111111111111111111111111111")}
	other := common.HexToAddress("0x2222222222222222222222222222222222222222")

	if err := client.checkRouterCalldata(other, routerABI.Methods["recvPacket"].ID); err == nil {
		t.Fatal("a call to a contract other than the ICS-26 router must be refused")
	}
}
