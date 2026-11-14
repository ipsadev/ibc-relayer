package ibcv2

import (
	"context"
	"strings"
	"testing"

	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
)

func routerInvocation(contract xdr.ContractId, function string) xdr.HostFunction {
	address := xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &contract}

	return xdr.HostFunction{
		Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
		InvokeContract: &xdr.InvokeContractArgs{
			ContractAddress: address,
			FunctionName:    xdr.ScSymbol(function),
			Args:            []xdr.ScVal{},
		},
	}
}

func testRouter(t *testing.T) xdr.ContractId {
	t.Helper()

	var router xdr.ContractId
	copy(router[:], strings.Repeat("r", 32))
	return router
}

func TestCheckRouterInvocationAllowsTheRouterEntryPoints(t *testing.T) {
	router := testRouter(t)

	for _, function := range []string{
		"update_client", "recv_packet", "acknowledge_packet",
		"timeout_packet", "create_client", "register_counterparty",
	} {
		if err := checkRouterInvocation(routerInvocation(router, function), router, "CROUTER"); err != nil {
			t.Fatalf("%q to the router must be allowed, got %v", function, err)
		}
	}
}

func TestCheckRouterInvocationRejectsAnotherContract(t *testing.T) {
	router := testRouter(t)

	var other xdr.ContractId
	copy(other[:], strings.Repeat("o", 32))

	if err := checkRouterInvocation(routerInvocation(other, "recv_packet"), router, "CROUTER"); err == nil {
		t.Fatal("a call to a contract other than the configured router must be refused")
	}
}

func TestCheckRouterInvocationRejectsAnUnknownFunction(t *testing.T) {
	router := testRouter(t)

	if err := checkRouterInvocation(routerInvocation(router, "transfer"), router, "CROUTER"); err == nil {
		t.Fatal("a router call to a function outside the allow-list must be refused")
	}
}

func TestCheckRouterInvocationRejectsANonInvocation(t *testing.T) {
	router := testRouter(t)

	function := xdr.HostFunction{Type: xdr.HostFunctionTypeHostFunctionTypeUploadContractWasm}

	if err := checkRouterInvocation(function, router, "CROUTER"); err == nil {
		t.Fatal("a non-invocation host function (UploadWasm) must be refused")
	}
}

func TestPrepareAttachesNoAuthEntries(t *testing.T) {
	data := sorobanDataB64(t)
	rpc := &fakeStellarRPC{
		simulates: []protocol.SimulateTransactionResponse{
			{TransactionDataXDR: data, MinResourceFee: 100},
		},
	}

	client := signedStellarClient(t, rpc)

	prepared, err := client.prepare(context.Background(), sampleHostFunction(t))
	if err != nil {
		t.Fatalf("prepare must succeed, got %v", err)
	}

	operation, ok := prepared.Operations()[0].(*txnbuild.InvokeHostFunction)
	if !ok {
		t.Fatal("expected an invoke-host-function operation")
	}

	if len(operation.Auth) != 0 {
		t.Fatalf(
			"prepare attached %d auth entries; attaching the simulated auth would let a "+
				"require_auth(relayer) succeed and a drain settle",
			len(operation.Auth),
		)
	}
}
