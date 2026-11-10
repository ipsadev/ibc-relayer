package ibcv2

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"
)

var errNoSigner = errors.New("the stellar bridge client has no signer")

func (c *StellarBridgeClient) callRouter(
	ctx context.Context,
	function string,
	args []xdr.ScVal,
) (xdr.ScVal, error) {
	contract, err := c.routerContract()
	if err != nil {
		return xdr.ScVal{}, err
	}

	return c.callContract(ctx, contract, function, args)
}

func (c *StellarBridgeClient) callContract(
	ctx context.Context,
	contract xdr.ContractId,
	function string,
	args []xdr.ScVal,
) (xdr.ScVal, error) {
	if c.signer == nil {
		return xdr.ScVal{}, errNoSigner
	}

	address := xdr.ScAddress{
		Type:       xdr.ScAddressTypeScAddressTypeContract,
		ContractId: &contract,
	}

	invocation := xdr.InvokeContractArgs{
		ContractAddress: address,
		FunctionName:    xdr.ScSymbol(function),
		Args:            args,
	}

	source := &txnbuild.SimpleAccount{AccountID: c.signer.Keypair().Address()}

	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        source,
		IncrementSequenceNum: true,
		Operations: []txnbuild.Operation{&txnbuild.InvokeHostFunction{
			HostFunction: xdr.HostFunction{
				Type:           xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
				InvokeContract: &invocation,
			},
		}},
		BaseFee:       stellarBaseFee,
		Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(300)},
	})
	if err != nil {
		return xdr.ScVal{}, fmt.Errorf("building the %s query: %w", function, err)
	}

	envelope, err := tx.Base64()
	if err != nil {
		return xdr.ScVal{}, fmt.Errorf("encoding the %s query: %w", function, err)
	}

	simulated, err := c.rpc.SimulateTransaction(
		ctx,
		protocol.SimulateTransactionRequest{Transaction: envelope},
	)
	if err != nil {
		return xdr.ScVal{}, fmt.Errorf("simulating %s: %w", function, err)
	}
	if simulated.Error != "" {
		return xdr.ScVal{}, fmt.Errorf("the %s simulation failed: %s", function, simulated.Error)
	}
	if len(simulated.Results) == 0 || simulated.Results[0].ReturnValueXDR == nil {
		return xdr.ScVal{}, fmt.Errorf("the %s simulation returned no value", function)
	}

	raw, err := base64.StdEncoding.DecodeString(*simulated.Results[0].ReturnValueXDR)
	if err != nil {
		return xdr.ScVal{}, fmt.Errorf("decoding the %s return value: %w", function, err)
	}

	var value xdr.ScVal
	if err = xdr.SafeUnmarshal(raw, &value); err != nil {
		return xdr.ScVal{}, fmt.Errorf("unmarshalling the %s return value: %w", function, err)
	}

	return value, nil
}

func (c *StellarBridgeClient) IsPacketCommitted(
	ctx context.Context,
	clientID string,
	sequence uint64,
) (bool, error) {
	value, err := c.callRouter(
		ctx,
		"packet_commitment",
		[]xdr.ScVal{scvString(clientID), scvU64(sequence)},
	)
	if err != nil {
		return false, fmt.Errorf(
			"querying the packet commitment for client %s sequence %d: %w",
			clientID, sequence, err,
		)
	}

	return value.Type != xdr.ScValTypeScvVoid, nil
}

func scvString(value string) xdr.ScVal {
	str := xdr.ScString(value)

	return xdr.ScVal{Type: xdr.ScValTypeScvString, Str: &str}
}

func scvU64(value uint64) xdr.ScVal {
	number := xdr.Uint64(value)

	return xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &number}
}

func (c *StellarBridgeClient) IsPacketReceived(
	ctx context.Context,
	clientID string,
	sequence uint64,
) (bool, error) {
	value, err := c.callRouter(
		ctx,
		"has_packet_receipt",
		[]xdr.ScVal{scvString(clientID), scvU64(sequence)},
	)
	if err != nil {
		return false, fmt.Errorf(
			"querying the packet receipt for client %s sequence %d: %w",
			clientID, sequence, err,
		)
	}

	received, ok := value.GetB()
	if !ok {
		return false, fmt.Errorf(
			"has_packet_receipt returned %s for client %s sequence %d, expected a bool",
			value.Type, clientID, sequence,
		)
	}

	return received, nil
}

func (c *StellarBridgeClient) ClientState(
	ctx context.Context,
	clientID string,
) (ClientState, error) {
	contract, err := c.lightClientAddress(ctx, clientID)
	if err != nil {
		return ClientState{}, err
	}

	value, err := c.callContract(ctx, contract, "client_state", []xdr.ScVal{scvString(clientID)})
	if err != nil {
		return ClientState{}, fmt.Errorf("querying the state of client %s: %w", clientID, err)
	}

	encoded, ok := value.GetBytes()
	if !ok {
		return ClientState{}, fmt.Errorf(
			"client_state returned %s for client %s, expected bytes", value.Type, clientID,
		)
	}

	state, err := decodeTendermintClientState(encoded)
	if err != nil {
		return ClientState{}, fmt.Errorf("decoding the state of client %s: %w", clientID, err)
	}

	return ClientState{TendermintClientState: state}, nil
}

func (c *StellarBridgeClient) lightClientAddress(
	ctx context.Context,
	clientID string,
) (xdr.ContractId, error) {
	value, err := c.callRouter(
		ctx,
		"light_client_address",
		[]xdr.ScVal{scvString(clientID)},
	)
	if err != nil {
		return xdr.ContractId{}, fmt.Errorf(
			"querying the light client address of %s: %w", clientID, err,
		)
	}

	address, ok := value.GetAddress()
	if !ok {
		return xdr.ContractId{}, fmt.Errorf(
			"the router has no light client registered for client %s", clientID,
		)
	}
	if address.Type != xdr.ScAddressTypeScAddressTypeContract || address.ContractId == nil {
		return xdr.ContractId{}, fmt.Errorf(
			"the light client of %s is not a contract address", clientID,
		)
	}

	return *address.ContractId, nil
}

func decodeTendermintClientState(encoded []byte) (*TendermintClientState, error) {
	var value xdr.ScVal
	if err := xdr.SafeUnmarshal(encoded, &value); err != nil {
		return nil, fmt.Errorf("the client state is not an ScVal: %w", err)
	}

	trusting, ok := scUint64(value, "trusting_period_secs")
	if !ok {
		return nil, fmt.Errorf("the client state carries no trusting_period_secs")
	}

	height, ok := scMapField(value, "latest_height")
	if !ok {
		return nil, fmt.Errorf("the client state carries no latest_height")
	}
	revisionHeight, ok := scUint64(height, "revision_height")
	if !ok {
		return nil, fmt.Errorf("the latest height carries no revision_height")
	}

	if trusting > uint64(math.MaxInt64)/uint64(time.Second) {
		return nil, fmt.Errorf("a trusting period of %d seconds does not fit a duration", trusting)
	}

	return &TendermintClientState{
		TrustingPeriod: time.Duration(trusting) * time.Second,
		LatestHeight:   revisionHeight,
	}, nil
}
