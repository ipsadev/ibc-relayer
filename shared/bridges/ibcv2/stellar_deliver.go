package ibcv2

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	corepb "github.com/stellar/go-stellar-sdk/protocols/stellarcore"
)

const stellarBaseFee = txnbuild.MinBaseFee

// decodeHostFunctions reads the length-prefixed XDR array the proof API returns
// for Stellar: a big-endian u32 count followed by that many HostFunction
// values, to be submitted in order.
func decodeHostFunctions(raw []byte) ([]xdr.HostFunction, error) {
	if len(raw) < 4 {
		return nil, fmt.Errorf("host function array is %d bytes, too short for a count", len(raw))
	}

	count := binary.BigEndian.Uint32(raw[:4])
	reader := bytes.NewReader(raw[4:])

	functions := make([]xdr.HostFunction, 0, count)
	for i := uint32(0); i < count; i++ {
		var function xdr.HostFunction
		if _, err := xdr.Unmarshal(reader, &function); err != nil {
			return nil, fmt.Errorf("decoding host function %d of %d: %w", i, count, err)
		}
		functions = append(functions, function)
	}

	if reader.Len() != 0 {
		return nil, fmt.Errorf(
			"host function array has %d trailing bytes after %d entries",
			reader.Len(), count,
		)
	}

	return functions, nil
}

// DeliverTx submits each host function the proof API produced as its own
// Soroban transaction, in order, stopping at the first failure. Soroban allows
// one host-function invocation per transaction, so a batch cannot be atomic;
// the reported BridgeTx is the last one that landed.
const (
	inclusionPoll    = 2 * time.Second
	inclusionTimeout = 60 * time.Second
)

func (c *StellarBridgeClient) DeliverTx(ctx context.Context, raw []byte, _ string) (*BridgeTx, error) {
	if c.signer == nil {
		return nil, fmt.Errorf("stellar chain %s has no signer configured", c.chainID)
	}

	functions, err := decodeHostFunctions(raw)
	if err != nil {
		return nil, err
	}
	if len(functions) == 0 {
		return nil, fmt.Errorf("nothing to submit: the host function array is empty")
	}

	c.submissionLock.Lock()
	defer c.submissionLock.Unlock()

	var last *BridgeTx
	for i, function := range functions {
		last, err = c.submitHostFunction(ctx, function)
		if err != nil {
			return nil, fmt.Errorf("submitting host function %d of %d: %w", i+1, len(functions), err)
		}

		// A later function reads state an earlier one writes: recv_packet
		// verifies against the consensus state update_client stores. Soroban
		// takes one host function per transaction, so the next simulation must
		// run against a ledger that already contains the previous one.
		if i+1 < len(functions) {
			if err := c.awaitInclusion(ctx, last.Hash); err != nil {
				return nil, fmt.Errorf(
					"waiting for host function %d of %d (%s): %w",
					i+1, len(functions), last.Hash, err,
				)
			}
		}
	}

	return last, nil
}

func (c *StellarBridgeClient) awaitInclusion(ctx context.Context, hash string) error {
	deadline := time.Now().Add(inclusionTimeout)

	for {
		tx, err := c.rpc.GetTransaction(ctx, protocol.GetTransactionRequest{Hash: hash})
		if err == nil {
			switch tx.Status {
			case protocol.TransactionStatusSuccess:
				return nil
			case protocol.TransactionStatusFailed:
				return fmt.Errorf("it failed on chain: %s", tx.ResultXDR)
			}
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("it was not included within %s", inclusionTimeout)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(inclusionPoll):
		}
	}
}

func (c *StellarBridgeClient) submitHostFunction(
	ctx context.Context,
	function xdr.HostFunction,
) (*BridgeTx, error) {
	kp := c.signer.Keypair()

	if err := c.restoreArchivedEntries(ctx, function); err != nil {
		return nil, fmt.Errorf("restoring archived footprint: %w", err)
	}

	account, err := c.rpc.LoadAccount(ctx, kp.Address())
	if err != nil {
		return nil, fmt.Errorf("loading account %s: %w", kp.Address(), err)
	}

	params := txnbuild.TransactionParams{
		SourceAccount:        account,
		IncrementSequenceNum: true,
		Operations:           []txnbuild.Operation{&txnbuild.InvokeHostFunction{HostFunction: function}},
		BaseFee:              stellarBaseFee,
		Preconditions:        txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(300)},
	}

	tx, err := txnbuild.NewTransaction(params)
	if err != nil {
		return nil, fmt.Errorf("building the transaction: %w", err)
	}

	prepared, err := c.prepare(ctx, tx, params)
	if err != nil {
		return nil, err
	}

	signed, err := prepared.Sign(c.cfg.NetworkPassphrase, kp)
	if err != nil {
		return nil, fmt.Errorf("signing the transaction: %w", err)
	}

	envelope, err := signed.Base64()
	if err != nil {
		return nil, fmt.Errorf("encoding the transaction: %w", err)
	}

	sent, err := c.rpc.SendTransaction(ctx, protocol.SendTransactionRequest{Transaction: envelope})
	if err != nil {
		return nil, fmt.Errorf("submitting the transaction: %w", err)
	}
	if sent.Status == corepb.TXStatusError {
		return nil, fmt.Errorf("the transaction was rejected: %s", sent.ErrorResultXDR)
	}

	return &BridgeTx{
		Hash:           sent.Hash,
		Timestamp:      time.Now().UTC(),
		RelayerAddress: kp.Address(),
	}, nil
}

// restoreArchivedEntries simulates the host function and, if the simulation
// reports a restore preamble (archived ledger entries the invocation needs),
// submits a RestoreFootprint transaction and waits for it to land. When there
// is nothing to restore it is a no-op and the normal prepare step proceeds.
func (c *StellarBridgeClient) restoreArchivedEntries(ctx context.Context, function xdr.HostFunction) error {
	kp := c.signer.Keypair()

	account, err := c.rpc.LoadAccount(ctx, kp.Address())
	if err != nil {
		return fmt.Errorf("loading account %s: %w", kp.Address(), err)
	}

	probe, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        account,
		IncrementSequenceNum: true,
		Operations:           []txnbuild.Operation{&txnbuild.InvokeHostFunction{HostFunction: function}},
		BaseFee:              stellarBaseFee,
		Preconditions:        txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(300)},
	})
	if err != nil {
		return fmt.Errorf("building the restore probe: %w", err)
	}

	envelope, err := probe.Base64()
	if err != nil {
		return fmt.Errorf("encoding the restore probe: %w", err)
	}

	simulated, err := c.rpc.SimulateTransaction(ctx, protocol.SimulateTransactionRequest{Transaction: envelope})
	if err != nil {
		return fmt.Errorf("simulating the restore probe: %w", err)
	}

	// Let prepare surface a genuine simulation error; only act on a restore
	// preamble here.
	if simulated.Error != "" || simulated.RestorePreamble == nil || simulated.RestorePreamble.TransactionDataXDR == "" {
		return nil
	}

	raw, err := base64.StdEncoding.DecodeString(simulated.RestorePreamble.TransactionDataXDR)
	if err != nil {
		return fmt.Errorf("decoding the restore preamble data: %w", err)
	}

	var data xdr.SorobanTransactionData
	if err = xdr.SafeUnmarshal(raw, &data); err != nil {
		return fmt.Errorf("decoding the restore preamble soroban data: %w", err)
	}

	restoreAccount, err := c.rpc.LoadAccount(ctx, kp.Address())
	if err != nil {
		return fmt.Errorf("loading account %s for restore: %w", kp.Address(), err)
	}

	restore, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        restoreAccount,
		IncrementSequenceNum: true,
		Operations:           []txnbuild.Operation{&txnbuild.RestoreFootprint{Ext: xdr.TransactionExt{V: 1, SorobanData: &data}}},
		BaseFee:              stellarBaseFee + simulated.RestorePreamble.MinResourceFee,
		Preconditions:        txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(300)},
	})
	if err != nil {
		return fmt.Errorf("building the restore transaction: %w", err)
	}

	signed, err := restore.Sign(c.cfg.NetworkPassphrase, kp)
	if err != nil {
		return fmt.Errorf("signing the restore transaction: %w", err)
	}

	restoreEnvelope, err := signed.Base64()
	if err != nil {
		return fmt.Errorf("encoding the restore transaction: %w", err)
	}

	sent, err := c.rpc.SendTransaction(ctx, protocol.SendTransactionRequest{Transaction: restoreEnvelope})
	if err != nil {
		return fmt.Errorf("submitting the restore transaction: %w", err)
	}
	if sent.Status == corepb.TXStatusError {
		return fmt.Errorf("the restore transaction was rejected: %s", sent.ErrorResultXDR)
	}

	return c.awaitInclusion(ctx, sent.Hash)
}

// prepare runs the simulation Soroban requires and folds the resulting
// footprint and resource fee back into the transaction. Submitting without it
// fails for insufficient resources rather than for anything meaningful.
func (c *StellarBridgeClient) prepare(
	ctx context.Context,
	tx *txnbuild.Transaction,
	params txnbuild.TransactionParams,
) (*txnbuild.Transaction, error) {
	envelope, err := tx.Base64()
	if err != nil {
		return nil, fmt.Errorf("encoding the transaction for simulation: %w", err)
	}

	simulated, err := c.rpc.SimulateTransaction(
		ctx,
		protocol.SimulateTransactionRequest{Transaction: envelope},
	)
	if err != nil {
		return nil, fmt.Errorf("simulating the transaction: %w", err)
	}
	if simulated.Error != "" {
		return nil, fmt.Errorf("the simulation failed: %s", simulated.Error)
	}
	if simulated.TransactionDataXDR == "" {
		return nil, fmt.Errorf("the simulation returned no transaction data")
	}

	raw, err := base64.StdEncoding.DecodeString(simulated.TransactionDataXDR)
	if err != nil {
		return nil, fmt.Errorf("decoding the simulated transaction data: %w", err)
	}

	var data xdr.SorobanTransactionData
	if err = xdr.SafeUnmarshal(raw, &data); err != nil {
		return nil, fmt.Errorf("decoding the simulated soroban data: %w", err)
	}

	operation, ok := params.Operations[0].(*txnbuild.InvokeHostFunction)
	if !ok {
		return nil, fmt.Errorf("expected a host function operation")
	}
	operation.Ext = xdr.TransactionExt{V: 1, SorobanData: &data}

	params.BaseFee = stellarBaseFee + simulated.MinResourceFee
	params.SourceAccount = accountAtSequence(params.SourceAccount, tx.SourceAccount().Sequence-1)

	prepared, err := txnbuild.NewTransaction(params)
	if err != nil {
		return nil, fmt.Errorf("rebuilding the simulated transaction: %w", err)
	}

	return prepared, nil
}

func accountAtSequence(account txnbuild.Account, sequence int64) txnbuild.Account {
	return &txnbuild.SimpleAccount{
		AccountID: account.GetAccountID(),
		Sequence:  sequence,
	}
}
