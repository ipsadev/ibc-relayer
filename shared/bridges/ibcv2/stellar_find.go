package ibcv2

import (
	"context"
	"fmt"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"
)

const (
	stellarAckPacketEvent     = "ack_packet"
	stellarTimeoutPacketEvent = "timeout_packet"
	stellarRecvPacketEvent    = "recv_packet"

	stellarEventLookbackLedgers = 17_280

	stellarEventPageLimit = 10

	stellarEventMaxPages = 64
)

func (c *StellarBridgeClient) FindAckTx(
	ctx context.Context,
	sourceClientID string,
	destClientID string,
	sequence uint64,
) (*BridgeTx, error) {
	return c.findPacketTx(
		ctx, stellarAckPacketEvent, sourceClientID, sequence,
		packetNames("dest_client", destClientID),
	)
}

func (c *StellarBridgeClient) FindRecvTx(
	ctx context.Context,
	sourceClientID string,
	destClientID string,
	sequence uint64,
	timeoutTimestamp time.Time,
) (*BridgeTx, error) {
	return c.findPacketTx(
		ctx, stellarRecvPacketEvent, destClientID, sequence,
		func(packet xdr.ScVal) (bool, error) {
			named, err := packetNames("source_client", sourceClientID)(packet)
			if err != nil || !named {
				return false, err
			}

			timeout, ok := scUint64(packet, "timeout_timestamp")
			if !ok {
				return false, fmt.Errorf("the packet carries no timeout_timestamp")
			}

			return timeout == uint64(timeoutTimestamp.Unix()), nil
		},
	)
}

func (c *StellarBridgeClient) FindTimeoutTx(
	ctx context.Context,
	sourceClientID string,
	destClientID string,
	sequence uint64,
) (*BridgeTx, error) {
	return c.findPacketTx(
		ctx, stellarTimeoutPacketEvent, sourceClientID, sequence,
		packetNames("dest_client", destClientID),
	)
}

type packetMatcher func(packet xdr.ScVal) (bool, error)

func (c *StellarBridgeClient) findPacketTx(
	ctx context.Context,
	eventName string,
	topicClientID string,
	sequence uint64,
	matches packetMatcher,
) (*BridgeTx, error) {
	if _, err := c.routerContract(); err != nil {
		return nil, err
	}

	latest, err := c.rpc.GetLatestLedger(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting the latest ledger: %w", err)
	}

	start := uint32(1)
	if latest.Sequence > stellarEventLookbackLedgers {
		start = latest.Sequence - stellarEventLookbackLedgers
	}

	name := xdr.ScSymbol(eventName)
	topics := protocol.TopicFilter{
		{ScVal: &xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &name}},
		{ScVal: ptr(scvString(topicClientID))},
		{ScVal: ptr(scvU64(sequence))},
	}

	request := protocol.GetEventsRequest{
		StartLedger: start,
		Filters: []protocol.EventFilter{{
			EventType:   protocol.EventTypeSet{protocol.EventTypeContract: nil},
			ContractIDs: []string{c.cfg.RouterContractID},
			Topics:      []protocol.TopicFilter{topics},
		}},
		Pagination: &protocol.PaginationOptions{Limit: stellarEventPageLimit},
	}

	for page := 0; page < stellarEventMaxPages; page++ {
		events, err := c.rpc.GetEvents(ctx, request)
		if err != nil {
			return nil, fmt.Errorf(
				"searching for a %s event on client %s sequence %d: %w",
				eventName, topicClientID, sequence, err,
			)
		}

		found, err := c.firstMatchingTx(ctx, eventName, events.Events, matches)
		if err != nil || found != nil {
			return found, err
		}

		if events.Cursor == "" {
			break
		}

		cursor, err := protocol.ParseCursor(events.Cursor)
		if err != nil {
			return nil, fmt.Errorf("parsing the events cursor %q: %w", events.Cursor, err)
		}

		if cursor.Ledger >= events.LatestLedger {
			break
		}

		request.StartLedger = 0
		request.Pagination = &protocol.PaginationOptions{
			Cursor: &cursor,
			Limit:  stellarEventPageLimit,
		}
	}

	return nil, ErrTxNotFound
}

func (c *StellarBridgeClient) firstMatchingTx(
	ctx context.Context,
	eventName string,
	events []protocol.EventInfo,
	matches packetMatcher,
) (*BridgeTx, error) {
	for i := range events {
		event := events[i]

		matched, err := eventMatches(event, matches)
		if err != nil {
			return nil, fmt.Errorf(
				"decoding a %s event in %s: %w", eventName, event.TransactionHash, err,
			)
		}
		if !matched {
			continue
		}

		closedAt, err := time.Parse(time.RFC3339, event.LedgerClosedAt)
		if err != nil {
			return nil, fmt.Errorf(
				"parsing the close time of %s: %w", event.TransactionHash, err,
			)
		}

		sender, err := c.GetTransactionSender(ctx, event.TransactionHash)
		if err != nil {
			return nil, fmt.Errorf(
				"getting the sender of %s: %w", event.TransactionHash, err,
			)
		}

		return &BridgeTx{
			Hash:           event.TransactionHash,
			Timestamp:      closedAt.UTC(),
			RelayerAddress: sender,
		}, nil
	}

	return nil, nil
}

func packetNames(field string, want string) packetMatcher {
	return func(packet xdr.ScVal) (bool, error) {
		got, ok := scString(packet, field)
		if !ok {
			return false, fmt.Errorf("the packet carries no %s", field)
		}

		return got == want, nil
	}
}

func eventMatches(event protocol.EventInfo, matches packetMatcher) (bool, error) {
	if event.ValueXDR == "" {
		return false, fmt.Errorf("the event carries no value")
	}

	var value xdr.ScVal
	if err := unmarshalBase64(event.ValueXDR, &value); err != nil {
		return false, fmt.Errorf("decoding the event value: %w", err)
	}

	packet, ok := scMapField(value, "packet")
	if !ok {
		return false, fmt.Errorf("the event carries no packet")
	}

	return matches(packet)
}

func ptr(value xdr.ScVal) *xdr.ScVal {
	return &value
}
