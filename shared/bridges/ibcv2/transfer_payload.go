package ibcv2

import (
	"encoding/json"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
)

const (
	transferPortID = "transfer"
	gmpPortID      = "gmpport"

	ApplicationICS20 = "ics20"
	ApplicationICS27 = "ics27"
)

type TransferPayload struct {
	Application string
	Denom       string
	Amount      string
	Sender      string
	Receiver    string
	Decimals    *uint8
	FastFill    bool
}

func applicationForPort(port string) string {
	switch port {
	case transferPortID:
		return ApplicationICS20
	case gmpPortID:
		return ApplicationICS27
	default:
		return ""
	}
}

var ics20ABIArguments = buildICS20ABIArguments()

func buildICS20ABIArguments() abi.Arguments {
	stringType, _ := abi.NewType("string", "", nil)
	uintType, _ := abi.NewType("uint256", "", nil)

	return abi.Arguments{
		{Type: stringType},
		{Type: stringType},
		{Type: stringType},
		{Type: uintType},
		{Type: stringType},
	}
}

const abiWordBytes = 32

// unpackMaybeWrapped decodes ABI arguments that may be either flat or wrapped in
// an outer offset word, as abi.encode of a single struct produces. It tries the
// value as-is, then with the leading offset word stripped.
func unpackMaybeWrapped(arguments abi.Arguments, value []byte) ([]any, bool) {
	if values, err := arguments.Unpack(value); err == nil {
		return values, true
	}

	if len(value) > abiWordBytes {
		if values, err := arguments.Unpack(value[abiWordBytes:]); err == nil {
			return values, true
		}
	}

	return nil, false
}

func decodeICS20ABI(value []byte) (*TransferPayload, string, bool) {
	values, ok := unpackMaybeWrapped(ics20ABIArguments, value)
	if !ok || len(values) < 4 {
		return nil, "", false
	}

	denom, _ := values[0].(string)
	sender, _ := values[1].(string)
	receiver, _ := values[2].(string)
	amount, _ := values[3].(*big.Int)

	if amount == nil {
		return nil, "", false
	}

	memo := ""
	if len(values) >= 5 {
		memo, _ = values[4].(string)
	}

	return &TransferPayload{
		Denom:    denom,
		Sender:   sender,
		Receiver: receiver,
		Amount:   amount.String(),
	}, memo, true
}

type ics20JSON struct {
	Denom    string `json:"denom"`
	Amount   string `json:"amount"`
	Sender   string `json:"sender"`
	Receiver string `json:"receiver"`
	Memo     string `json:"memo"`
}

func decodeICS20JSON(value []byte) (*TransferPayload, string, bool) {
	var data ics20JSON

	if err := json.Unmarshal(value, &data); err != nil || data.Amount == "" {
		return nil, "", false
	}

	return &TransferPayload{
		Denom:    data.Denom,
		Amount:   data.Amount,
		Sender:   data.Sender,
		Receiver: data.Receiver,
	}, data.Memo, true
}

type fastFillMemo struct {
	FastFill string `json:"fast_fill"`
}

func applyFastFillTerms(payload *TransferPayload, memo string) {
	if memo == "" {
		return
	}

	var parsed fastFillMemo
	if err := json.Unmarshal([]byte(memo), &parsed); err != nil || parsed.FastFill == "" {
		return
	}

	payload.FastFill = true

	recipient := parsed.FastFill
	if index := strings.IndexByte(recipient, ':'); index >= 0 {
		recipient = recipient[:index]
	}
	if recipient != "" {
		payload.Receiver = recipient
	}
}

var gmpABIArguments = buildGMPABIArguments()

func buildGMPABIArguments() abi.Arguments {
	stringType, _ := abi.NewType("string", "", nil)
	bytesType, _ := abi.NewType("bytes", "", nil)

	return abi.Arguments{
		{Type: stringType},
		{Type: stringType},
		{Type: bytesType},
		{Type: bytesType},
		{Type: stringType},
	}
}

// decodeGMP reads the ics27 packet (sender, receiver, salt, payload, memo). The
// sender is the calling account and the receiver is the called account.
func decodeGMP(value []byte) (sender string, receiver string, ok bool) {
	values, ok := unpackMaybeWrapped(gmpABIArguments, value)
	if !ok || len(values) < 2 {
		return "", "", false
	}

	sender, _ = values[0].(string)
	receiver, _ = values[1].(string)

	return sender, receiver, true
}

func DecodeTransferPayload(port, encoding string, value []byte) *TransferPayload {
	application := applicationForPort(port)

	switch application {
	case ApplicationICS20:
		decoders := []func([]byte) (*TransferPayload, string, bool){decodeICS20ABI, decodeICS20JSON}
		if strings.Contains(strings.ToLower(encoding), "json") {
			decoders = []func([]byte) (*TransferPayload, string, bool){decodeICS20JSON, decodeICS20ABI}
		}

		for _, decode := range decoders {
			if payload, memo, ok := decode(value); ok {
				payload.Application = application
				applyFastFillTerms(payload, memo)
				return payload
			}
		}

		return &TransferPayload{Application: application}
	case ApplicationICS27:
		payload := &TransferPayload{Application: application}
		if sender, receiver, ok := decodeGMP(value); ok {
			payload.Sender = sender
			payload.Receiver = receiver
		}

		return payload
	default:
		return &TransferPayload{Application: application}
	}
}

func EVMTransferPayload(port string, value []byte) *TransferPayload {
	return DecodeTransferPayload(port, "", value)
}

func JSONTransferPayload(port string, value []byte) *TransferPayload {
	return DecodeTransferPayload(port, "json", value)
}
