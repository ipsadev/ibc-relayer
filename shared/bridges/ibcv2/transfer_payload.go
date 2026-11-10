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

func decodeICS20ABI(value []byte) (*TransferPayload, bool) {
	values, err := ics20ABIArguments.Unpack(value)
	if err != nil || len(values) < 4 {
		return nil, false
	}

	denom, _ := values[0].(string)
	sender, _ := values[1].(string)
	receiver, _ := values[2].(string)
	amount, _ := values[3].(*big.Int)

	if amount == nil {
		return nil, false
	}

	return &TransferPayload{
		Denom:    denom,
		Sender:   sender,
		Receiver: receiver,
		Amount:   amount.String(),
	}, true
}

type ics20JSON struct {
	Denom    string `json:"denom"`
	Amount   string `json:"amount"`
	Sender   string `json:"sender"`
	Receiver string `json:"receiver"`
}

func decodeICS20JSON(value []byte) (*TransferPayload, bool) {
	var data ics20JSON

	if err := json.Unmarshal(value, &data); err != nil || data.Amount == "" {
		return nil, false
	}

	return &TransferPayload{
		Denom:    data.Denom,
		Amount:   data.Amount,
		Sender:   data.Sender,
		Receiver: data.Receiver,
	}, true
}

func DecodeTransferPayload(port, encoding string, value []byte) *TransferPayload {
	application := applicationForPort(port)

	if application != ApplicationICS20 {
		return &TransferPayload{Application: application}
	}

	decoders := []func([]byte) (*TransferPayload, bool){decodeICS20ABI, decodeICS20JSON}
	if strings.Contains(strings.ToLower(encoding), "json") {
		decoders = []func([]byte) (*TransferPayload, bool){decodeICS20JSON, decodeICS20ABI}
	}

	for _, decode := range decoders {
		if payload, ok := decode(value); ok {
			payload.Application = application
			return payload
		}
	}

	return &TransferPayload{Application: application}
}

func EVMTransferPayload(port string, value []byte) *TransferPayload {
	return DecodeTransferPayload(port, "", value)
}

func JSONTransferPayload(port string, value []byte) *TransferPayload {
	return DecodeTransferPayload(port, "json", value)
}
