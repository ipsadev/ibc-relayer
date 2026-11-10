package ibcv2

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEVMTransferPayloadDecodesICS20(t *testing.T) {
	amount := new(big.Int)
	amount.SetString("3420000000000000", 10)

	value, err := ics20ABIArguments.Pack(
		"0xfff9976782d46cc05630d1f6ebab18b2324d6b14",
		"0x06ecf411be934d0ecb80a247508ca96c8030808e",
		"GDKP7AGPKAVJUL6AGGJDXOVLWIOVMZ2YDDKGCY4YSRHHOV6BMJMLWOEC",
		amount,
		"",
	)
	require.NoError(t, err)

	payload := EVMTransferPayload("transfer", value)

	assert.Equal(t, ApplicationICS20, payload.Application)
	assert.Equal(t, "3420000000000000", payload.Amount)
	assert.Equal(t, "0xfff9976782d46cc05630d1f6ebab18b2324d6b14", payload.Denom)
	assert.Equal(t, "0x06ecf411be934d0ecb80a247508ca96c8030808e", payload.Sender)
	assert.Equal(t, "GDKP7AGPKAVJUL6AGGJDXOVLWIOVMZ2YDDKGCY4YSRHHOV6BMJMLWOEC", payload.Receiver)
}

func TestEVMTransferPayloadGMPHasNoAmount(t *testing.T) {
	payload := EVMTransferPayload("gmpport", []byte("whatever"))

	assert.Equal(t, ApplicationICS27, payload.Application)
	assert.Empty(t, payload.Amount)
	assert.Empty(t, payload.Denom)
}

func TestEVMTransferPayloadUnknownPort(t *testing.T) {
	payload := EVMTransferPayload("other", nil)

	assert.Empty(t, payload.Application)
	assert.Empty(t, payload.Amount)
}

func TestJSONTransferPayloadDecodesICS20(t *testing.T) {
	value := []byte(`{"denom":"uatom","amount":"1500000","sender":"cosmos1abc","receiver":"GDKP7AGP"}`)

	payload := JSONTransferPayload("transfer", value)

	assert.Equal(t, ApplicationICS20, payload.Application)
	assert.Equal(t, "1500000", payload.Amount)
	assert.Equal(t, "uatom", payload.Denom)
	assert.Equal(t, "cosmos1abc", payload.Sender)
	assert.Equal(t, "GDKP7AGP", payload.Receiver)
}

func TestEVMTransferPayloadFastFillUsesMemoRecipient(t *testing.T) {
	amount := new(big.Int)
	amount.SetString("3420000000000000", 10)

	memo := `{"dest_callback":{"address":"CBM35FY3LHHGBCGJQU6AX3ZA5MS5HLVHXOIVRFN4EK3OFNPASDAFVFTQ"},"fast_fill":"GDKP7AGPKAVJUL6AGGJDXOVLWIOVMZ2YDDKGCY4YSRHHOV6BMJMLWOEC:3420000000000:1794091668"}`

	value, err := ics20ABIArguments.Pack(
		"0xfff9976782d46cc05630d1f6ebab18b2324d6b14",
		"0x6b1a9014faf309aa31af42d42931f4da3f72a408",
		"CBM35FY3LHHGBCGJQU6AX3ZA5MS5HLVHXOIVRFN4EK3OFNPASDAFVFTQ",
		amount,
		memo,
	)
	require.NoError(t, err)

	payload := DecodeTransferPayload("transfer", "", value)

	assert.True(t, payload.FastFill)
	assert.Equal(t, "GDKP7AGPKAVJUL6AGGJDXOVLWIOVMZ2YDDKGCY4YSRHHOV6BMJMLWOEC", payload.Receiver)
	assert.Equal(t, "3420000000000000", payload.Amount)
}

func TestEVMTransferPayloadStandardHasNoFastFill(t *testing.T) {
	amount := new(big.Int)
	amount.SetString("1000", 10)

	value, err := ics20ABIArguments.Pack("denom", "sender", "GRECEIVER", amount, "")
	require.NoError(t, err)

	payload := DecodeTransferPayload("transfer", "", value)

	assert.False(t, payload.FastFill)
	assert.Equal(t, "GRECEIVER", payload.Receiver)
}

func abiOffsetWord() []byte {
	word := make([]byte, 32)
	word[31] = 0x20
	return word
}

func TestDecodeICS20WrappedTuple(t *testing.T) {
	amount := new(big.Int)
	amount.SetString("3420000000000000", 10)

	flat, err := ics20ABIArguments.Pack(
		"0xfff9976782d46cc05630d1f6ebab18b2324d6b14",
		"0x06ecf411be934d0ecb80a247508ca96c8030808e",
		"GDKP7AGPKAVJUL6AGGJDXOVLWIOVMZ2YDDKGCY4YSRHHOV6BMJMLWOEC",
		amount,
		"",
	)
	require.NoError(t, err)

	wrapped := append(abiOffsetWord(), flat...)

	payload := DecodeTransferPayload("transfer", "", wrapped)

	assert.Equal(t, ApplicationICS20, payload.Application)
	assert.Equal(t, "3420000000000000", payload.Amount)
	assert.Equal(t, "GDKP7AGPKAVJUL6AGGJDXOVLWIOVMZ2YDDKGCY4YSRHHOV6BMJMLWOEC", payload.Receiver)
}

func TestDecodeGMPUsesCallerAndCallee(t *testing.T) {
	flat, err := gmpABIArguments.Pack(
		"0x6b1a9014faf309aa31af42d42931f4da3f72a408",
		"CBM35FY3LHHGBCGJQU6AX3ZA5MS5HLVHXOIVRFN4EK3OFNPASDAFVFTQ",
		[]byte{0x01},
		[]byte{0x02},
		"",
	)
	require.NoError(t, err)

	wrapped := append(abiOffsetWord(), flat...)

	payload := DecodeTransferPayload("gmpport", "", wrapped)

	assert.Equal(t, ApplicationICS27, payload.Application)
	assert.Equal(t, "0x6b1a9014faf309aa31af42d42931f4da3f72a408", payload.Sender)
	assert.Equal(t, "CBM35FY3LHHGBCGJQU6AX3ZA5MS5HLVHXOIVRFN4EK3OFNPASDAFVFTQ", payload.Receiver)
}
