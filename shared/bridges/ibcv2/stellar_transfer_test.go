package ibcv2

import (
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
)

func scSymVal(symbol string) xdr.ScVal {
	value := xdr.ScSymbol(symbol)

	return xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &value}
}

func scStrVal(text string) xdr.ScVal {
	value := xdr.ScString(text)

	return xdr.ScVal{Type: xdr.ScValTypeScvString, Str: &value}
}

func scMapVal(entries ...xdr.ScMapEntry) xdr.ScVal {
	entryMap := xdr.ScMap(entries)
	pointer := &entryMap

	return xdr.ScVal{Type: xdr.ScValTypeScvMap, Map: &pointer}
}

func scEntry(key string, val xdr.ScVal) xdr.ScMapEntry {
	return xdr.ScMapEntry{Key: scSymVal(key), Val: val}
}

func TestDecodeStellarTransfer(t *testing.T) {
	jsonValue := []byte(`{"denom":"native","amount":"7000000","sender":"GABC","receiver":"cosmos1xyz"}`)

	payload := scMapVal(
		scEntry("source_port", scStrVal("transfer")),
		scEntry("encoding", scStrVal("application/json")),
		scEntry("value", scBytesVal(jsonValue)),
	)

	packet := scMapVal(scEntry("payloads", scVecVal(payload)))

	transfer := decodeStellarTransfer(packet)

	assert.NotNil(t, transfer)
	assert.Equal(t, ApplicationICS20, transfer.Application)
	assert.Equal(t, "7000000", transfer.Amount)
	assert.Equal(t, "native", transfer.Denom)
	assert.Equal(t, "GABC", transfer.Sender)
	assert.Equal(t, "cosmos1xyz", transfer.Receiver)
}

func TestDecodeStellarTransferNoPayloads(t *testing.T) {
	packet := scMapVal()

	assert.Nil(t, decodeStellarTransfer(packet))
}
