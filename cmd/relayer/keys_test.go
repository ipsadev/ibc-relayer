package main

import "testing"

func TestKeysParseFromTheKeysFileJSON(t *testing.T) {
	keys, err := ParseChainIDToPrivateKeyMap([]byte(`{
		"stellar-testnet": {"private_key": "SAAA"},
		"11155111": {"name": "Sepolia", "address": "0xBbA4", "private_key": "0xabc"}
	}`))
	if err != nil {
		t.Fatalf("parsing keys: %v", err)
	}

	if keys["stellar-testnet"] != "SAAA" || keys["11155111"] != "0xabc" || len(keys) != 2 {
		t.Fatalf("unexpected keys: %v", keys)
	}
}

func TestMalformedKeysAreAnError(t *testing.T) {
	if _, err := ParseChainIDToPrivateKeyMap([]byte(`{"stellar-testnet": "SAAA"`)); err == nil {
		t.Fatal("malformed keys JSON must be an error")
	}
}
