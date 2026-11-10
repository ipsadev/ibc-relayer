package ibcv2

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// ErrUnpinnedQuorumSet is returned when a quorum set served by the gateway does
// not hash to a fingerprint this relayer pins.
var ErrUnpinnedQuorumSet = errors.New("quorum set is not pinned for this network")

// VerifyQuorumFingerprints checks every quorum set against the pinned
// fingerprints before it can become a client's trust root.
//
// Quorum sets reach the relayer over the gateway, which is untrusted transport.
// A compromised or merely misconfigured gateway could otherwise seed a light
// client with a validator set of its choosing, and every subsequent proof would
// verify happily against it. Pinning the sha256 makes that substitution a
// startup failure instead of a silent compromise.
func VerifyQuorumFingerprints(quorumSets [][]byte, pinned []string) error {
	if len(quorumSets) == 0 {
		return errors.New("no quorum sets to verify")
	}
	if len(pinned) == 0 {
		return errors.New(
			"no pinned fingerprints to check against; refusing to trust an unverified validator set",
		)
	}

	allowed := make(map[string]struct{}, len(pinned))
	for i, entry := range pinned {
		normalised, err := normaliseFingerprint(entry)
		if err != nil {
			return fmt.Errorf("pinned fingerprint %d: %w", i, err)
		}
		allowed[normalised] = struct{}{}
	}

	for i, quorumSet := range quorumSets {
		actual := sha256.Sum256(quorumSet)
		if _, ok := allowed[hex.EncodeToString(actual[:])]; !ok {
			return fmt.Errorf(
				"%w: quorum set %d fingerprints to %s",
				ErrUnpinnedQuorumSet, i, hex.EncodeToString(actual[:]),
			)
		}
	}

	return nil
}

func normaliseFingerprint(entry string) (string, error) {
	trimmed := strings.ToLower(strings.TrimPrefix(strings.TrimSpace(entry), "0x"))
	decoded, err := hex.DecodeString(trimmed)
	if err != nil {
		return "", fmt.Errorf("%q is not hex: %w", entry, err)
	}
	if len(decoded) != 32 {
		return "", fmt.Errorf("%q is %d bytes, expected a 32 byte sha256", entry, len(decoded))
	}
	return trimmed, nil
}
