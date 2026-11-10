package ibcv2

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

func fingerprint(quorumSet []byte) string {
	sum := sha256.Sum256(quorumSet)
	return hex.EncodeToString(sum[:])
}

func TestVerifyQuorumFingerprintsAcceptsAPinnedSet(t *testing.T) {
	quorumSet := []byte("tier-one")

	if err := VerifyQuorumFingerprints([][]byte{quorumSet}, []string{fingerprint(quorumSet)}); err != nil {
		t.Fatalf("a pinned quorum set must be accepted, got %v", err)
	}
}

func TestVerifyQuorumFingerprintsAcceptsAPrefixedAndUppercasePin(t *testing.T) {
	quorumSet := []byte("tier-one")
	pin := "0x" + strings.ToUpper(fingerprint(quorumSet))

	if err := VerifyQuorumFingerprints([][]byte{quorumSet}, []string{pin}); err != nil {
		t.Fatalf("pin formatting must not change the decision, got %v", err)
	}
}

// The substitution this exists to stop: the gateway serves a validator set the
// operator never approved.
func TestVerifyQuorumFingerprintsRejectsASubstitutedSet(t *testing.T) {
	err := VerifyQuorumFingerprints([][]byte{[]byte("attacker")}, []string{fingerprint([]byte("tier-one"))})

	if !errors.Is(err, ErrUnpinnedQuorumSet) {
		t.Fatalf("a substituted quorum set must be refused, got %v", err)
	}
}

func TestVerifyQuorumFingerprintsRejectsAnEmptyPinList(t *testing.T) {
	if err := VerifyQuorumFingerprints([][]byte{[]byte("tier-one")}, nil); err == nil {
		t.Fatal("an empty pin list must refuse rather than trust whatever arrives")
	}
}

func TestVerifyQuorumFingerprintsRejectsNoQuorumSets(t *testing.T) {
	if err := VerifyQuorumFingerprints(nil, []string{fingerprint([]byte("tier-one"))}); err == nil {
		t.Fatal("a client with no trust root must not be created")
	}
}

// Every set must be pinned, not just one: a client evaluates the predicate
// against whichever configuration governs the slot.
func TestVerifyQuorumFingerprintsRequiresEverySetToBePinned(t *testing.T) {
	good := []byte("tier-one")
	bad := []byte("attacker")

	err := VerifyQuorumFingerprints([][]byte{good, bad}, []string{fingerprint(good)})

	if !errors.Is(err, ErrUnpinnedQuorumSet) {
		t.Fatalf("one unpinned set among several must refuse the whole list, got %v", err)
	}
}

func TestVerifyQuorumFingerprintsRejectsAMalformedPin(t *testing.T) {
	quorumSet := []byte("tier-one")

	for name, pin := range map[string]string{"not hex": "zz", "too short": "abcd"} {
		t.Run(name, func(t *testing.T) {
			if err := VerifyQuorumFingerprints([][]byte{quorumSet}, []string{pin}); err == nil {
				t.Fatalf("expected %s to be rejected", name)
			}
		})
	}
}
