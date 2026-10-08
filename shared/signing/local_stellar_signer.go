package signing

import (
	"context"
	"fmt"

	"github.com/stellar/go-stellar-sdk/keypair"
)

type LocalStellarSigner struct {
	full *keypair.Full
}

var _ Signer = (*LocalStellarSigner)(nil)

func NewLocalStellarSigner(secretSeed string) (*LocalStellarSigner, error) {
	full, err := keypair.ParseFull(secretSeed)
	if err != nil {
		return nil, fmt.Errorf("failed to parse stellar secret seed: %w", err)
	}

	return &LocalStellarSigner{full: full}, nil
}

// Stellar transactions are signed by the bridge client, which holds the
// network passphrase the signature commits to.
//
//nolint:nilnil // intentional no-op: signing happens in the bridge client
func (*LocalStellarSigner) Sign(_ context.Context, _ string, _ Transaction) (Transaction, error) {
	return nil, nil
}

func (s *LocalStellarSigner) Keypair() *keypair.Full {
	return s.full
}

func (s *LocalStellarSigner) Address(_ context.Context) []byte {
	return []byte(s.full.Address())
}
