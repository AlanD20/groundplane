package age

import (
	"bytes"
	"testing"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// Rationale: the public 255 KiB reusable-Secret input ceiling is valid only
// if the production X25519 age wrapper always stays within the locked 256 KiB
// durable ciphertext ceiling and decrypts the boundary value exactly.
func TestMaximumSecretValueFitsDurableCiphertextLimit(t *testing.T) {
	t.Parallel()
	keypair, err := GenerateKeypair()
	if err != nil {
		t.Fatalf("GenerateKeypair() error = %v", err)
	}
	plaintext := bytes.Repeat([]byte{'s'}, apiTypes.MaximumSecretValueBytes)
	defer clear(plaintext)
	ciphertext, err := Encrypt(keypair.Recipient, plaintext)
	if err != nil {
		t.Fatalf("Encrypt(maximum Secret value) error = %v", err)
	}
	defer clear(ciphertext)
	if len(ciphertext) > 256<<10 {
		t.Fatalf("Encrypt(maximum Secret value) ciphertext bytes = %d, want <= %d", len(ciphertext), 256<<10)
	}
	revealed, err := Decrypt(keypair.Identity, ciphertext)
	if err != nil {
		t.Fatalf("Decrypt(maximum Secret value) error = %v", err)
	}
	defer clear(revealed)
	if !bytes.Equal(revealed, plaintext) {
		t.Fatal("Decrypt(maximum Secret value) did not preserve exact bytes")
	}
}
