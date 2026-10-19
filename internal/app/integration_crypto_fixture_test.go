package app

import (
	"bytes"
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	ageinfra "github.com/AlanD20/groundplane/internal/infra/age"
)

func manualJourneyEncryptedValue(t *testing.T, value string) (*secretvalue.Protector, []byte) {
	t.Helper()
	key, err := ageinfra.GenerateKeypair()
	if err != nil {
		t.Fatal(err)
	}
	cipher := manualJourneyAgeCipher{key: key}
	protector, err := secretvalue.NewProtector(cipher, cipher)
	if err != nil {
		t.Fatal(err)
	}
	plaintext := []byte(value)
	defer clear(plaintext)
	envelope, err := protector.Seal(context.Background(), plaintext)
	if err != nil {
		t.Fatal(err)
	}
	defer envelope.Clear()
	ciphertext := envelope.Ciphertext()
	if bytes.Contains(ciphertext, plaintext) {
		t.Fatal("test encryption retained plaintext")
	}
	return protector, ciphertext
}

// A single fresh in-memory X25519 identity stands in for the Controller key;
// no key files, existing installation credentials, or backup operations run.
type manualJourneyAgeCipher struct{ key ageinfra.Keypair }

func (cipher manualJourneyAgeCipher) Seal(ctx context.Context, value []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return ageinfra.Encrypt(cipher.key.Recipient, value)
}

func (cipher manualJourneyAgeCipher) Open(ctx context.Context, value []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return ageinfra.Decrypt(cipher.key.Identity, value)
}
