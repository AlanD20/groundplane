package secretvalue

import (
	"context"
	"testing"
)

// Rationale: Backup delivery already owns durable ciphertext and needs one
// clearable path through the crypto port without Protector-created copies.
// Rationale: the owned seam must release every caller/provider byte buffer
// after consumption without claiming impossible Go runtime zeroization.
func TestOpenOwnedConsumesCiphertextAndProviderPlaintext(t *testing.T) {
	ciphertext := []byte("owned-ciphertext")
	envelope, err := RestoreOwned(currentMetadata(ciphertext), ciphertext)
	if err != nil {
		t.Fatalf("restore owned envelope: %v", err)
	}
	openerPlaintext := []byte("owned-plaintext")
	opener := &retainingOpener{plaintext: openerPlaintext}
	protector := mustProtector(t, &retainingSealer{}, opener)
	var callback []byte
	if err := protector.OpenOwned(context.Background(), &envelope, func(value []byte) error {
		callback = value
		return nil
	}); err != nil {
		t.Fatalf("open owned envelope: %v", err)
	}
	if !allZero(ciphertext) || !allZero(opener.ciphertext) ||
		!allZero(opener.returned) || !allZero(callback) {
		t.Fatal("open owned envelope retained an owned byte buffer")
	}
	if envelope.ciphertext != nil {
		t.Fatal("open owned envelope retained the consumed envelope")
	}
}

// Rationale: ownership transfer applies on validation failure too; callers
// must not need a second error-only cleanup path.
// Rationale: rejected envelope metadata must not leave its caller-owned
// ciphertext resident after validation fails.
func TestRestoreOwnedClearsRejectedCiphertext(t *testing.T) {
	ciphertext := []byte("tampered")
	metadata := currentMetadata([]byte("different"))
	if _, err := RestoreOwned(metadata, ciphertext); err == nil {
		t.Fatal("restore owned envelope error = nil")
	}
	if !allZero(ciphertext) {
		t.Fatal("restore owned envelope retained rejected ciphertext")
	}
}
