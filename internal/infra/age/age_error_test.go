package age

import (
	"errors"
	"strings"
	"testing"

	filippoage "filippo.io/age"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: invalid operator-supplied age ciphertext is a validation failure,
// while the parsing cause remains private and must not enter the API problem.
func TestDecryptClassifiesInvalidCiphertextAsValidation(t *testing.T) {
	keypair, err := GenerateKeypair()
	if err != nil {
		t.Fatalf("generate keypair: %v", err)
	}
	const ciphertext = "private-ciphertext-material"
	_, err = Decrypt(keypair.Identity, []byte(ciphertext))
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("expected validation failure, got %v", err)
	}
	var domainError *errs.Error
	if !errors.As(err, &domainError) {
		t.Fatalf("expected domain error, got %T", err)
	}
	problem := domainError.ToProblem()
	if strings.Contains(problem.Detail, ciphertext) {
		t.Fatalf("problem detail leaked ciphertext: %q", problem.Detail)
	}
}

// Rationale: ControllerKey decrypts durable Controller-owned ciphertext, so a
// corrupt value is internal state failure even though public Decrypt treats the
// same bytes as operator validation input.
func TestControllerKeyUnwrapClassifiesDurableCiphertextAsInternal(t *testing.T) {
	keypair, err := GenerateKeypair()
	if err != nil {
		t.Fatalf("generate keypair: %v", err)
	}
	identity, err := filippoage.ParseX25519Identity(keypair.Identity)
	if err != nil {
		t.Fatalf("parse generated identity: %v", err)
	}
	controllerKey := &ControllerKey{identity: identity}
	const ciphertext = "private-durable-ciphertext"
	_, err = controllerKey.Unwrap([]byte(ciphertext))
	kind, ok := errs.KindOf(err)
	if !ok || kind != errs.KindInternal {
		t.Fatalf("KindOf = %d, %t; error = %v", kind, ok, err)
	}
	var domainError *errs.Error
	if !errors.As(err, &domainError) {
		t.Fatalf("expected domain error, got %T", err)
	}
	problem := domainError.ToProblem()
	if problem.Detail != "Internal Server Error" || problem.Title != "Internal Server Error" ||
		strings.Contains(problem.Detail, ciphertext) {
		t.Fatalf("public problem = %#v", problem)
	}
}
