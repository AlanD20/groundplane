package app

import (
	"context"

	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	ageinfra "github.com/AlanD20/groundplane/internal/infra/age"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// secretValueCipher adapts the synchronous root Controller age key to the
// context-aware cryptographic ports owned by the secret-value Protector.
type secretValueCipher struct {
	key controllerKey
}

func newSecretValueProtector(key controllerKey) (*secretvalue.Protector, error) {
	if key == nil {
		return nil, errs.New(errs.KindInternal, "secret value controller key is required")
	}
	cipher := &secretValueCipher{key: key}
	return secretvalue.NewProtector(cipher, cipher)
}

func (cipher *secretValueCipher) Seal(ctx context.Context, plaintext []byte) ([]byte, error) {
	if ctx == nil {
		return nil, errs.New(errs.KindInternal, "secret value seal context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	ciphertext, err := cipher.key.Wrap(plaintext)
	if contextErr := ctx.Err(); contextErr != nil {
		clear(ciphertext)
		return nil, contextErr
	}
	if err != nil {
		clear(ciphertext)
		return nil, errs.New(errs.KindInternal, "secret value controller key wrap failed")
	}
	return ciphertext, nil
}

func (cipher *secretValueCipher) Open(ctx context.Context, ciphertext []byte) ([]byte, error) {
	if ctx == nil {
		return nil, errs.New(errs.KindInternal, "secret value open context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	plaintext, err := cipher.key.Unwrap(ciphertext)
	if contextErr := ctx.Err(); contextErr != nil {
		clear(plaintext)
		return nil, contextErr
	}
	if err != nil {
		clear(plaintext)
		return nil, errs.New(errs.KindInternal, "secret value controller key unwrap failed")
	}
	return plaintext, nil
}

var _ secretvalue.Sealer = (*secretValueCipher)(nil)
var _ secretvalue.Opener = (*secretValueCipher)(nil)
var _ controllerKey = (*ageinfra.ControllerKey)(nil)
