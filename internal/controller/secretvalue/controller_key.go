package secretvalue

import (
	"context"

	ageinfra "github.com/AlanD20/groundplane/internal/infra/age"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// secretValueCipher adapts the synchronous root Controller age key to the
// context-aware cryptographic ports owned by the secret-value Protector.
type secretValueCipher struct {
	key secretValueKey
}

// secretValueKey is deliberately local to this adapter. It does not inherit
// repository, Agent credential, or materialization semantics from another app
// wiring seam.
type secretValueKey interface {
	Wrap(plaintext []byte) ([]byte, error)
	Unwrap(ciphertext []byte) ([]byte, error)
}

func NewControllerKeyProtector(key *ageinfra.ControllerKey) (*Protector, error) {
	if key == nil {
		return nil, errs.New(errs.KindInternal, "secret value controller key is required")
	}
	cipher := &secretValueCipher{key: key}
	return NewProtector(cipher, cipher)
}

// NewControllerKeyCipher adapts the loaded Controller key for consumers that
// need context-aware encryption without the value envelope protocol.
func NewControllerKeyCipher(key *ageinfra.ControllerKey) *secretValueCipher {
	return &secretValueCipher{key: key}
}

func (cipher *secretValueCipher) Seal(ctx context.Context, plaintext []byte) ([]byte, error) {
	if ctx == nil {
		return nil, errs.New(errs.KindInternal, "secret value seal context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	privatePlaintext := append([]byte(nil), plaintext...)
	defer clear(privatePlaintext)
	providerCiphertext, err := cipher.key.Wrap(privatePlaintext)
	defer clear(providerCiphertext)
	if contextErr := ctx.Err(); contextErr != nil {
		return nil, contextErr
	}
	if err != nil {
		return nil, errs.New(errs.KindInternal, "secret value controller key wrap failed")
	}

	ciphertext := append([]byte(nil), providerCiphertext...)
	if contextErr := ctx.Err(); contextErr != nil {
		clear(ciphertext)
		return nil, contextErr
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

	privateCiphertext := append([]byte(nil), ciphertext...)
	defer clear(privateCiphertext)
	providerPlaintext, err := cipher.key.Unwrap(privateCiphertext)
	defer clear(providerPlaintext)
	if contextErr := ctx.Err(); contextErr != nil {
		return nil, contextErr
	}
	if err != nil {
		return nil, errs.New(errs.KindInternal, "secret value controller key unwrap failed")
	}

	plaintext := append([]byte(nil), providerPlaintext...)
	if contextErr := ctx.Err(); contextErr != nil {
		clear(plaintext)
		return nil, contextErr
	}
	return plaintext, nil
}

var _ Sealer = (*secretValueCipher)(nil)
var _ Opener = (*secretValueCipher)(nil)
var _ secretValueKey = (*ageinfra.ControllerKey)(nil)
