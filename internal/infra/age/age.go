// Package age implements the two encryption layers described in mvp.md,
// "Secrets & generated credentials": the single root-only controller key
// that wraps every secret value at rest, and the per-environment age
// keypair used for backup encryption.
package age

import (
	"context"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Keypair is a per-environment backup-encryption identity, generated
// LAZILY the first time backups are enabled for that environment (a
// backing environment never generates one — see mvp.md).
type Keypair struct {
	Recipient string // public — safe in desired state
	Identity  string // private — stored in the secret store, wrapped by ControllerKey
}

// GenerateKeypair creates a new age X25519 keypair. Pure in-memory
// crypto — no I/O, so no ctx (see docs/standards.md, section 4:
// the ctx-first rule applies to functions with side effects).
//
// TODO: wire filippo.io/age (age.GenerateX25519Identity).
func GenerateKeypair() (Keypair, error) {
	return Keypair{}, errs.New(errs.CodeNotImplemented, "age: not implemented — wire filippo.io/age in internal/infra/age")
}

// ControllerKey wraps and unwraps secret-store values at rest with the
// single, platform-wide, root-only key file at
// /etc/groundplane/controller.age (0600). Never per-environment — a
// per-environment wrapping key would be reachable by the same
// compromise and buy nothing. See mvp.md, "Encryption at rest + backup
// encryption (locked)".
type ControllerKey struct {
	Path string
}

// Load reads the key file (generating one if it doesn't exist yet on
// first boot) — file I/O, so ctx first.
//
// TODO: wire the actual X25519 key material + file permission
// enforcement (0600, root-only).
func (k *ControllerKey) Load(ctx context.Context) error {
	return errs.New(errs.CodeNotImplemented, "age: not implemented")
}

func (k *ControllerKey) Wrap(plaintext []byte) ([]byte, error) {
	return nil, errs.New(errs.CodeNotImplemented, "age: not implemented")
}

func (k *ControllerKey) Unwrap(ciphertext []byte) ([]byte, error) {
	return nil, errs.New(errs.CodeNotImplemented, "age: not implemented")
}

// Encrypt encrypts data under a recipient's public key (backup
// encryption path — the environment's AgeRecipient). Pure crypto, no ctx.
func Encrypt(recipient string, plaintext []byte) ([]byte, error) {
	return nil, errs.New(errs.CodeNotImplemented, "age: not implemented")
}

// Decrypt decrypts data under a private identity (restore path — the
// environment's current identity, or an operator-supplied exported one
// for a recovery point encrypted under a rotated-away key).
func Decrypt(identity string, ciphertext []byte) ([]byte, error) {
	return nil, errs.New(errs.CodeNotImplemented, "age: not implemented")
}
