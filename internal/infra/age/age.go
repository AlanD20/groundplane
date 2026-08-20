// Package age implements the two encryption layers described in mvp.md,
// "Secrets & generated credentials": the single root-only controller key
// that wraps every secret value at rest, and the per-environment age
// keypair used for backup encryption.
package age

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	age "filippo.io/age"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const maxIdentitySize = 4096

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
func GenerateKeypair() (Keypair, error) {
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		return Keypair{}, errs.Wrap(errs.KindInternal, fmt.Errorf("age: generate identity: %w", err))
	}
	return Keypair{
		Recipient: identity.Recipient().String(),
		Identity:  identity.String(),
	}, nil
}

// ControllerKey wraps and unwraps secret-store values at rest with the
// single, platform-wide, root-only key file at
// /etc/groundplane/controller.age (0600). Never per-environment — a
// per-environment wrapping key would be reachable by the same
// compromise and buy nothing. See mvp.md, "Encryption at rest + backup
// encryption (locked)".
type ControllerKey struct {
	Path string

	mu       sync.RWMutex
	identity *age.X25519Identity
}

// Load reads the key file (generating one if it doesn't exist yet on
// first boot) — file I/O, so ctx first.
func (k *ControllerKey) Load(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if k.Path == "" {
		return errs.New(errs.KindValidationFailed, "age: controller key path is required")
	}

	k.mu.Lock()
	defer k.mu.Unlock()
	if k.identity != nil {
		return nil
	}

	parentPath := filepath.Dir(k.Path)
	if err := os.MkdirAll(parentPath, 0o700); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("age: create key directory: %w", err))
	}
	if err := validateRootOwnedDirectory(parentPath); err != nil {
		return err
	}
	root, err := os.OpenRoot(parentPath)
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("age: open key directory: %w", err))
	}
	defer root.Close()

	name := filepath.Base(k.Path)
	identity, err := loadIdentity(root, name)
	if err == nil {
		k.identity = identity
		return nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return err
	}

	identity, err = age.GenerateX25519Identity()
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("age: generate controller identity: %w", err))
	}
	file, err := root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if errors.Is(err, fs.ErrExist) {
		identity, err = loadIdentity(root, name)
		if err != nil {
			return err
		}
		k.identity = identity
		return nil
	}
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("age: create controller key: %w", err))
	}
	created := true
	defer func() {
		if created {
			_ = root.Remove(name)
		}
	}()
	if _, err := io.WriteString(file, identity.String()+"\n"); err != nil {
		_ = file.Close()
		return errs.Wrap(errs.KindInternal, fmt.Errorf("age: write controller key: %w", err))
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return errs.Wrap(errs.KindInternal, fmt.Errorf("age: sync controller key: %w", err))
	}
	if err := file.Close(); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("age: close controller key: %w", err))
	}
	directory, err := root.Open(".")
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("age: open key directory for sync: %w", err))
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return errs.Wrap(errs.KindInternal, fmt.Errorf("age: sync key directory: %w", err))
	}
	if err := directory.Close(); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("age: close key directory: %w", err))
	}
	created = false
	k.identity = identity
	return nil
}

func (k *ControllerKey) Wrap(plaintext []byte) ([]byte, error) {
	k.mu.RLock()
	identity := k.identity
	k.mu.RUnlock()
	if identity == nil {
		return nil, errs.New(errs.KindInternal, "age: controller key is not loaded")
	}
	return encrypt(identity.Recipient(), plaintext)
}

func (k *ControllerKey) Unwrap(ciphertext []byte) ([]byte, error) {
	k.mu.RLock()
	identity := k.identity
	k.mu.RUnlock()
	if identity == nil {
		return nil, errs.New(errs.KindInternal, "age: controller key is not loaded")
	}
	plaintext, err := decrypt(identity, ciphertext)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return plaintext, nil
}

// Encrypt encrypts data under a recipient's public key (backup
// encryption path — the environment's AgeRecipient). Pure crypto, no ctx.
func Encrypt(recipient string, plaintext []byte) ([]byte, error) {
	parsed, err := age.ParseX25519Recipient(strings.TrimSpace(recipient))
	if err != nil {
		return nil, errs.Wrap(errs.KindValidationFailed, fmt.Errorf("age: invalid recipient: %w", err))
	}
	return encrypt(parsed, plaintext)
}

// Decrypt decrypts data under a private identity (restore path — the
// environment's current identity, or an operator-supplied exported one
// for a recovery point encrypted under a rotated-away key).
func Decrypt(identity string, ciphertext []byte) ([]byte, error) {
	parsed, err := age.ParseX25519Identity(strings.TrimSpace(identity))
	if err != nil {
		return nil, errs.Wrap(errs.KindValidationFailed, fmt.Errorf("age: invalid identity: %w", err))
	}
	return decrypt(parsed, ciphertext)
}

func encrypt(recipient age.Recipient, plaintext []byte) ([]byte, error) {
	var encrypted bytes.Buffer
	writer, err := age.Encrypt(&encrypted, recipient)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, fmt.Errorf("age: initialize encryption: %w", err))
	}
	if _, err := writer.Write(plaintext); err != nil {
		_ = writer.Close()
		return nil, errs.Wrap(errs.KindInternal, fmt.Errorf("age: encrypt: %w", err))
	}
	if err := writer.Close(); err != nil {
		return nil, errs.Wrap(errs.KindInternal, fmt.Errorf("age: finalize encryption: %w", err))
	}
	return encrypted.Bytes(), nil
}

func decrypt(identity age.Identity, ciphertext []byte) ([]byte, error) {
	reader, err := age.Decrypt(bytes.NewReader(ciphertext), identity)
	if err != nil {
		return nil, errs.Wrap(errs.KindValidationFailed, fmt.Errorf("age: decrypt: %w", err))
	}
	plaintext, err := io.ReadAll(reader)
	if err != nil {
		return nil, errs.Wrap(errs.KindValidationFailed, fmt.Errorf("age: read plaintext: %w", err))
	}
	return plaintext, nil
}

func loadIdentity(root *os.Root, name string) (*age.X25519Identity, error) {
	return loadIdentityOwnedBy(root, name, 0)
}

func loadIdentityOwnedBy(root *os.Root, name string, expectedUID uint32) (*age.X25519Identity, error) {
	info, err := root.Lstat(name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fs.ErrNotExist
		}
		return nil, errs.Wrap(errs.KindInternal, fmt.Errorf("age: inspect controller key: %w", err))
	}
	if !info.Mode().IsRegular() {
		return nil, errs.New(errs.KindInternal, "age: controller key is not a regular file")
	}
	if info.Mode().Perm() != 0o600 {
		return nil, errs.Newf(errs.KindInternal, "age: controller key mode is %04o, want 0600", info.Mode().Perm())
	}
	if err := validateOwnership(info, "controller key", expectedUID); err != nil {
		return nil, err
	}

	file, err := root.Open(name)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, fmt.Errorf("age: open controller key: %w", err))
	}
	defer file.Close()
	contents, err := io.ReadAll(io.LimitReader(file, maxIdentitySize+1))
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, fmt.Errorf("age: read controller key: %w", err))
	}
	if len(contents) > maxIdentitySize {
		return nil, errs.New(errs.KindInternal, "age: controller key exceeds size limit")
	}
	identity, err := age.ParseX25519Identity(strings.TrimSpace(string(contents)))
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, fmt.Errorf("age: parse controller key: %w", err))
	}
	return identity, nil
}

func validateRootOwnedDirectory(path string) error {
	return validateOwnedDirectory(path, 0)
}

func validateOwnedDirectory(path string, expectedUID uint32) error {
	info, err := os.Stat(path)
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("age: inspect key directory: %w", err))
	}
	if !info.IsDir() {
		return errs.New(errs.KindInternal, "age: key parent is not a directory")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return errs.Newf(
			errs.KindInternal,
			"age: key directory mode is %04o, want no group or other access",
			info.Mode().Perm(),
		)
	}
	return validateOwnership(info, "key directory", expectedUID)
}

func validateOwnership(info fs.FileInfo, label string, expectedUID uint32) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return errs.Newf(errs.KindInternal, "age: cannot determine %s ownership", label)
	}
	if stat.Uid != expectedUID {
		return errs.Newf(errs.KindInternal, "age: %s is owned by uid %d, want %d", label, stat.Uid, expectedUID)
	}
	return nil
}
