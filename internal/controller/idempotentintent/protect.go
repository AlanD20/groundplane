package idempotentintent

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"

	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	protectedPlaintextBytes         = 1 + sha256.Size
	maximumProtectedCiphertextBytes = 4 << 10
)

// Protect consumes digest and seals only its versioned binary representation.
func Protect(ctx context.Context, protector *secretvalue.Protector, version Version, digest *Digest) (
	secretvalue.Envelope,
	error,
) {
	value, err := digest.consume()
	if err != nil {
		return secretvalue.Envelope{}, err
	}
	defer clear(value[:])
	if ctx == nil {
		return secretvalue.Envelope{}, internalError("context is required")
	}
	if err := ctx.Err(); err != nil {
		return secretvalue.Envelope{}, err
	}
	if protector == nil {
		return secretvalue.Envelope{}, internalError("protector is required")
	}
	if version != Version1 {
		return secretvalue.Envelope{}, internalError("canonicalization version is invalid")
	}
	plaintext := make([]byte, protectedPlaintextBytes)
	defer clear(plaintext)
	plaintext[0] = byte(version)
	copy(plaintext[1:], value[:])
	envelope, err := protector.Seal(ctx, plaintext)
	if err != nil {
		return secretvalue.Envelope{}, err
	}
	ciphertext := envelope.Ciphertext()
	defer clear(ciphertext)
	if len(ciphertext) > maximumProtectedCiphertextBytes {
		return secretvalue.Envelope{}, internalError("protected intent exceeds ciphertext limit")
	}
	return envelope, nil
}

// CompareProtected compares two protected durable values in constant time
// inside nested Protector plaintext callbacks.
func CompareProtected(
	ctx context.Context,
	protector *secretvalue.Protector,
	existing secretvalue.Envelope,
	candidate secretvalue.Envelope,
) (bool, error) {
	if ctx == nil {
		return false, internalError("context is required")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if protector == nil {
		return false, internalError("protector is required")
	}
	matched := false
	err := protector.Open(ctx, candidate, func(candidatePlaintext []byte) error {
		if len(candidatePlaintext) != protectedPlaintextBytes || Version(candidatePlaintext[0]) != Version1 {
			return errs.New(errs.KindInternal, "idempotent intent: protected digest is malformed")
		}
		return protector.Open(ctx, existing, func(existingPlaintext []byte) error {
			if len(existingPlaintext) != protectedPlaintextBytes || Version(existingPlaintext[0]) != Version1 {
				return errs.New(errs.KindInternal, "idempotent intent: protected digest is malformed")
			}
			matched = subtle.ConstantTimeCompare(existingPlaintext, candidatePlaintext) == 1
			return nil
		})
	})
	if err != nil {
		return false, err
	}
	return matched, nil
}
