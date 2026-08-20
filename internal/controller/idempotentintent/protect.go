package idempotentintent

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"

	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const protectedPlaintextBytes = 1 + sha256.Size

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
	return protector.Seal(ctx, plaintext)
}

// Compare consumes candidate and compares it with one protected durable value
// in constant time inside the Protector plaintext callback.
func Compare(
	ctx context.Context,
	protector *secretvalue.Protector,
	envelope secretvalue.Envelope,
	version Version,
	candidate *Digest,
) (bool, error) {
	value, err := candidate.consume()
	if err != nil {
		return false, err
	}
	defer clear(value[:])
	if ctx == nil {
		return false, internalError("context is required")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if protector == nil {
		return false, internalError("protector is required")
	}
	if version != Version1 {
		return false, internalError("canonicalization version is invalid")
	}
	matched := false
	err = protector.Open(ctx, envelope, func(plaintext []byte) error {
		if len(plaintext) != protectedPlaintextBytes || Version(plaintext[0]) != Version1 {
			return errs.New(errs.KindInternal, "idempotent intent: protected digest is malformed")
		}
		matched = subtle.ConstantTimeCompare(plaintext[1:], value[:]) == 1
		return nil
	})
	if err != nil {
		return false, err
	}
	return matched, nil
}
