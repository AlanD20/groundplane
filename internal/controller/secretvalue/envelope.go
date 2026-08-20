// Package secretvalue protects Controller-owned secret values before they
// cross a durable-storage boundary. It defines no resource ownership,
// repository layout, resolution order, or materialization policy.
package secretvalue

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const maxCiphertextBytes = 256 << 10

// EnvelopeVersion identifies this package's ciphertext envelope contract. It
// is independent of the etcd keyspace and record schema versions.
type EnvelopeVersion uint8

const EnvelopeVersion1 EnvelopeVersion = 1

// CipherSuite identifies the injected cryptographic implementation that
// produced an envelope.
type CipherSuite string

const CipherSuiteAgeX25519 CipherSuite = "age-x25519"

// DigestAlgorithm identifies the integrity digest over ciphertext. Digests
// never cover plaintext, which would disclose equality and invite guessing of
// low-entropy secret values.
type DigestAlgorithm string

const DigestAlgorithmSHA256 DigestAlgorithm = "sha256"

// Digest is non-secret envelope metadata. Value is the lowercase hexadecimal
// digest of the ciphertext bytes.
type Digest struct {
	Algorithm DigestAlgorithm
	Value     string
}

// Metadata contains only non-secret, durable-safe envelope fields.
type Metadata struct {
	Version EnvelopeVersion
	Cipher  CipherSuite
	Digest  Digest
}

// Envelope owns authenticated ciphertext and its non-secret metadata. Its
// byte buffer is private; Ciphertext always returns an independent copy.
type Envelope struct {
	metadata   Metadata
	ciphertext []byte
}

// Sealer consumes plaintext synchronously and must not retain its input. It
// returns a caller-owned ciphertext buffer. A Protector may call one Sealer
// concurrently, so implementations must be concurrency-safe.
type Sealer interface {
	Seal(context.Context, []byte) ([]byte, error)
}

// Opener consumes ciphertext synchronously and must not retain its input. It
// returns a caller-owned plaintext buffer. A Protector may call one Opener
// concurrently, so implementations must be concurrency-safe.
type Opener interface {
	Open(context.Context, []byte) ([]byte, error)
}

// PlaintextConsumer may use plaintext only during its invocation. The buffer
// is cleared immediately after the callback returns and must not be retained.
type PlaintextConsumer func([]byte) error

// Protector owns the secret-value cryptographic boundary. Implementations are
// injected so application composition can adapt the Controller age key
// without coupling this package to infrastructure.
type Protector struct {
	sealer Sealer
	opener Opener
}

func NewProtector(sealer Sealer, opener Opener) (*Protector, error) {
	if sealer == nil {
		return nil, errs.New(errs.KindInternal, "secret value: sealer is required")
	}
	if opener == nil {
		return nil, errs.New(errs.KindInternal, "secret value: opener is required")
	}
	return &Protector{sealer: sealer, opener: opener}, nil
}

// Restore validates durable-safe metadata and ciphertext and takes an
// independent copy of the ciphertext buffer.
func Restore(metadata Metadata, ciphertext []byte) (Envelope, error) {
	envelope := Envelope{
		metadata:   metadata,
		ciphertext: append([]byte(nil), ciphertext...),
	}
	if err := envelope.Validate(); err != nil {
		clear(envelope.ciphertext)
		return Envelope{}, err
	}
	return envelope, nil
}

func (e Envelope) Metadata() Metadata { return e.metadata }

func (e Envelope) Ciphertext() []byte { return append([]byte(nil), e.ciphertext...) }

// Validate fails closed when durable envelope metadata, bounds, or integrity
// do not match the only format this implementation understands.
func (e Envelope) Validate() error {
	if e.metadata.Version != EnvelopeVersion1 {
		return errs.New(errs.KindInternal, "secret value: unsupported envelope version")
	}
	if e.metadata.Cipher != CipherSuiteAgeX25519 {
		return errs.New(errs.KindInternal, "secret value: unsupported cipher suite")
	}
	if e.metadata.Digest.Algorithm != DigestAlgorithmSHA256 {
		return errs.New(errs.KindInternal, "secret value: unsupported digest algorithm")
	}
	if len(e.ciphertext) == 0 {
		return errs.New(errs.KindInternal, "secret value: ciphertext is empty")
	}
	if len(e.ciphertext) > maxCiphertextBytes {
		return errs.New(errs.KindInternal, "secret value: ciphertext exceeds record limit")
	}
	digestBytes, err := hex.DecodeString(e.metadata.Digest.Value)
	if err != nil || len(digestBytes) != sha256.Size || hex.EncodeToString(digestBytes) != e.metadata.Digest.Value {
		return errs.New(errs.KindInternal, "secret value: ciphertext digest is malformed")
	}
	want := sha256.Sum256(e.ciphertext)
	if subtle.ConstantTimeCompare(digestBytes, want[:]) != 1 {
		return errs.New(errs.KindInternal, "secret value: ciphertext digest does not match")
	}
	return nil
}

// Seal copies caller-owned plaintext, clears the private copy after the
// synchronous seal, and returns an envelope that owns its ciphertext.
func (p *Protector) Seal(ctx context.Context, plaintext []byte) (Envelope, error) {
	if ctx == nil {
		return Envelope{}, errs.New(errs.KindInternal, "secret value: context is required")
	}
	if err := ctx.Err(); err != nil {
		return Envelope{}, err
	}
	privatePlaintext := append([]byte(nil), plaintext...)
	defer clear(privatePlaintext)

	ciphertext, err := p.sealer.Seal(ctx, privatePlaintext)
	defer clear(ciphertext)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return Envelope{}, contextErr
		}
		return Envelope{}, errs.New(errs.KindInternal, "secret value: seal failed")
	}
	if err := ctx.Err(); err != nil {
		return Envelope{}, err
	}
	return Restore(currentMetadata(ciphertext), ciphertext)
}

// Open validates an envelope before decryption, clears every temporary copy,
// and exposes plaintext only for the duration of consume.
func (p *Protector) Open(ctx context.Context, envelope Envelope, consume PlaintextConsumer) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "secret value: context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if consume == nil {
		return errs.New(errs.KindInternal, "secret value: plaintext consumer is required")
	}
	if err := envelope.Validate(); err != nil {
		return err
	}

	ciphertext := append([]byte(nil), envelope.ciphertext...)
	defer clear(ciphertext)
	plaintext, err := p.opener.Open(ctx, ciphertext)
	defer clear(plaintext)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
		return errs.New(errs.KindInternal, "secret value: open failed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	transient := append([]byte(nil), plaintext...)
	defer clear(transient)
	if err := consume(transient); err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
		return errs.New(errs.KindInternal, "secret value: plaintext consumer failed")
	}
	return nil
}

func currentMetadata(ciphertext []byte) Metadata {
	digest := sha256.Sum256(ciphertext)
	return Metadata{
		Version: EnvelopeVersion1,
		Cipher:  CipherSuiteAgeX25519,
		Digest: Digest{
			Algorithm: DigestAlgorithmSHA256,
			Value:     hex.EncodeToString(digest[:]),
		},
	}
}
