package secretvalue

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: sealing must leave caller-owned plaintext unchanged while every
// private plaintext and provider-owned ciphertext copy is cleared.
func TestSealOwnsCiphertextAndClearsTemporaryBuffers(t *testing.T) {
	t.Parallel()

	plaintext := []byte("environment-secret")
	providerCiphertext := []byte("age-encrypted-value")
	sealer := &retainingSealer{ciphertext: providerCiphertext}
	protector := mustProtector(t, sealer, &retainingOpener{})

	envelope, err := protector.Seal(context.Background(), plaintext)
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	if string(plaintext) != "environment-secret" {
		t.Fatalf("caller plaintext = %q, want unchanged", plaintext)
	}
	if !allZero(sealer.plaintext) {
		t.Fatal("Seal() did not clear the private plaintext passed to the sealer")
	}
	if !allZero(providerCiphertext) {
		t.Fatal("Seal() did not clear provider-owned ciphertext after copying it")
	}
	if got := string(envelope.Ciphertext()); got != "age-encrypted-value" {
		t.Fatalf("Ciphertext() = %q, want age-encrypted-value", got)
	}
	firstCopy := envelope.Ciphertext()
	firstCopy[0] = 'X'
	if got := string(envelope.Ciphertext()); got != "age-encrypted-value" {
		t.Fatalf("Ciphertext() exposed mutable envelope storage: %q", got)
	}
	if err := envelope.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

// Rationale: decrypted values may exist only inside one callback; the opener,
// callback, and ciphertext buffers must all be cleared when it returns.
func TestOpenLimitsPlaintextLifetimeAndClearsTemporaryBuffers(t *testing.T) {
	t.Parallel()

	ciphertext := []byte("age-encrypted-value")
	envelope, err := Restore(currentMetadata(ciphertext), ciphertext)
	if err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	openerPlaintext := []byte("file-secret-contents")
	opener := &retainingOpener{plaintext: openerPlaintext}
	protector := mustProtector(t, &retainingSealer{}, opener)
	var callbackBuffer []byte
	var observed string

	err = protector.Open(context.Background(), envelope, func(plaintext []byte) error {
		callbackBuffer = plaintext
		observed = string(plaintext)
		return nil
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if observed != "file-secret-contents" {
		t.Fatalf("callback plaintext = %q, want file-secret-contents", observed)
	}
	if !allZero(opener.ciphertext) {
		t.Fatal("Open() did not clear the ciphertext copy passed to the opener")
	}
	if !allZero(opener.returned) {
		t.Fatal("Open() did not clear opener-owned plaintext")
	}
	if !allZero(callbackBuffer) {
		t.Fatal("Open() did not clear callback plaintext")
	}
	if got := string(envelope.Ciphertext()); got != "age-encrypted-value" {
		t.Fatalf("Open() mutated envelope ciphertext: %q", got)
	}
}

// Rationale: durable corruption or unsupported metadata must fail before any
// plaintext is requested from the Controller key.
func TestRestoreAndOpenRejectInvalidEnvelopesBeforeDecryption(t *testing.T) {
	t.Parallel()

	ciphertext := []byte("ciphertext")
	valid := currentMetadata(ciphertext)
	tests := []struct {
		name       string
		metadata   Metadata
		ciphertext []byte
	}{
		{name: "version", metadata: mutateMetadata(valid, func(value *Metadata) {
			value.Version = 2
		}), ciphertext: ciphertext},
		{name: "cipher", metadata: mutateMetadata(valid, func(value *Metadata) {
			value.Cipher = "unknown"
		}), ciphertext: ciphertext},
		{name: "digest algorithm", metadata: mutateMetadata(valid, func(value *Metadata) {
			value.Digest.Algorithm = "unknown"
		}), ciphertext: ciphertext},
		{name: "uppercase digest", metadata: mutateMetadata(valid, func(value *Metadata) {
			value.Digest.Value = strings.ToUpper(value.Digest.Value)
		}), ciphertext: ciphertext},
		{name: "malformed digest", metadata: mutateMetadata(valid, func(value *Metadata) {
			value.Digest.Value = "not-a-digest"
		}), ciphertext: ciphertext},
		{name: "digest mismatch", metadata: valid, ciphertext: []byte("tampered")},
		{name: "empty ciphertext", metadata: currentMetadata(nil), ciphertext: nil},
		{
			name:       "record limit",
			metadata:   currentMetadata(make([]byte, maxCiphertextBytes+1)),
			ciphertext: make([]byte, maxCiphertextBytes+1),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := Restore(test.metadata, test.ciphertext); err == nil {
				t.Fatal("Restore() error = nil, want refusal")
			}
			opener := &retainingOpener{plaintext: []byte("must-not-open")}
			protector := mustProtector(t, &retainingSealer{}, opener)
			invalid := Envelope{
				metadata:   test.metadata,
				ciphertext: append([]byte(nil), test.ciphertext...),
			}
			if err := protector.Open(context.Background(), invalid, func([]byte) error { return nil }); err == nil {
				t.Fatal("Open() error = nil, want refusal")
			}
			if opener.calls != 0 {
				t.Fatalf("opener calls = %d, want zero", opener.calls)
			}
		})
	}
}

// Rationale: dependency and callback failures may contain plaintext, so this
// boundary must classify them without preserving secret-bearing error text.
func TestProtectorErrorsNeverRetainPlaintext(t *testing.T) {
	t.Parallel()

	const secret = "plaintext-must-not-leak"
	sealer := &retainingSealer{err: errors.New(secret), ciphertext: []byte("partial-ciphertext")}
	protector := mustProtector(t, sealer, &retainingOpener{})
	_, err := protector.Seal(context.Background(), []byte(secret))
	assertNoSecret(t, err, secret)
	if !allZero(sealer.plaintext) || !allZero(sealer.ciphertext) {
		t.Fatal("Seal() failure did not clear provider buffers")
	}

	ciphertext := []byte("ciphertext")
	envelope, err := Restore(currentMetadata(ciphertext), ciphertext)
	if err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	opener := &retainingOpener{plaintext: []byte(secret), err: errors.New(secret)}
	protector = mustProtector(t, &retainingSealer{}, opener)
	err = protector.Open(context.Background(), envelope, func([]byte) error { return nil })
	assertNoSecret(t, err, secret)
	if !allZero(opener.ciphertext) || !allZero(opener.returned) {
		t.Fatal("Open() failure did not clear provider buffers")
	}

	opener = &retainingOpener{plaintext: []byte(secret)}
	protector = mustProtector(t, &retainingSealer{}, opener)
	var callbackBuffer []byte
	err = protector.Open(context.Background(), envelope, func(plaintext []byte) error {
		callbackBuffer = plaintext
		return errors.New(secret)
	})
	assertNoSecret(t, err, secret)
	if !allZero(callbackBuffer) {
		t.Fatal("callback failure did not clear transient plaintext")
	}
}

// Rationale: a nil context is a wiring fault and must fail as an internal
// error before secret providers or plaintext callbacks can observe a call.
func TestProtectorRejectsNilContextBeforeDependencies(t *testing.T) {
	t.Parallel()

	sealer := &retainingSealer{ciphertext: []byte("must-not-seal")}
	opener := &retainingOpener{plaintext: []byte("must-not-open")}
	protector := mustProtector(t, sealer, opener)
	_, sealErr := protector.Seal(nil, []byte("plaintext"))
	assertInternal(t, sealErr)
	if sealer.calls != 0 {
		t.Fatalf("sealer calls = %d, want zero", sealer.calls)
	}

	ciphertext := []byte("ciphertext")
	envelope, err := Restore(currentMetadata(ciphertext), ciphertext)
	if err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	callbackCalled := false
	openErr := protector.Open(nil, envelope, func([]byte) error {
		callbackCalled = true
		return nil
	})
	assertInternal(t, openErr)
	if opener.calls != 0 {
		t.Fatalf("opener calls = %d, want zero", opener.calls)
	}
	if callbackCalled {
		t.Fatal("plaintext callback was called")
	}
}

// Rationale: one Protector is application-scoped, so parallel secret
// operations must not share or race on transient buffers.
func TestProtectorSupportsConcurrentIndependentOperations(t *testing.T) {
	protector := mustProtector(t, deterministicSealer{}, deterministicOpener{})
	const operations = 64
	errorsByOperation := make(chan error, operations)
	var workers sync.WaitGroup
	workers.Add(operations)
	for index := range operations {
		go func() {
			defer workers.Done()
			want := []byte{byte(index), byte(index >> 8), 0xa5}
			envelope, err := protector.Seal(context.Background(), want)
			if err != nil {
				errorsByOperation <- err
				return
			}
			err = protector.Open(context.Background(), envelope, func(got []byte) error {
				if !bytes.Equal(got, want) {
					return errors.New("round trip mismatch")
				}
				return nil
			})
			if err != nil {
				errorsByOperation <- err
			}
		}()
	}
	workers.Wait()
	close(errorsByOperation)
	for err := range errorsByOperation {
		t.Fatalf("concurrent operation error = %v", err)
	}
}

type retainingSealer struct {
	plaintext  []byte
	ciphertext []byte
	err        error
	calls      int
}

func (s *retainingSealer) Seal(_ context.Context, plaintext []byte) ([]byte, error) {
	s.calls++
	s.plaintext = plaintext
	return s.ciphertext, s.err
}

type retainingOpener struct {
	plaintext  []byte
	ciphertext []byte
	returned   []byte
	err        error
	calls      int
}

func (o *retainingOpener) Open(_ context.Context, ciphertext []byte) ([]byte, error) {
	o.calls++
	o.ciphertext = ciphertext
	o.returned = append([]byte(nil), o.plaintext...)
	return o.returned, o.err
}

type deterministicSealer struct{}

func (deterministicSealer) Seal(_ context.Context, plaintext []byte) ([]byte, error) {
	return append([]byte("sealed:"), plaintext...), nil
}

type deterministicOpener struct{}

func (deterministicOpener) Open(_ context.Context, ciphertext []byte) ([]byte, error) {
	const prefix = "sealed:"
	if !bytes.HasPrefix(ciphertext, []byte(prefix)) {
		return nil, errors.New("invalid ciphertext")
	}
	return append([]byte(nil), ciphertext[len(prefix):]...), nil
}

func mustProtector(t *testing.T, sealer Sealer, opener Opener) *Protector {
	t.Helper()
	protector, err := NewProtector(sealer, opener)
	if err != nil {
		t.Fatalf("NewProtector() error = %v", err)
	}
	return protector
}

func mutateMetadata(metadata Metadata, mutate func(*Metadata)) Metadata {
	mutate(&metadata)
	return metadata
}

func allZero(value []byte) bool {
	for _, item := range value {
		if item != 0 {
			return false
		}
	}
	return true
}

func assertNoSecret(t *testing.T, err error, secret string) {
	t.Helper()
	if err == nil {
		t.Fatal("error = nil, want failure")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("error leaked plaintext: %v", err)
	}
}

func assertInternal(t *testing.T, err error) {
	t.Helper()
	kind, ok := errs.KindOf(err)
	if !ok || kind != errs.KindInternal {
		t.Fatalf("error = %v, want internal kind", err)
	}
}
