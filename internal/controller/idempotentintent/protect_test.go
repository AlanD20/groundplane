package idempotentintent

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: a durable marker may contain only a sealed version and digest,
// while the caller-owned digest and every transient plaintext are cleared.
func TestProtectConsumesAndClearsDigest(t *testing.T) {
	t.Parallel()

	cipher := &retainingCipher{}
	protector := testProtector(t, cipher)
	digest := canonicalTestDigest(t, canonicalTestIntent())
	before := digest.state.value
	copy := *digest
	envelope, err := Protect(context.Background(), protector, Version1, digest)
	if err != nil {
		t.Fatalf("Protect() error = %v", err)
	}
	if err := envelope.Validate(); err != nil {
		t.Fatalf("envelope.Validate() error = %v", err)
	}
	if !digest.state.consumed || digest.state.value != [sha256.Size]byte{} {
		t.Fatal("Protect() did not consume and clear the caller digest")
	}
	if copy.state.value != [sha256.Size]byte{} {
		t.Fatal("Protect() did not clear a copied digest handle")
	}
	if len(cipher.sealInput) != protectedPlaintextBytes || cipher.sealInput[0] != byte(Version1) {
		t.Fatalf("sealed plaintext shape = %d bytes, version %d", len(cipher.sealInput), cipher.sealInput[0])
	}
	if got := [sha256.Size]byte(cipher.sealInput[1:]); got != before {
		t.Fatal("sealed digest does not match canonical digest")
	}
	if _, err := Protect(context.Background(), protector, Version1, digest); !hasKind(err, errs.KindInternal) {
		t.Fatalf("second Protect() error = %v, want internal", err)
	}
}

// Rationale: valid duplicates match, changed valid intents mismatch, and both
// candidates are consumed without exposing either digest.
func TestCompareConsumesCandidateAndDistinguishesMismatch(t *testing.T) {
	t.Parallel()

	cipher := &retainingCipher{}
	protector := testProtector(t, cipher)
	version, stored, err := Canonicalize(context.Background(), canonicalTestIntent())
	if err != nil {
		t.Fatalf("Canonicalize(stored) error = %v", err)
	}
	envelope, err := Protect(context.Background(), protector, version, stored)
	if err != nil {
		t.Fatalf("Protect() error = %v", err)
	}

	equal := canonicalTestDigest(t, canonicalTestIntent())
	matched, err := Compare(context.Background(), protector, envelope, Version1, equal)
	if err != nil || !matched {
		t.Fatalf("Compare(equal) = %v, %v; want true, nil", matched, err)
	}
	if !equal.state.consumed || equal.state.value != [sha256.Size]byte{} {
		t.Fatal("Compare(equal) did not consume candidate")
	}

	changedIntent := canonicalTestIntent()
	changedIntent.Path[0].Value = ids.NewAt(ids.KindService, testTime(4), 4)
	changed := canonicalTestDigest(t, changedIntent)
	matched, err = Compare(context.Background(), protector, envelope, Version1, changed)
	if err != nil || matched {
		t.Fatalf("Compare(changed) = %v, %v; want false, nil", matched, err)
	}
}

// Rationale: corrupt durable plaintext is an internal invariant failure, not
// an idempotency mismatch that could invite an unsafe second mutation.
func TestCompareRejectsMalformedProtectedPlaintext(t *testing.T) {
	t.Parallel()

	for _, plaintext := range [][]byte{
		make([]byte, protectedPlaintextBytes-1),
		append([]byte{2}, make([]byte, sha256.Size)...),
	} {
		cipher := &retainingCipher{openResult: plaintext}
		protector := testProtector(t, cipher)
		envelope, err := protector.Seal(context.Background(), []byte("ciphertext source"))
		if err != nil {
			t.Fatalf("protector.Seal() error = %v", err)
		}
		candidate := canonicalTestDigest(t, canonicalTestIntent())
		matched, err := Compare(context.Background(), protector, envelope, Version1, candidate)
		if matched || !hasKind(err, errs.KindInternal) {
			t.Fatalf("Compare() = %v, %v; want false, internal", matched, err)
		}
		if !candidate.state.consumed || candidate.state.value != [sha256.Size]byte{} {
			t.Fatal("Compare() did not clear candidate on corrupt durable state")
		}
	}
}

// Rationale: invalid dependencies, versions, and contexts must still consume
// the one-use digest so no failure path leaves sensitive bytes resident.
func TestProtectAndCompareClearDigestOnEarlyFailure(t *testing.T) {
	t.Parallel()

	protectDigest := canonicalTestDigest(t, canonicalTestIntent())
	if _, err := Protect(nil, nil, 0, protectDigest); !hasKind(err, errs.KindInternal) {
		t.Fatalf("Protect() error = %v, want internal", err)
	}
	if !protectDigest.state.consumed || protectDigest.state.value != [sha256.Size]byte{} {
		t.Fatal("Protect() early failure retained digest")
	}

	compareDigest := canonicalTestDigest(t, canonicalTestIntent())
	if _, err := Compare(nil, nil, secretvalue.Envelope{}, 0, compareDigest); !hasKind(err, errs.KindInternal) {
		t.Fatalf("Compare() error = %v, want internal", err)
	}
	if !compareDigest.state.consumed || compareDigest.state.value != [sha256.Size]byte{} {
		t.Fatal("Compare() early failure retained digest")
	}
}

// Rationale: explicit destruction clears every copied handle and remains safe
// when cleanup paths call it more than once.
func TestDigestDestroyIsSharedAndIdempotent(t *testing.T) {
	t.Parallel()

	digest := canonicalTestDigest(t, canonicalTestIntent())
	copy := *digest
	digest.Destroy()
	copy.Destroy()
	if copy.state.value != [sha256.Size]byte{} || !copy.state.consumed {
		t.Fatal("Destroy() retained copied digest state")
	}
}

// Rationale: copied handles can race in retry cleanup paths, but exactly one
// caller may obtain the digest while every other caller fails closed.
func TestDigestCopiedHandlesHaveOneConcurrentConsumer(t *testing.T) {
	t.Parallel()

	digest := canonicalTestDigest(t, canonicalTestIntent())
	copy := *digest
	results := make(chan error, 2)
	for _, handle := range []*Digest{digest, &copy} {
		go func(candidate *Digest) {
			value, err := candidate.consume()
			clear(value[:])
			results <- err
		}(handle)
	}
	var succeeded, failed int
	for range 2 {
		if err := <-results; err == nil {
			succeeded++
		} else if hasKind(err, errs.KindInternal) {
			failed++
		} else {
			t.Fatalf("consume() error = %v, want internal", err)
		}
	}
	if succeeded != 1 || failed != 1 {
		t.Fatalf("consume outcomes = %d success, %d failure; want one each", succeeded, failed)
	}
}

type retainingCipher struct {
	sealInput  []byte
	openResult []byte
}

func (cipher *retainingCipher) Seal(_ context.Context, plaintext []byte) ([]byte, error) {
	cipher.sealInput = append([]byte(nil), plaintext...)
	return append([]byte("sealed:"), plaintext...), nil
}

func (cipher *retainingCipher) Open(_ context.Context, ciphertext []byte) ([]byte, error) {
	if cipher.openResult != nil {
		return append([]byte(nil), cipher.openResult...), nil
	}
	return append([]byte(nil), ciphertext[len("sealed:"):]...), nil
}

func testProtector(t *testing.T, cipher *retainingCipher) *secretvalue.Protector {
	t.Helper()
	protector, err := secretvalue.NewProtector(cipher, cipher)
	if err != nil {
		t.Fatalf("secretvalue.NewProtector() error = %v", err)
	}
	return protector
}

func testTime(second int) time.Time {
	return time.Date(2026, time.August, 20, 0, 0, second, 0, time.UTC)
}

func hasKind(err error, want errs.Kind) bool {
	kind, ok := errs.KindOf(err)
	return ok && kind == want
}
