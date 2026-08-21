package idempotentintent

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	infraetcd "github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: exact replay bodies must not leak through Resolution formatting,
// including detailed and Go-syntax verbs.
func TestResolutionFormattingRedactsResponseBody(t *testing.T) {
	t.Parallel()

	resolution := Resolution{
		Kind: ResolutionReplay,
		Response: infraetcd.IdempotencyResponse{
			Status: 200, ContentKind: "application/json", Body: []byte(`{"token":"resolution-secret"}`),
		},
	}
	formatted := fmt.Sprintf("%v|%+v|%#v", resolution, &resolution, resolution)
	if strings.Contains(formatted, "resolution-secret") || !strings.Contains(formatted, "response:redacted") {
		t.Fatalf("formatted resolution leaked response: %s", formatted)
	}
}

// Rationale: unknown-outcome reconciliation is limited to retryable/deadline
// transaction results, and a real caller cancellation after the bounded read
// wins over both stored evidence and the original backend result.
func TestResolveUnknownEligibilityAndCallerCancellation(t *testing.T) {
	t.Parallel()

	protector := testProtector(t, &retainingCipher{})
	coordinator, err := NewCoordinator(protector)
	if err != nil {
		t.Fatalf("NewCoordinator() error = %v", err)
	}
	locator := infraetcd.IdempotencyLocator{
		ScopeKind: infraetcd.IdempotencyScopeTenant,
		ScopeID:   ids.NewAt(ids.KindTenant, testTime(1), 1),
		Method:    http.MethodPatch, Route: "/projects/{id}", Key: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
	}
	storageOutcome := errs.New(errs.KindStorageUnavailable, "transaction outcome is unknown")

	ctx, cancel := context.WithCancel(context.Background())
	store := &unknownReadStore{beforeReturn: cancel}
	repository, err := infraetcd.NewIdempotencyRepository(store)
	if err != nil {
		t.Fatalf("NewIdempotencyRepository(cancel) error = %v", err)
	}
	_, err = coordinator.ResolveUnknown(ctx, repository, locator, ProtectedEvidence{}, storageOutcome)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ResolveUnknown(canceled) error = %v, want context canceled", err)
	}

	ineligibleStore := &unknownReadStore{}
	repository, err = infraetcd.NewIdempotencyRepository(ineligibleStore)
	if err != nil {
		t.Fatalf("NewIdempotencyRepository(ineligible) error = %v", err)
	}
	_, err = coordinator.ResolveUnknown(
		context.Background(), repository, locator, ProtectedEvidence{},
		errs.New(errs.KindValidationFailed, "not submitted"),
	)
	if !hasKind(err, errs.KindInternal) || ineligibleStore.getCalls != 0 {
		t.Fatalf("ResolveUnknown(ineligible) error/calls = %v/%d", err, ineligibleStore.getCalls)
	}

	missingStore := &unknownReadStore{}
	repository, err = infraetcd.NewIdempotencyRepository(missingStore)
	if err != nil {
		t.Fatalf("NewIdempotencyRepository(missing) error = %v", err)
	}
	_, err = coordinator.ResolveUnknown(
		context.Background(), repository, locator, ProtectedEvidence{}, storageOutcome,
	)
	if !errors.Is(err, storageOutcome) || missingStore.getCalls != 1 {
		t.Fatalf("ResolveUnknown(missing) error/calls = %v/%d", err, missingStore.getCalls)
	}
}

// Rationale: replay classification must use protected-to-protected comparison
// and distinguish terminal replay, active Task, and valid intent mismatch.
func TestCoordinatorClassifiesKnownTransactionEvidence(t *testing.T) {
	t.Parallel()

	protector := testProtector(t, &retainingCipher{})
	coordinator, err := NewCoordinator(protector)
	if err != nil {
		t.Fatalf("NewCoordinator() error = %v", err)
	}
	version, digest, err := Canonicalize(context.Background(), canonicalTestIntent())
	if err != nil {
		t.Fatalf("Canonicalize() error = %v", err)
	}
	protected, err := coordinator.ProtectIntent(context.Background(), version, digest)
	if err != nil {
		t.Fatalf("ProtectIntent() error = %v", err)
	}
	durable, err := protected.DurableRecord()
	if err != nil {
		t.Fatalf("DurableRecord() error = %v", err)
	}
	marker := infraetcd.IdempotencyMarker{
		Kind: infraetcd.IdempotencyMarkerDirect, State: infraetcd.IdempotencyMarkerCompleted,
		Intent: durable,
		Response: infraetcd.IdempotencyResponse{
			Status: http.StatusOK, ContentKind: "application/json", Body: []byte(`{"id":"svc"}`),
		},
	}
	resolution, err := coordinator.classifyMarker(context.Background(), protected, marker)
	if err != nil || resolution.Kind != ResolutionReplay || string(resolution.Response.Body) != `{"id":"svc"}` {
		t.Fatalf("ResolveKnown(replay) = %#v, %v", resolution, err)
	}

	marker.Kind = infraetcd.IdempotencyMarkerTask
	marker.State = infraetcd.IdempotencyMarkerPending
	_, err = coordinator.classifyMarker(context.Background(), protected, marker)
	if !hasKind(err, errs.KindIdempotencyInProgress) {
		t.Fatalf("ResolveKnown(pending) error = %v, want in-progress", err)
	}

	changed := canonicalTestIntent()
	changed.Path[0].Value = ids.NewAt(ids.KindService, testTime(4), 4)
	changedVersion, changedDigest, err := Canonicalize(context.Background(), changed)
	if err != nil {
		t.Fatalf("Canonicalize(changed) error = %v", err)
	}
	changedProtected, err := coordinator.ProtectIntent(context.Background(), changedVersion, changedDigest)
	if err != nil {
		t.Fatalf("ProtectIntent(changed) error = %v", err)
	}
	_, err = coordinator.classifyMarker(context.Background(), changedProtected, marker)
	if !hasKind(err, errs.KindIdempotencyMismatch) {
		t.Fatalf("ResolveKnown(mismatch) error = %v, want mismatch", err)
	}
	if _, err := coordinator.ResolveKnown(
		context.Background(),
		protected,
		infraetcd.IdempotencyTransactionResult{},
	); !hasKind(err, errs.KindInternal) {
		t.Fatalf("ResolveKnown(zero evidence) error = %v, want internal", err)
	}
}

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

// Rationale: valid protected duplicates match and changed valid protected
// intents mismatch without retaining either plaintext digest.
func TestCompareProtectedDistinguishesMismatch(t *testing.T) {
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

	equalDigest := canonicalTestDigest(t, canonicalTestIntent())
	equal, err := Protect(context.Background(), protector, Version1, equalDigest)
	if err != nil {
		t.Fatalf("Protect(equal) error = %v", err)
	}
	matched, err := CompareProtected(context.Background(), protector, envelope, equal)
	if err != nil || !matched {
		t.Fatalf("CompareProtected(equal) = %v, %v; want true, nil", matched, err)
	}

	changedIntent := canonicalTestIntent()
	changedIntent.Path[0].Value = ids.NewAt(ids.KindService, testTime(4), 4)
	changedDigest := canonicalTestDigest(t, changedIntent)
	changed, err := Protect(context.Background(), protector, Version1, changedDigest)
	if err != nil {
		t.Fatalf("Protect(changed) error = %v", err)
	}
	matched, err = CompareProtected(context.Background(), protector, envelope, changed)
	if err != nil || matched {
		t.Fatalf("CompareProtected(changed) = %v, %v; want false, nil", matched, err)
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
		candidateCipher := &retainingCipher{}
		candidateProtector := testProtector(t, candidateCipher)
		candidateDigest := canonicalTestDigest(t, canonicalTestIntent())
		candidate, err := Protect(context.Background(), candidateProtector, Version1, candidateDigest)
		if err != nil {
			t.Fatalf("Protect(candidate) error = %v", err)
		}
		matched, err := CompareProtected(context.Background(), protector, envelope, candidate)
		if matched || !hasKind(err, errs.KindInternal) {
			t.Fatalf("CompareProtected() = %v, %v; want false, internal", matched, err)
		}
	}
}

// Rationale: invalid dependencies, versions, and contexts must still consume
// the one-use digest, while protected comparison fails closed.
func TestProtectAndCompareProtectedFailClosed(t *testing.T) {
	t.Parallel()

	protectDigest := canonicalTestDigest(t, canonicalTestIntent())
	if _, err := Protect(nil, nil, 0, protectDigest); !hasKind(err, errs.KindInternal) {
		t.Fatalf("Protect() error = %v, want internal", err)
	}
	if !protectDigest.state.consumed || protectDigest.state.value != [sha256.Size]byte{} {
		t.Fatal("Protect() early failure retained digest")
	}

	if _, err := CompareProtected(nil, nil, secretvalue.Envelope{}, secretvalue.Envelope{}); !hasKind(err, errs.KindInternal) {
		t.Fatalf("CompareProtected() error = %v, want internal", err)
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

type unknownReadStore struct {
	infraetcd.Store
	beforeReturn func()
	getCalls     int
}

func (store *unknownReadStore) Get(
	ctx context.Context,
	_ string,
) (*infraetcd.GetResult, error) {
	store.getCalls++
	if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) <= 0 {
		return nil, errs.New(errs.KindInternal, "unknown evidence read is not bounded")
	}
	if store.beforeReturn != nil {
		store.beforeReturn()
	}
	return &infraetcd.GetResult{ReadRevision: 1}, nil
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
