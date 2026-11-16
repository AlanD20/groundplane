package idempotency

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
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: exact replay bodies must not leak through Resolution formatting,
// including detailed and Go-syntax verbs.
func TestResolutionFormattingRedactsResponseBody(t *testing.T) {
	t.Parallel()

	resolution := Resolution{
		Kind: ResolutionReplay,
		Response: idempotencyrecord.IdempotencyResponse{
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
	locator := idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopeTenant,
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
	marker := idempotencyrecord.IdempotencyMarker{
		Kind: idempotencyrecord.IdempotencyMarkerDirect, State: idempotencyrecord.IdempotencyMarkerCompleted,
		Intent: durable,
		Response: idempotencyrecord.IdempotencyResponse{
			Status: http.StatusOK, ContentKind: "application/json", Body: []byte(`{"id":"svc"}`),
		},
	}
	resolution, err := coordinator.classifyMarker(context.Background(), protected, marker)
	if err != nil || resolution.Kind != ResolutionReplay || string(resolution.Response.Body) != `{"id":"svc"}` {
		t.Fatalf("ResolveKnown(replay) = %#v, %v", resolution, err)
	}

	marker.Kind = idempotencyrecord.IdempotencyMarkerTask
	marker.State = idempotencyrecord.IdempotencyMarkerPending
	_, err = coordinator.classifyMarker(context.Background(), protected, marker)
	if !hasKind(err, errs.KindIdempotencyInProgress) {
		t.Fatalf("ResolveKnown(pending) error = %v, want in-progress", err)
	}
	resolution, err = coordinator.classifyOperationRootMarker(context.Background(), protected, marker)
	if err != nil || resolution.Kind != ResolutionReplay || string(resolution.Response.Body) != `{"id":"svc"}` {
		t.Fatalf("operation-root pending replay = %#v, %v", resolution, err)
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
	//lint:ignore SA1012 This test verifies fail-closed handling of a nil operation context.
	if _, err := Protect(nil, nil, 0, protectDigest); !hasKind(err, errs.KindInternal) {
		t.Fatalf("Protect() error = %v, want internal", err)
	}
	if !protectDigest.state.consumed || protectDigest.state.value != [sha256.Size]byte{} {
		t.Fatal("Protect() early failure retained digest")
	}

	if _, err := CompareProtected(
		//lint:ignore SA1012 This test verifies fail-closed comparison with a nil operation context.
		nil,
		nil,
		secretvalue.Envelope{},
		secretvalue.Envelope{},
	); !hasKind(
		err,
		errs.KindInternal,
	) {
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
	testkeyvalue.Store

	beforeReturn func()
	getCalls     int
	value        []byte
}

func (store *unknownReadStore) Get(
	ctx context.Context,
	key string,
) (*testkeyvalue.GetResult, error) {
	store.getCalls++
	if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) <= 0 {
		return nil, errs.New(errs.KindInternal, "unknown evidence read is not bounded")
	}
	if store.beforeReturn != nil {
		store.beforeReturn()
	}
	var entry *testkeyvalue.KeyValue
	if store.value != nil {
		entry = &testkeyvalue.KeyValue{Key: key, Value: append([]byte(nil), store.value...), ModRevision: 1}
	}
	return &testkeyvalue.GetResult{ReadRevision: 1, Entry: entry}, nil
}

func (store *unknownReadStore) GetMany(
	ctx context.Context,
	request testkeyvalue.GetManyRequest,
) (*testkeyvalue.GetManyResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	values := make([]*testkeyvalue.KeyValue, len(request.Keys))
	for index, key := range request.Keys {
		if store.value != nil {
			values[index] = &testkeyvalue.KeyValue{Key: key, Value: append([]byte(nil), store.value...), ModRevision: 1}
		}
	}
	return &testkeyvalue.GetManyResult{ReadRevision: request.Revision, ResponseRevision: 1, Values: values}, nil
}

// An unknown final acceptance response may replay its immutable operation root,
// while ordinary repeats still report active work. Every path compares protected
// evidence, performs at most one bounded read, and respects caller cancellation.
func TestOperationRootUnknownReplaysOnlyMatchingBoundedEvidence(t *testing.T) {
	coordinator, err := NewCoordinator(testProtector(t, &retainingCipher{}))
	if err != nil {
		t.Fatal(err)
	}
	version, digest, err := Canonicalize(t.Context(), canonicalTestIntent())
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := coordinator.ProtectIntent(t.Context(), version, digest)
	if err != nil {
		t.Fatal(err)
	}
	defer evidence.Destroy()
	durable, err := evidence.DurableRecord()
	if err != nil {
		t.Fatal(err)
	}
	defer clear(durable.Ciphertext)
	locator := idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopeTenant,
		ScopeID:   ids.NewAt(ids.KindTenant, testTime(1), 1),
		Method:    http.MethodPost,
		Route:     "/test",
		Key:       "operation-root-unknown",
	}
	taskID := ids.NewAt(ids.KindTask, testTime(2), 2)
	marker := idempotencyrecord.IdempotencyMarker{
		Locator:   locator,
		Kind:      idempotencyrecord.IdempotencyMarkerTask,
		State:     idempotencyrecord.IdempotencyMarkerPending,
		Intent:    durable,
		TaskID:    taskID,
		CreatedAt: testTime(2),
		UpdatedAt: testTime(2),
		Response: idempotencyrecord.IdempotencyResponse{
			Status:      202,
			ContentKind: "application/json",
			Body:        []byte(`{"task_id":"` + taskID + `"}`),
		},
	}
	value, err := idempotencyrecord.EncodeIdempotencyMarker(marker)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(value)
	original := errs.New(errs.KindStorageUnavailable, "unknown acceptance outcome")
	for _, scenario := range []string{"immutable root", "ordinary pending", "different intent", "cancelled", "absent", "ineligible"} {
		t.Run(scenario, func(t *testing.T) {
			store := &unknownReadStore{value: value}
			repository, err := infraetcd.NewIdempotencyRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			candidate := evidence
			if scenario == "different intent" {
				changed := canonicalTestIntent()
				changed.Path[0].Value = ids.NewAt(ids.KindService, testTime(4), 4)
				v, d, err := Canonicalize(t.Context(), changed)
				if err != nil {
					t.Fatal(err)
				}
				candidate, err = coordinator.ProtectIntent(t.Context(), v, d)
				if err != nil {
					t.Fatal(err)
				}
				defer candidate.Destroy()
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if scenario == "cancelled" {
				store.beforeReturn = cancel
			}
			if scenario == "absent" {
				store.value = nil
			}
			cause := original
			if scenario == "ineligible" {
				cause = errs.New(errs.KindValidationFailed, "not submitted")
			}
			var result Resolution
			if scenario == "ordinary pending" {
				result, err = coordinator.ResolveUnknown(ctx, repository, locator, candidate, cause)
			} else {
				result, err = coordinator.ResolveOperationRootUnknown(ctx, repository, locator, candidate, cause)
			}
			switch scenario {
			case "immutable root":
				if err != nil || result.Kind != ResolutionReplay ||
					string(result.Response.Body) != string(marker.Response.Body) {
					t.Fatalf("immutable acceptance lost: %#v %v", result, err)
				}
			case "ordinary pending":
				if !hasKind(err, errs.KindIdempotencyInProgress) {
					t.Fatalf("ordinary pending semantics changed: %v", err)
				}
			case "different intent":
				if !hasKind(err, errs.KindIdempotencyMismatch) {
					t.Fatalf("different request replayed: %v", err)
				}
			case "cancelled":
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("caller cancellation lost: %v", err)
				}
			case "absent":
				if !errors.Is(err, original) {
					t.Fatalf("unknown missing outcome replaced: %v", err)
				}
			case "ineligible":
				if !hasKind(err, errs.KindInternal) {
					t.Fatalf("ineligible latest reread allowed: %v", err)
				}
			}
			expectedReads := 1
			if scenario == "ineligible" {
				expectedReads = 0
			}
			if store.getCalls != expectedReads {
				t.Fatalf("read count=%d, want %d", store.getCalls, expectedReads)
			}
		})
	}
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
