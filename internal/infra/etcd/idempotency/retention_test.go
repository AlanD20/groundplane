package idempotency

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type retentionTransactionStore struct {
	testkeyvalue.Store
	result     testkeyvalue.TransactionResult
	err        error
	conditions []testkeyvalue.Condition
	mutations  []testkeyvalue.Mutation
}

func (store *retentionTransactionStore) Transact(
	_ context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	store.conditions = append([]testkeyvalue.Condition(nil), conditions...)
	store.mutations = append([]testkeyvalue.Mutation(nil), mutations...)
	return store.result, store.err
}

// Rationale: pruning a deletion marker must validate marker, retention, and replay-target counterparts while
// fitting exactly 16 triples into 48 compares plus 48 deletes.
func TestIdempotencyRepositoryPrunesAtMost16ValidatedTriples(t *testing.T) {
	t.Parallel()

	backend := &retentionTransactionStore{result: testkeyvalue.TransactionResult{Succeeded: true, Revision: 80}}
	repository := newTestRetentionRepository(t, backend)
	candidates := make([]pruneCandidate, maximumPruneMarkers)
	for index := range candidates {
		marker := testDirectMarker()
		marker.Locator.Key = "01ARZ3NDEKTSV4RRFFQ69G5" + string(rune('A'+index))
		marker.ReplayTarget = &IdempotencyReplayTarget{
			Kind: IdempotencyReplayTargetAttach,
			ID:   ids.NewAt(ids.KindAttach, marker.CreatedAt, int64(index+30)),
		}
		markerKey, keyErr := IdempotencyMarkerKey(marker.Locator)
		if keyErr != nil {
			t.Fatalf("idempotencyMarkerKey(%d) error = %v", index, keyErr)
		}
		retentionKey, keyErr := IdempotencyRetentionKey(markerKey, marker.RetainUntil)
		if keyErr != nil {
			t.Fatalf("idempotencyRetentionKey(%d) error = %v", index, keyErr)
		}
		retentionValue, marshalErr := json.Marshal(RetentionReferenceJSON{Schema: 1, MarkerKey: markerKey})
		if marshalErr != nil {
			t.Fatalf("json.Marshal(retention %d) error = %v", index, marshalErr)
		}
		targetKey, keyErr := IdempotencyReplayTargetKey(
			*marker.ReplayTarget, marker.Locator.Method, marker.Locator.Route, marker.Locator.Key,
		)
		if keyErr != nil {
			t.Fatalf("idempotencyReplayTargetKey(%d) error = %v", index, keyErr)
		}
		targetValue, marshalErr := EncodeReplayTargetReference(markerKey)
		if marshalErr != nil {
			t.Fatalf("encodeReplayTargetReference(%d) error = %v", index, marshalErr)
		}
		candidates[index] = pruneCandidate{
			Marker: marker, MarkerModRevision: int64(index + 1),
			RetentionKey: retentionKey, RetentionValue: retentionValue,
			RetentionModRevision: int64(index + 101),
			ReplayTargetKey:      targetKey, ReplayTargetValue: targetValue,
			ReplayTargetModRevision: int64(index + 201),
		}
	}
	revision, err := repository.pruneExpired(
		context.Background(),
		testDirectMarker().RetainUntil.Add(time.Nanosecond),
		candidates,
	)
	if err != nil || revision != 80 {
		t.Fatalf("pruneExpired(16) = %d, %v", revision, err)
	}
	if len(backend.conditions) != 48 || len(backend.mutations) != 48 {
		t.Fatalf(
			"prune transaction conditions/operations = %d/%d",
			len(backend.conditions),
			len(backend.mutations),
		)
	}
	for _, mutation := range backend.mutations {
		if mutation.Type != testkeyvalue.MutationDelete {
			t.Fatal("prune transaction contains a non-delete operation")
		}
	}

	backend.conditions, backend.mutations = nil, nil
	tooMany := append(append([]pruneCandidate(nil), candidates...), candidates[0])
	if _, err := repository.pruneExpired(
		context.Background(),
		testMarkerTime(),
		tooMany,
	); !isKind(err, errs.KindValidationFailed) {
		t.Fatalf("pruneExpired(17) error = %v, want validation", err)
	}
	if backend.conditions != nil || backend.mutations != nil {
		t.Fatal("pruneExpired(17) reached etcd")
	}

	malformed := append([]pruneCandidate(nil), candidates...)
	malformed[0].RetentionValue = nil
	if _, err := repository.pruneExpired(
		context.Background(),
		testDirectMarker().RetainUntil.Add(time.Nanosecond),
		malformed,
	); !isKind(err, errs.KindInternal) {
		t.Fatalf("pruneExpired(missing counterpart) error = %v, want internal", err)
	}
	if backend.conditions != nil || backend.mutations != nil {
		t.Fatal("pruneExpired(missing counterpart) reached etcd")
	}
}

// Rationale: a pruning transaction error has unknown commit status and must
// propagate unchanged rather than being followed by a speculative read.
func TestIdempotencyPrunePropagatesUnknownOutcome(t *testing.T) {
	t.Parallel()

	backendError := errors.New("private backend detail")
	backend := &retentionTransactionStore{err: errs.Wrap(errs.KindStorageUnavailable, backendError)}
	repository := newTestRetentionRepository(t, backend)
	marker := testDirectMarker()
	markerKey, err := IdempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatalf("idempotencyMarkerKey() error = %v", err)
	}
	retentionKey, err := IdempotencyRetentionKey(markerKey, marker.RetainUntil)
	if err != nil {
		t.Fatalf("idempotencyRetentionKey() error = %v", err)
	}
	retentionValue, err := json.Marshal(RetentionReferenceJSON{Schema: 1, MarkerKey: markerKey})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	_, err = repository.pruneExpired(context.Background(), marker.RetainUntil, []pruneCandidate{{
		Marker: marker, MarkerModRevision: 7,
		RetentionKey: retentionKey, RetentionValue: retentionValue, RetentionModRevision: 8,
	}})
	if !isKind(err, errs.KindStorageUnavailable) || !errors.Is(err, backendError) {
		t.Fatalf("pruneExpired() error = %v", err)
	}
}

type collectorSnapshot struct {
	rangeResult testkeyvalue.RangeResult
	markers     testkeyvalue.GetManyResult
}

type collectorTestStore struct {
	testkeyvalue.Store

	mu                 sync.Mutex
	snapshots          []collectorSnapshot
	transactionResults []testkeyvalue.TransactionResult
	transactionErrors  []error
	rangeCalls         int
	getManyCalls       int
	transactionCalls   int
	getManyRevisions   []int64
}

func (store *collectorTestStore) Range(
	ctx context.Context,
	request testkeyvalue.RangeRequest,
) (*testkeyvalue.RangeResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Prefix != IdempotencyRetentionPrefix || request.Limit != maximumPruneMarkers ||
		request.Revision != 0 || store.rangeCalls >= len(store.snapshots) {
		return nil, errs.New(errs.KindInternal, "unexpected collector range")
	}
	snapshot := store.snapshots[store.rangeCalls]
	store.rangeCalls++
	result := snapshot.rangeResult
	result.Values = cloneRetentionKeyValueSlice(result.Values)
	return &result, nil
}

func (store *collectorTestStore) GetMany(
	ctx context.Context,
	request testkeyvalue.GetManyRequest,
) (*testkeyvalue.GetManyResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if store.getManyCalls >= len(store.snapshots) ||
		request.Revision != store.snapshots[store.getManyCalls].rangeResult.ReadRevision {
		return nil, errs.New(errs.KindInternal, "unexpected collector multi-get")
	}
	snapshot := store.snapshots[store.getManyCalls]
	store.getManyCalls++
	store.getManyRevisions = append(store.getManyRevisions, request.Revision)
	result := snapshot.markers
	result.Values = cloneRetentionKeyValuePointers(result.Values)
	return &result, nil
}

func (store *collectorTestStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return testkeyvalue.TransactionResult{}, err
	}
	index := store.transactionCalls
	store.transactionCalls++
	if len(conditions) != 2 || len(mutations) != 2 {
		return testkeyvalue.TransactionResult{}, errs.New(errs.KindInternal, "unexpected collector transaction")
	}
	if index < len(store.transactionErrors) && store.transactionErrors[index] != nil {
		return testkeyvalue.TransactionResult{}, store.transactionErrors[index]
	}
	if index >= len(store.transactionResults) {
		return testkeyvalue.TransactionResult{}, errs.New(errs.KindInternal, "missing collector transaction result")
	}
	return store.transactionResults[index], nil
}

func (store *collectorTestStore) Get(ctx context.Context, key string) (*testkeyvalue.GetResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if key != idempotencyPruneCursorKey || store.rangeCalls >= len(store.snapshots) {
		return nil, errs.New(errs.KindInternal, "unexpected collector cursor read")
	}
	return &testkeyvalue.GetResult{ReadRevision: store.snapshots[store.rangeCalls].rangeResult.ReadRevision}, nil
}

func testCollectorSnapshot(
	t *testing.T,
	marker IdempotencyMarker,
	revision int64,
	retentionRevision int64,
	markerRevision int64,
) collectorSnapshot {
	t.Helper()
	markerKey, err := IdempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatalf("idempotencyMarkerKey() error = %v", err)
	}
	retentionKey, err := IdempotencyRetentionKey(markerKey, marker.RetainUntil)
	if err != nil {
		t.Fatalf("idempotencyRetentionKey() error = %v", err)
	}
	retentionValue, err := json.Marshal(RetentionReferenceJSON{Schema: 1, MarkerKey: markerKey})
	if err != nil {
		t.Fatalf("json.Marshal(retention) error = %v", err)
	}
	markerValue, err := EncodeIdempotencyMarker(marker)
	if err != nil {
		t.Fatalf("encodeIdempotencyMarker() error = %v", err)
	}
	return collectorSnapshot{
		rangeResult: testkeyvalue.RangeResult{
			Values: []testkeyvalue.KeyValue{{
				Key: retentionKey, Value: retentionValue, ModRevision: retentionRevision,
			}},
			ReadRevision: revision, ResponseRevision: revision,
		},
		markers: testkeyvalue.GetManyResult{
			Values: []*testkeyvalue.KeyValue{{
				Key: markerKey, Value: markerValue, ModRevision: markerRevision,
			}},
			ReadRevision: revision, ResponseRevision: revision,
		},
	}
}

func newTestRetentionRepository(t *testing.T, store retentionStore) *RetentionRepository {
	t.Helper()
	repository, err := NewRetentionRepository(
		store,
		func(context.Context, PruneGuardInput) (PruneGuardResult, error) {
			return PruneGuardResult{}, nil
		},
	)
	if err != nil {
		t.Fatalf("NewRetentionRepository() error = %v", err)
	}
	return repository
}

// Rationale: daily collection must hydrate both counterparts at one fixed
// revision and retry a known CAS miss only after taking a fresh snapshot.
func TestIdempotencyPruneExpiredUsesFreshFixedRevisionAfterCASConflict(t *testing.T) {
	t.Parallel()

	marker := testDirectMarker()
	first := testCollectorSnapshot(t, marker, 51, 41, 42)
	second := testCollectorSnapshot(t, marker, 52, 43, 44)
	store := &collectorTestStore{
		snapshots: []collectorSnapshot{first, second},
		transactionResults: []testkeyvalue.TransactionResult{
			{Revision: 60},
			{Succeeded: true, Revision: 61},
		},
	}
	repository := newTestRetentionRepository(t, store)
	count, err := repository.PruneExpired(context.Background(), marker.RetainUntil.Add(time.Nanosecond))
	if err != nil || count != 1 {
		t.Fatalf("PruneExpired() = %d, %v", count, err)
	}
	if store.rangeCalls != 2 || store.getManyCalls != 2 || store.transactionCalls != 2 ||
		store.getManyRevisions[0] != 51 || store.getManyRevisions[1] != 52 {
		t.Fatalf(
			"collector calls/revisions = %d/%d/%d %#v",
			store.rangeCalls,
			store.getManyCalls,
			store.transactionCalls,
			store.getManyRevisions,
		)
	}
}

// Rationale: a missing marker counterpart is corruption before any prune
// write, while an unknown transaction outcome is returned without a retry.
func TestIdempotencyPruneExpiredAbortsMissingAndUnknownEvidence(t *testing.T) {
	t.Parallel()

	marker := testDirectMarker()
	snapshot := testCollectorSnapshot(t, marker, 51, 41, 42)
	snapshot.markers.Values[0] = nil
	missingStore := &collectorTestStore{snapshots: []collectorSnapshot{snapshot}}
	repository := newTestRetentionRepository(t, missingStore)
	if _, err := repository.PruneExpired(
		context.Background(), marker.RetainUntil,
	); !isKind(err, errs.KindInternal) {
		t.Fatalf("PruneExpired(missing) error = %v, want internal", err)
	}
	if missingStore.transactionCalls != 0 {
		t.Fatal("PruneExpired(missing) reached transaction")
	}

	backendError := errs.New(errs.KindStorageUnavailable, "unknown transaction outcome")
	unknownStore := &collectorTestStore{
		snapshots:         []collectorSnapshot{testCollectorSnapshot(t, marker, 53, 45, 46)},
		transactionErrors: []error{backendError},
	}
	repository = newTestRetentionRepository(t, unknownStore)
	if _, err := repository.PruneExpired(
		context.Background(), marker.RetainUntil,
	); !errors.Is(err, backendError) {
		t.Fatalf("PruneExpired(unknown) error = %v, want original", err)
	}
	if unknownStore.rangeCalls != 1 || unknownStore.transactionCalls != 1 {
		t.Fatalf("PruneExpired(unknown) calls = %d/%d", unknownStore.rangeCalls, unknownStore.transactionCalls)
	}
}

func cloneRetentionKeyValueSlice(values []testkeyvalue.KeyValue) []testkeyvalue.KeyValue {
	result := make([]testkeyvalue.KeyValue, len(values))
	for index, value := range values {
		result[index] = testkeyvalue.KeyValue{
			Key: value.Key, Value: append([]byte(nil), value.Value...), ModRevision: value.ModRevision,
		}
	}
	return result
}

func cloneRetentionKeyValuePointers(values []*testkeyvalue.KeyValue) []*testkeyvalue.KeyValue {
	result := make([]*testkeyvalue.KeyValue, len(values))
	for index, value := range values {
		if value != nil {
			result[index] = &testkeyvalue.KeyValue{
				Key: value.Key, Value: append([]byte(nil), value.Value...), ModRevision: value.ModRevision,
			}
		}
	}
	return result
}
