package etcd

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

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
	if request.Prefix != testidempotency.IdempotencyRetentionPrefix || request.Limit != maximumPruneMarkers ||
		request.Revision != 0 ||
		store.rangeCalls >= len(store.snapshots) {
		return nil, errs.New(errs.KindInternal, "unexpected collector range")
	}
	snapshot := store.snapshots[store.rangeCalls]
	store.rangeCalls++
	result := snapshot.rangeResult
	result.Values = cloneKeyValueSlice(result.Values)
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
	result.Values = cloneKeyValuePointers(result.Values)
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

func testCollectorSnapshot(
	t *testing.T,
	marker testidempotency.IdempotencyMarker,
	revision int64,
	retentionRevision int64,
	markerRevision int64,
) collectorSnapshot {
	t.Helper()
	markerKey, err := testidempotency.IdempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatalf("idempotencyMarkerKey() error = %v", err)
	}
	retentionKey, err := testidempotency.IdempotencyRetentionKey(markerKey, marker.RetainUntil)
	if err != nil {
		t.Fatalf("idempotencyRetentionKey() error = %v", err)
	}
	retentionValue, err := json.Marshal(testidempotency.RetentionReferenceJSON{Schema: 1, MarkerKey: markerKey})
	if err != nil {
		t.Fatalf("json.Marshal(retention) error = %v", err)
	}
	markerValue, err := testidempotency.EncodeIdempotencyMarker(marker)
	if err != nil {
		t.Fatalf("encodeIdempotencyMarker() error = %v", err)
	}
	return collectorSnapshot{
		rangeResult: testkeyvalue.RangeResult{
			Values: []testkeyvalue.KeyValue{
				{Key: retentionKey, Value: retentionValue, ModRevision: retentionRevision},
			},
			ReadRevision: revision, ResponseRevision: revision,
		},
		markers: testkeyvalue.GetManyResult{
			Values:       []*testkeyvalue.KeyValue{{Key: markerKey, Value: markerValue, ModRevision: markerRevision}},
			ReadRevision: revision, ResponseRevision: revision,
		},
	}
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

func SeedIndependentPruneMarker(t *testing.T, backend testkeyvalue.Store, at time.Time) string {
	t.Helper()
	marker := testDirectMarker()
	marker.Locator.Key = "volume-retention-independent-0001"
	marker.CreatedAt, marker.UpdatedAt, marker.TerminalAt = at, at, at
	marker.RetainUntil = at.Add(testidempotency.MarkerRetention)
	key, err := testidempotency.IdempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatal(err)
	}
	value, err := testidempotency.EncodeIdempotencyMarker(marker)
	if err != nil {
		t.Fatal(err)
	}
	retentionKey, err := testidempotency.IdempotencyRetentionKey(key, marker.RetainUntil)
	if err != nil {
		t.Fatal(err)
	}
	retention, err := json.Marshal(testidempotency.RetentionReferenceJSON{Schema: 1, MarkerKey: key})
	if err != nil {
		t.Fatal(err)
	}
	result, err := backend.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{Key: key}, {Key: retentionKey}},
		[]testkeyvalue.Mutation{
			{Type: testkeyvalue.MutationPut, Key: key, Value: value},
			{Type: testkeyvalue.MutationPut, Key: retentionKey, Value: retention},
		},
	)
	if err != nil || !result.Succeeded {
		t.Fatalf("seed independent marker: %v", err)
	}
	return key
}

func SeedCompletedVolumePruneBatch(t *testing.T, fixture *VolumePolicyDesiredFixture, completed TaskRecord) []string {
	t.Helper()
	keys := []string{}
	for index := range maximumPruneMarkers {
		task := cloneTaskRecord(completed)
		task.ID = ids.NewAt(ids.KindTask, task.CreatedAt, int64(index+400))
		task.OperationID = ids.NewAt(ids.KindOperation, task.CreatedAt, int64(index+400))
		task.Target = ids.NewAt(ids.KindVolume, task.CreatedAt, int64(index+400))
		task.IdempotencyKey = "volume-expired-batch-" + strconv.Itoa(index)
		task.Params[removalrecord.OriginTaskParam] = task.ID
		marker := fixture.Marker
		marker.Locator.Key, marker.TaskID = task.IdempotencyKey, task.ID
		marker.ReplayTarget = &testidempotency.IdempotencyReplayTarget{
			Kind: testidempotency.IdempotencyReplayTargetVolume,
			ID:   task.Target,
		}
		marker.Response.Body = []byte(`{"task_id":"` + task.ID + `"}`)
		marker.State, marker.UpdatedAt, marker.TerminalAt = testidempotency.IdempotencyMarkerCompleted, *task.FinishedAt, *task.FinishedAt
		marker.RetainUntil = *task.RetainUntil
		task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
		key, err := testidempotency.IdempotencyMarkerKey(marker.Locator)
		if err != nil {
			t.Fatal(err)
		}
		taskValue, err := EncodeTaskRecord(task)
		if err != nil {
			t.Fatal(err)
		}
		markerValue, err := testidempotency.EncodeIdempotencyMarker(marker)
		if err != nil {
			t.Fatal(err)
		}
		retentionKey, err := testidempotency.IdempotencyRetentionKey(key, marker.RetainUntil)
		if err != nil {
			t.Fatal(err)
		}
		retentionValue, err := json.Marshal(testidempotency.RetentionReferenceJSON{Schema: 1, MarkerKey: key})
		if err != nil {
			t.Fatal(err)
		}
		replayKey, err := testidempotency.IdempotencyReplayTargetKey(
			*marker.ReplayTarget,
			marker.Locator.Method,
			marker.Locator.Route,
			marker.Locator.Key,
		)
		if err != nil {
			t.Fatal(err)
		}
		replayValue, err := testidempotency.EncodeReplayTargetReference(key)
		if err != nil {
			t.Fatal(err)
		}
		result, err := fixture.Store.Transact(context.Background(), nil, []testkeyvalue.Mutation{
			{Type: testkeyvalue.MutationPut, Key: testtaskjournal.TaskStorageKey(task.ID), Value: taskValue},
			{Type: testkeyvalue.MutationPut, Key: key, Value: markerValue},
			{Type: testkeyvalue.MutationPut, Key: retentionKey, Value: retentionValue},
			{Type: testkeyvalue.MutationPut, Key: replayKey, Value: replayValue},
		})
		if err != nil || !result.Succeeded {
			t.Fatalf("seed completed removal marker: %v", err)
		}
		keys = append(keys, key)
	}
	return keys
}
