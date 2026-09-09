package etcd

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type collectorSnapshot struct {
	rangeResult RangeResult
	markers     GetManyResult
}

type collectorTestStore struct {
	Store
	mu                 sync.Mutex
	snapshots          []collectorSnapshot
	transactionResults []TransactionResult
	transactionErrors  []error
	rangeCalls         int
	getManyCalls       int
	transactionCalls   int
	getManyRevisions   []int64
}

func (store *collectorTestStore) Range(ctx context.Context, request RangeRequest) (*RangeResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Prefix != idempotencyRetentionPrefix || request.Limit != maximumPruneMarkers || request.Revision != 0 ||
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
	request GetManyRequest,
) (*GetManyResult, error) {
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
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return TransactionResult{}, err
	}
	index := store.transactionCalls
	store.transactionCalls++
	if len(conditions) != 2 || len(mutations) != 2 {
		return TransactionResult{}, errs.New(errs.KindInternal, "unexpected collector transaction")
	}
	if index < len(store.transactionErrors) && store.transactionErrors[index] != nil {
		return TransactionResult{}, store.transactionErrors[index]
	}
	if index >= len(store.transactionResults) {
		return TransactionResult{}, errs.New(errs.KindInternal, "missing collector transaction result")
	}
	return store.transactionResults[index], nil
}

func testCollectorSnapshot(
	t *testing.T,
	marker IdempotencyMarker,
	revision int64,
	retentionRevision int64,
	markerRevision int64,
) collectorSnapshot {
	t.Helper()
	markerKey, err := idempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatalf("idempotencyMarkerKey() error = %v", err)
	}
	retentionKey, err := idempotencyRetentionKey(markerKey, marker.RetainUntil)
	if err != nil {
		t.Fatalf("idempotencyRetentionKey() error = %v", err)
	}
	retentionValue, err := json.Marshal(retentionReferenceJSON{Schema: 1, MarkerKey: markerKey})
	if err != nil {
		t.Fatalf("json.Marshal(retention) error = %v", err)
	}
	markerValue, err := encodeIdempotencyMarker(marker)
	if err != nil {
		t.Fatalf("encodeIdempotencyMarker() error = %v", err)
	}
	return collectorSnapshot{
		rangeResult: RangeResult{
			Values:       []KeyValue{{Key: retentionKey, Value: retentionValue, ModRevision: retentionRevision}},
			ReadRevision: revision, ResponseRevision: revision,
		},
		markers: GetManyResult{
			Values:       []*KeyValue{{Key: markerKey, Value: markerValue, ModRevision: markerRevision}},
			ReadRevision: revision, ResponseRevision: revision,
		},
	}
}

func (store *collectorTestStore) Get(ctx context.Context, key string) (*GetResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if key != idempotencyPruneCursorKey || store.rangeCalls >= len(store.snapshots) {
		return nil, errs.New(errs.KindInternal, "unexpected collector cursor read")
	}
	return &GetResult{ReadRevision: store.snapshots[store.rangeCalls].rangeResult.ReadRevision}, nil
}

func SeedIndependentPruneMarker(t *testing.T, backend Store, at time.Time) string {
	t.Helper()
	marker := testDirectMarker()
	marker.Locator.Key = "volume-retention-independent-0001"
	marker.CreatedAt, marker.UpdatedAt, marker.TerminalAt = at, at, at
	marker.RetainUntil = at.Add(markerRetention)
	key, err := idempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatal(err)
	}
	value, err := encodeIdempotencyMarker(marker)
	if err != nil {
		t.Fatal(err)
	}
	retentionKey, err := idempotencyRetentionKey(key, marker.RetainUntil)
	if err != nil {
		t.Fatal(err)
	}
	retention, err := json.Marshal(retentionReferenceJSON{Schema: 1, MarkerKey: key})
	if err != nil {
		t.Fatal(err)
	}
	result, err := backend.Transact(
		context.Background(),
		[]Condition{{Key: key}, {Key: retentionKey}},
		[]Mutation{
			{Type: MutationPut, Key: key, Value: value},
			{Type: MutationPut, Key: retentionKey, Value: retention},
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
		marker.ReplayTarget = &IdempotencyReplayTarget{Kind: IdempotencyReplayTargetVolume, ID: task.Target}
		marker.Response.Body = []byte(`{"task_id":"` + task.ID + `"}`)
		marker.State, marker.UpdatedAt, marker.TerminalAt = IdempotencyMarkerCompleted, *task.FinishedAt, *task.FinishedAt
		marker.RetainUntil = *task.RetainUntil
		task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
		key, err := idempotencyMarkerKey(marker.Locator)
		if err != nil {
			t.Fatal(err)
		}
		taskValue, err := encodeTaskRecord(task)
		if err != nil {
			t.Fatal(err)
		}
		markerValue, err := encodeIdempotencyMarker(marker)
		if err != nil {
			t.Fatal(err)
		}
		retentionKey, err := idempotencyRetentionKey(key, marker.RetainUntil)
		if err != nil {
			t.Fatal(err)
		}
		retentionValue, err := json.Marshal(retentionReferenceJSON{Schema: 1, MarkerKey: key})
		if err != nil {
			t.Fatal(err)
		}
		replayKey, err := idempotencyReplayTargetKey(
			*marker.ReplayTarget,
			marker.Locator.Method,
			marker.Locator.Route,
			marker.Locator.Key,
		)
		if err != nil {
			t.Fatal(err)
		}
		replayValue, err := encodeReplayTargetReference(key)
		if err != nil {
			t.Fatal(err)
		}
		result, err := fixture.Store.Transact(context.Background(), nil, []Mutation{
			{Type: MutationPut, Key: taskKey(task.ID), Value: taskValue},
			{Type: MutationPut, Key: key, Value: markerValue},
			{Type: MutationPut, Key: retentionKey, Value: retentionValue},
			{Type: MutationPut, Key: replayKey, Value: replayValue},
		})
		if err != nil || !result.Succeeded {
			t.Fatalf("seed completed removal marker: %v", err)
		}
		keys = append(keys, key)
	}
	return keys
}
