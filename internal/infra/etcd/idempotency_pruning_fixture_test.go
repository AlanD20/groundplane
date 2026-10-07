package etcd

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
)

func NewTestIdempotencyRetention(
	t testing.TB,
	backend idempotencyRepositoryStore,
) *testidempotency.RetentionRepository {
	t.Helper()
	repository, err := NewVolumeRemovalAwareIdempotencyRetention(backend)
	if err != nil {
		t.Fatalf("initialize idempotency retention: %v", err)
	}
	return repository
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
	for index := range 16 {
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
