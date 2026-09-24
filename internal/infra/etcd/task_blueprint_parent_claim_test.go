package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// Rationale: a Blueprint parent must leave the Agent/native queues atomically
// and remain discoverable after Controller restart, without a child assignment.
func TestBlueprintParentClaimSurvivesRestartWithoutAgentAssignment(t *testing.T) {
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	now := taskJournalTime()
	task := validTaskRecord(now)
	task.Executor = testtaskjournal.TaskExecutorBlueprint
	task.Owner.ProjectID = ids.NewAt(ids.KindProject, now, 20)
	task.Owner.EnvironmentID = ids.NewAt(ids.KindEnvironment, now, 21)
	task.Target = task.Owner.EnvironmentID
	task.Params = map[string]string{testblueprints.EnvironmentDesiredRevisionParam: task.ID}
	task.Steps = nil
	marker := pendingTaskMarker(task)
	task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	value, err := EncodeTaskRecord(task)
	if err != nil {
		t.Fatal(err)
	}
	reference, err := testidempotency.EncodeTaskReference(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	seed, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testtaskjournal.TaskStorageKey(task.ID), Value: value},
		{Type: testkeyvalue.MutationPut, Key: testtaskjournal.TaskActiveOperationKey(task.OperationID), Value: reference},
		{Type: testkeyvalue.MutationPut, Key: testtaskjournal.TaskQueueKey(task.Executor, task.ID), Value: reference},
		{Type: testkeyvalue.MutationPut, Key: testblueprints.EnvironmentBlueprintHeadKey(task.Owner.EnvironmentID), Value: reference},
	})
	if err != nil || !seed.Succeeded {
		t.Fatalf("seed Blueprint parent = %#v, %v", seed, err)
	}
	claimed, found, err := repository.ClaimNextBlueprintParent(ctx, now.Add(time.Second))
	if err != nil || !found || claimed.Record.ID != task.ID ||
		claimed.Record.Status != testtaskjournal.TaskStatusRunning {
		t.Fatalf("claim Blueprint parent = %#v, %t, %v", claimed, found, err)
	}
	if _, found, err := repository.ClaimNextTask(ctx, ids.NewAt(ids.KindAgent, now, 22), 1, now.Add(time.Second)); err != nil || found {
		t.Fatalf("Agent claimed Blueprint parent: %t, %v", found, err)
	}
	restarted, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := restarted.ListBlueprintParentClaims(ctx)
	if err != nil || len(claims) != 1 || claims[0].Record.ID != task.ID ||
		claims[0].Record.StartedAt == nil || !claims[0].Record.StartedAt.Equal(now.Add(time.Second)) {
		t.Fatalf("resumed Blueprint parent claims = %#v, %v", claims, err)
	}
	if _, found, err := restarted.ClaimNextBlueprintParent(ctx, now.Add(2*time.Second)); err != nil || found {
		t.Fatalf("duplicate Blueprint parent claim = %t, %v", found, err)
	}
}

// Rationale: a newer accepted Blueprint may overtake a queued parent. The old
// parent must become replayable terminal history without receiving execution.
func TestBlueprintParentClaimRetiresSupersededPendingTask(t *testing.T) {
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	now := taskJournalTime()
	task := validTaskRecord(now)
	task.Executor = testtaskjournal.TaskExecutorBlueprint
	task.Owner.ProjectID = ids.NewAt(ids.KindProject, now, 30)
	task.Owner.EnvironmentID = ids.NewAt(ids.KindEnvironment, now, 31)
	task.Target = task.Owner.EnvironmentID
	task.Params = map[string]string{testblueprints.EnvironmentDesiredRevisionParam: task.ID}
	task.Steps = nil
	marker := pendingTaskMarker(task)
	task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	taskValue, err := EncodeTaskRecord(task)
	if err != nil {
		t.Fatal(err)
	}
	markerValue, err := testidempotency.EncodeIdempotencyMarker(marker)
	if err != nil {
		t.Fatal(err)
	}
	markerKey, err := testidempotency.IdempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatal(err)
	}
	reference, err := testidempotency.EncodeTaskReference(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	newHeadID := ids.NewAt(ids.KindTask, now, 32)
	newHead, err := testidempotency.EncodeTaskReference(newHeadID)
	if err != nil {
		t.Fatal(err)
	}
	seed, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testtaskjournal.TaskStorageKey(task.ID), Value: taskValue},
		{Type: testkeyvalue.MutationPut, Key: testtaskjournal.TaskActiveOperationKey(task.OperationID), Value: reference},
		{Type: testkeyvalue.MutationPut, Key: testtaskjournal.TaskQueueKey(task.Executor, task.ID), Value: reference},
		{Type: testkeyvalue.MutationPut, Key: testblueprints.EnvironmentBlueprintHeadKey(task.Owner.EnvironmentID), Value: newHead},
		{Type: testkeyvalue.MutationPut, Key: markerKey, Value: markerValue},
	})
	if err != nil || !seed.Succeeded {
		t.Fatalf("seed superseded Blueprint parent = %#v, %v", seed, err)
	}
	if _, found, err := repository.ClaimNextBlueprintParent(ctx, now.Add(time.Second)); err != nil || found {
		t.Fatalf("superseded parent was claimed: %t, %v", found, err)
	}
	read, err := store.GetMany(ctx, testkeyvalue.GetManyRequest{Keys: []string{
		testtaskjournal.TaskStorageKey(task.ID),
		testtaskjournal.TaskQueueKey(task.Executor, task.ID),
		testtaskjournal.TaskActiveOperationKey(task.OperationID),
		markerKey,
		testblueprints.EnvironmentBlueprintHeadKey(task.Owner.EnvironmentID),
	}})
	if err != nil || read == nil || len(read.Values) != 5 {
		t.Fatalf("read retired parent = %#v, %v", read, err)
	}
	retired, err := DecodeTaskRecord(read.Values[0].Value)
	if err != nil || retired.Status != testtaskjournal.TaskStatusAborted || retired.StartedAt != nil ||
		read.Values[1] != nil || read.Values[2] != nil {
		t.Fatalf("retired parent state = %#v, %v", retired, err)
	}
	terminalMarker, err := testidempotency.DecodeIdempotencyMarker(read.Values[3].Value, marker.Locator)
	if err != nil || terminalMarker.State != testidempotency.IdempotencyMarkerFailed ||
		terminalMarker.TaskID != task.ID {
		t.Fatalf("retired parent replay marker = %#v, %v", terminalMarker, err)
	}
	selectedHead, err := testidempotency.DecodeTaskReference(read.Values[4].Value)
	if err != nil || selectedHead != newHeadID {
		t.Fatalf("current head = %q, %v", selectedHead, err)
	}
}
