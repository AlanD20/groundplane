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
