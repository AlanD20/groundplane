package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

// Rationale: Host update history must include queued/preparation-failed Tasks,
// and its bounded newest read must validate primary/index evidence together.
func TestLatestControllerUpdateUsesDurableTaskHistory(t *testing.T) {
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, found, err := repository.LatestControllerUpdate(ctx); err != nil || found {
		t.Fatalf("empty history = %t, %v", found, err)
	}
	now := taskJournalTime()
	var newest TaskRecord
	for index := range 2 {
		task := validTaskRecord(now.Add(time.Duration(index) * time.Second))
		task.ID, task.OperationID = ids.New(ids.KindTask), ids.New(ids.KindOperation)
		task.Owner, task.Executor, task.Type, task.Target = PlatformTaskOwner(), TaskExecutorController, TaskUpdate, "controller"
		task.Params = map[string]string{TaskResourceKindParam: TaskResourceController}
		marker := pendingRetryMarker(task, task.ID, task.CreatedAt, "native-history-key-"+task.ID)
		if _, err := repository.CreateTask(ctx, task, marker); err != nil {
			t.Fatal(err)
		}
		newest = task
	}
	got, found, err := repository.LatestControllerUpdate(ctx)
	if err != nil || !found || got.Record.ID != newest.ID || got.Record.Status != TaskStatusPending {
		t.Fatalf("latest update = %#v, %t, %v", got, found, err)
	}
	if _, err := store.Transact(ctx, nil, []Mutation{{Type: MutationDelete, Key: taskKey(newest.ID)}}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := repository.LatestControllerUpdate(ctx); err == nil {
		t.Fatal("orphaned native history accepted")
	}
}

// Rationale: a retained newest pointer must never outlive the Task it indexes;
// ordinary journal retention removes both in its existing fenced transaction.
func TestNativeControllerUpdateHistoryPrunesWithTask(t *testing.T) {
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	task := validTaskRecord(taskJournalTime())
	task.Owner, task.Executor, task.Type, task.Target = PlatformTaskOwner(), TaskExecutorController, TaskUpdate, "controller"
	task.Params = map[string]string{TaskResourceKindParam: TaskResourceController}
	createLifecycleTask(t, repository, task)
	terminal, err := repository.AbortPendingTask(ctx, task.ID, task.CreatedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	pruneAt := terminal.Record.RetainUntil.Add(time.Nanosecond)
	idempotency, err := newIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := idempotency.PruneExpired(ctx, pruneAt); err != nil {
		t.Fatal(err)
	}
	if count, err := repository.PruneExpiredTasks(ctx, pruneAt); err != nil || count != 1 {
		t.Fatalf("prune = %d, %v", count, err)
	}
	if _, found, err := repository.LatestControllerUpdate(ctx); err != nil || found {
		t.Fatalf("pruned native history = %t, %v", found, err)
	}
}
