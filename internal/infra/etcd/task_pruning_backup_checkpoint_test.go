package etcd

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const taskPruneCheckpointDigest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestTaskPruningDrainsBackupCheckpointsInBoundedBatches(t *testing.T) {
	// Rationale: more than 47 checkpoint records of each kind must continue
	// through multiple exact CAS batches without exceeding the 96-operation cap
	// or leaving Task-scoped runtime authority behind.
	t.Parallel()
	ctx := context.Background()
	store := &taskPruneCheckpointStore{memoryTaskStore: newMemoryTaskStore()}
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	now := taskJournalTime().Add(3 * time.Minute)
	task := validTaskRecord(now)
	createLifecycleTask(t, repository, task)
	seedTaskPruneBackupCheckpoints(t, store.memoryTaskStore, task.ID, now, 60, 60)
	terminal, err := repository.AbortPendingTask(ctx, task.ID, now.Add(time.Second))
	if err != nil {
		t.Fatalf("AbortPendingTask() error = %v", err)
	}
	pruneAt := terminal.Record.RetainUntil.Add(time.Nanosecond)
	pruneTaskCheckpointMarker(t, store.memoryTaskStore, pruneAt)
	if count, err := repository.PruneExpiredTasks(ctx, pruneAt); err != nil || count != 1 {
		t.Fatalf("PruneExpiredTasks() = %d, %v", count, err)
	}
	if store.maximumOperations != maximumTransactionOperations {
		t.Fatalf(
			"maximum checkpoint prune operations = %d, want %d",
			store.maximumOperations,
			maximumTransactionOperations,
		)
	}
	assertTaskPruneCheckpointPrefixEmpty(
		t,
		store.memoryTaskStore,
		backupCheckpointCursorTaskPrefix(task.ID),
	)
	assertTaskPruneCheckpointPrefixEmpty(
		t,
		store.memoryTaskStore,
		backupCheckpointDedupTaskPrefix(task.ID),
	)
	assertTaskLifecycleValue(t, store.memoryTaskStore, taskKey(task.ID), false)
	assertTaskLifecycleValue(t, store.memoryTaskStore, taskPruneIntentKey(task.ID), false)
}

func TestTaskPruningReplaysCommittedBackupCheckpointBatch(t *testing.T) {
	// Rationale: a lost response after a checkpoint batch commits must leave the
	// Task primary readable and let the private prune intent resume without
	// repeating or skipping subordinate cleanup.
	t.Parallel()
	ctx := context.Background()
	unknown := errs.New(errs.KindStorageUnavailable, "unknown checkpoint prune outcome")
	store := &taskPruneCheckpointStore{
		memoryTaskStore: newMemoryTaskStore(),
		failure:         unknown,
	}
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	now := taskJournalTime().Add(4 * time.Minute)
	task := validTaskRecord(now)
	store.failureTaskID = task.ID
	createLifecycleTask(t, repository, task)
	seedTaskPruneBackupCheckpoints(t, store.memoryTaskStore, task.ID, now, 50, 3)
	terminal, err := repository.AbortPendingTask(ctx, task.ID, now.Add(time.Second))
	if err != nil {
		t.Fatalf("AbortPendingTask() error = %v", err)
	}
	pruneAt := terminal.Record.RetainUntil.Add(time.Nanosecond)
	pruneTaskCheckpointMarker(t, store.memoryTaskStore, pruneAt)
	if count, err := repository.PruneExpiredTasks(ctx, pruneAt); count != 0 ||
		!errors.Is(err, unknown) {
		t.Fatalf("PruneExpiredTasks(unknown) = %d, %v", count, err)
	}
	assertTaskLifecycleValue(t, store.memoryTaskStore, taskKey(task.ID), true)
	intentValue := mustTaskPruneCheckpointValue(
		t,
		store.memoryTaskStore,
		taskPruneIntentKey(task.ID),
	)
	intent, err := decodeTaskPruneIntent(intentValue.Value)
	if err != nil || intent.TaskPrimaryDeleted || intent.BackupCheckpointCursorsComplete {
		t.Fatalf("checkpointed Task prune intent = %#v, %v", intent, err)
	}
	remaining, err := store.Range(ctx, RangeRequest{
		Prefix: backupCheckpointCursorTaskPrefix(task.ID),
		Limit:  maximumTaskPruneBatchRecords + 1,
	})
	if err != nil || remaining == nil || len(remaining.Values) != 3 {
		t.Fatalf("remaining checkpoint cursors = %#v, %v", remaining, err)
	}
	clearKeyValueSlice(remaining.Values)
	if count, err := repository.PruneExpiredTasks(ctx, pruneAt); err != nil || count != 1 {
		t.Fatalf("PruneExpiredTasks(replay) = %d, %v", count, err)
	}
	assertTaskPruneCheckpointPrefixEmpty(
		t,
		store.memoryTaskStore,
		backupCheckpointCursorTaskPrefix(task.ID),
	)
	assertTaskPruneCheckpointPrefixEmpty(
		t,
		store.memoryTaskStore,
		backupCheckpointDedupTaskPrefix(task.ID),
	)
	assertTaskLifecycleValue(t, store.memoryTaskStore, taskKey(task.ID), false)
}

func TestTaskPruningDoesNotDeleteAnotherTasksBackupCheckpoints(t *testing.T) {
	// Rationale: Task-rooted checkpoint prefixes and value validation must keep
	// another Task's cursor and deduplication records outside the prune plan.
	t.Parallel()
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	now := taskJournalTime().Add(5 * time.Minute)
	task := validTaskRecord(now)
	createLifecycleTask(t, repository, task)
	seedTaskPruneBackupCheckpoints(t, store, task.ID, now, 2, 2)
	otherTaskID := ids.NewAt(ids.KindTask, now, 9100)
	seedTaskPruneBackupCheckpoints(t, store, otherTaskID, now, 2, 2)
	terminal, err := repository.AbortPendingTask(ctx, task.ID, now.Add(time.Second))
	if err != nil {
		t.Fatalf("AbortPendingTask() error = %v", err)
	}
	pruneAt := terminal.Record.RetainUntil.Add(time.Nanosecond)
	pruneTaskCheckpointMarker(t, store, pruneAt)
	if count, err := repository.PruneExpiredTasks(ctx, pruneAt); err != nil || count != 1 {
		t.Fatalf("PruneExpiredTasks() = %d, %v", count, err)
	}
	assertTaskPruneCheckpointPrefixEmpty(t, store, backupCheckpointCursorTaskPrefix(task.ID))
	assertTaskPruneCheckpointPrefixEmpty(t, store, backupCheckpointDedupTaskPrefix(task.ID))
	assertTaskPruneCheckpointPrefixCount(
		t,
		store,
		backupCheckpointCursorTaskPrefix(otherTaskID),
		2,
	)
	assertTaskPruneCheckpointPrefixCount(
		t,
		store,
		backupCheckpointDedupTaskPrefix(otherTaskID),
		2,
	)
}

type taskPruneCheckpointStore struct {
	*memoryTaskStore
	maximumOperations int
	failureTaskID     string
	failure           error
	failed            bool
}

func (store *taskPruneCheckpointStore) Transact(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	operations := len(conditions) + len(mutations)
	if operations > store.maximumOperations {
		store.maximumOperations = operations
	}
	result, err := store.memoryTaskStore.Transact(ctx, conditions, mutations)
	if err != nil || !result.Succeeded || store.failure == nil || store.failed {
		return result, err
	}
	prefix := backupCheckpointCursorTaskPrefix(store.failureTaskID)
	for _, mutation := range mutations {
		if mutation.Type == MutationDelete && len(mutation.Key) > len(prefix) &&
			mutation.Key[:len(prefix)] == prefix {
			store.failed = true
			return TransactionResult{}, store.failure
		}
	}
	return result, nil
}

func seedTaskPruneBackupCheckpoints(
	t *testing.T,
	store *memoryTaskStore,
	taskID string,
	now time.Time,
	cursors int,
	deduplications int,
) {
	t.Helper()
	for index := 0; index < cursors; index++ {
		record := backupCheckpointCursorRecord{
			TaskID:       taskID,
			AssignmentID: ids.NewAt(ids.KindAssignment, now, int64(9200+index)),
			StepID:       ids.NewAt(ids.KindStep, now, int64(9300+index)),
			NextSequence: 2,
		}
		value, err := encodeBackupCheckpointCursorRecord(record)
		if err != nil {
			t.Fatalf("encodeBackupCheckpointCursorRecord() error = %v", err)
		}
		seedTaskRepositoryValue(t, store, backupCheckpointCursorKey(BackupCheckpointInput{
			TaskID: record.TaskID, AssignmentID: record.AssignmentID, StepID: record.StepID,
		}), value)
		clear(value)
	}
	assignmentID := ids.NewAt(ids.KindAssignment, now, 9400)
	stepID := ids.NewAt(ids.KindStep, now, 9401)
	for index := 0; index < deduplications; index++ {
		record := backupCheckpointDedupRecord{
			TaskID: taskID, AssignmentID: assignmentID, StepID: stepID,
			Sequence: uint64(index + 1), Kind: BackupCheckpointUploadCompleted,
			PayloadSHA256: taskPruneCheckpointDigest,
		}
		value, err := encodeBackupCheckpointDedupRecord(record)
		if err != nil {
			t.Fatalf("encodeBackupCheckpointDedupRecord() error = %v", err)
		}
		seedTaskRepositoryValue(t, store, backupCheckpointDedupKey(BackupCheckpointInput{
			TaskID: record.TaskID, AssignmentID: record.AssignmentID,
			StepID: record.StepID, Sequence: record.Sequence,
		}), value)
		clear(value)
	}
}

func pruneTaskCheckpointMarker(t *testing.T, store *memoryTaskStore, pruneAt time.Time) {
	t.Helper()
	repository, err := newIdempotencyRepository(store)
	if err != nil {
		t.Fatalf("newIdempotencyRepository() error = %v", err)
	}
	if count, err := repository.PruneExpired(
		context.Background(),
		pruneAt,
	); err != nil ||
		count != 1 {
		t.Fatalf("PruneExpired(marker) = %d, %v", count, err)
	}
}

func assertTaskPruneCheckpointPrefixEmpty(
	t *testing.T,
	store *memoryTaskStore,
	prefix string,
) {
	t.Helper()
	assertTaskPruneCheckpointPrefixCount(t, store, prefix, 0)
}

func assertTaskPruneCheckpointPrefixCount(
	t *testing.T,
	store *memoryTaskStore,
	prefix string,
	want int,
) {
	t.Helper()
	page, err := store.Range(
		context.Background(),
		RangeRequest{Prefix: prefix, Limit: int64(want + 1)},
	)
	if err != nil || page == nil {
		t.Fatalf("Range(%s) = %#v, %v", prefix, page, err)
	}
	defer clearKeyValueSlice(page.Values)
	if len(page.Values) != want || page.More {
		t.Fatalf(
			"Range(%s) count/more = %d/%t, want %d/false",
			prefix,
			len(page.Values),
			page.More,
			want,
		)
	}
}

func mustTaskPruneCheckpointValue(
	t *testing.T,
	store *memoryTaskStore,
	key string,
) *KeyValue {
	t.Helper()
	result, err := store.Get(context.Background(), key)
	if err != nil || result.Entry == nil {
		t.Fatalf("Get(%s) = %#v, %v", key, result, err)
	}
	return result.Entry
}
