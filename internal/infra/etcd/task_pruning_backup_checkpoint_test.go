package etcd

import (
	"context"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
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
	if store.maximumOperations != testkeyvalue.MaximumOperations {
		t.Fatalf(
			"maximum checkpoint prune operations = %d, want %d",
			store.maximumOperations, testkeyvalue.MaximumOperations,
		)
	}
	assertTaskPruneCheckpointPrefixEmpty(
		t,
		store.memoryTaskStore, testbackupruntime.BackupCheckpointCursorTaskPrefix(task.ID),
	)
	assertTaskPruneCheckpointPrefixEmpty(
		t,
		store.memoryTaskStore, testbackupruntime.BackupCheckpointDedupTaskPrefix(task.ID),
	)
	assertTaskLifecycleValue(t, store.memoryTaskStore, testtaskjournal.TaskStorageKey(task.ID), false)
	assertTaskLifecycleValue(t, store.memoryTaskStore, testtaskjournal.TaskPruneIntentKey(task.ID), false)
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
	assertTaskLifecycleValue(t, store.memoryTaskStore, testtaskjournal.TaskStorageKey(task.ID), true)
	intentValue := mustTaskPruneCheckpointValue(
		t,
		store.memoryTaskStore, testtaskjournal.TaskPruneIntentKey(task.ID),
	)
	intent, err := testtaskjournal.DecodePruneIntent(intentValue.Value)
	if err != nil || intent.TaskPrimaryDeleted || intent.BackupCheckpointCursorsComplete {
		t.Fatalf("checkpointed Task prune intent = %#v, %v", intent, err)
	}
	remaining, err := store.Range(ctx, testkeyvalue.RangeRequest{
		Prefix: testbackupruntime.BackupCheckpointCursorTaskPrefix(task.ID),
		Limit:  maximumTaskPruneBatchRecords + 1,
	})
	if err != nil || remaining == nil || len(remaining.Values) != 3 {
		t.Fatalf("remaining checkpoint cursors = %#v, %v", remaining, err)
	}
	testkeyvalue.ClearRangeValues(remaining.Values)
	if count, err := repository.PruneExpiredTasks(ctx, pruneAt); err != nil || count != 1 {
		t.Fatalf("PruneExpiredTasks(replay) = %d, %v", count, err)
	}
	assertTaskPruneCheckpointPrefixEmpty(
		t,
		store.memoryTaskStore, testbackupruntime.BackupCheckpointCursorTaskPrefix(task.ID),
	)
	assertTaskPruneCheckpointPrefixEmpty(
		t,
		store.memoryTaskStore, testbackupruntime.BackupCheckpointDedupTaskPrefix(task.ID),
	)
	assertTaskLifecycleValue(t, store.memoryTaskStore, testtaskjournal.TaskStorageKey(task.ID), false)
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
	assertTaskPruneCheckpointPrefixEmpty(t, store, testbackupruntime.BackupCheckpointCursorTaskPrefix(task.ID))
	assertTaskPruneCheckpointPrefixEmpty(t, store, testbackupruntime.BackupCheckpointDedupTaskPrefix(task.ID))
	assertTaskPruneCheckpointPrefixCount(
		t,
		store, testbackupruntime.BackupCheckpointCursorTaskPrefix(otherTaskID), 2,
	)
	assertTaskPruneCheckpointPrefixCount(
		t,
		store, testbackupruntime.BackupCheckpointDedupTaskPrefix(otherTaskID), 2,
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
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	operations := len(conditions) + len(mutations)
	if operations > store.maximumOperations {
		store.maximumOperations = operations
	}
	result, err := store.memoryTaskStore.Transact(ctx, conditions, mutations)
	if err != nil || !result.Succeeded || store.failure == nil || store.failed {
		return result, err
	}
	prefix := testbackupruntime.BackupCheckpointCursorTaskPrefix(store.failureTaskID)
	for _, mutation := range mutations {
		if mutation.Type == testkeyvalue.MutationDelete && len(mutation.Key) > len(prefix) &&
			mutation.Key[:len(prefix)] == prefix {
			store.failed = true
			return testkeyvalue.TransactionResult{}, store.failure
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
		record := testbackupruntime.BackupCheckpointCursorRecord{
			TaskID:       taskID,
			AssignmentID: ids.NewAt(ids.KindAssignment, now, int64(9200+index)),
			StepID:       ids.NewAt(ids.KindStep, now, int64(9300+index)),
			ExecutionID:  ids.NewULID(), AssignmentGeneration: 1,
			AuthoritySHA256: taskPruneCheckpointDigest, NextSequence: 2,
		}
		value, err := testbackupruntime.EncodeBackupCheckpointCursorRecord(record)
		if err != nil {
			t.Fatalf("encodeBackupCheckpointCursorRecord() error = %v", err)
		}
		seedTaskRepositoryValue(
			t,
			store,
			testbackupruntime.BackupCheckpointCursorKey(testbackupruntime.BackupCheckpointInput{
				TaskID: record.TaskID, AssignmentID: record.AssignmentID,
				StepID: record.StepID, ExecutionID: record.ExecutionID,
			}),
			value,
		)
		clear(value)
	}
	assignmentID := ids.NewAt(ids.KindAssignment, now, 9400)
	stepID := ids.NewAt(ids.KindStep, now, 9401)
	executionID := ids.NewULID()
	authorityDigest, err := hex.DecodeString(taskPruneCheckpointDigest)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < deduplications; index++ {
		sequence := uint64(index + 1)
		request := &agentpb.BackupCheckpointRequest{
			TaskId: taskID, AssignmentId: assignmentID, StepId: stepID, ExecutionId: executionID,
			CheckpointSequence: sequence, AuthorityDigest: append([]byte(nil), authorityDigest...),
			Checkpoint: &agentpb.BackupCheckpointRequest_ArtifactPrepared{
				ArtifactPrepared: &agentpb.BackupArtifactPrepared{
					PointId: testBackupPointID,
					Evidence: &agentpb.BackupArtifactEvidence{
						SourceSizeBytes: 4096, SourceSha256: append([]byte(nil), authorityDigest...),
						StoredSizeBytes: 4296, StoredSha256: append([]byte(nil), authorityDigest...),
					},
					Finals: &agentpb.BackupStagingFinals{
						SourceRelativeName: executionplan.BackupSourceStagingFinal,
						StoredRelativeName: executionplan.BackupStoredStagingFinal,
						SameInode:          func() *bool { value := false; return &value }(),
					},
					Archive: &agentpb.BackupArtifactPrepared_Postgres{
						Postgres: &agentpb.BackupPostgresArchiveEvidence{PgDumpMajor: 16, AdapterContractVersion: 1},
					},
				},
			},
		}
		preceding := int64(0)
		if sequence > 1 {
			preceding = int64(index)
			request.PrecedingCheckpoint = &agentpb.CheckpointFence{
				AuthorityDigest: append([]byte(nil), authorityDigest...), DedupeKeyModRevision: preceding,
			}
		}
		input := testbackupruntime.BackupCheckpointInput{
			TaskID: taskID, AssignmentID: assignmentID, StepID: stepID, ExecutionID: executionID,
			AssignmentGeneration: 1, AuthoritySHA256: taskPruneCheckpointDigest,
			PrecedingCheckpointRevision: preceding, Sequence: sequence, Request: request,
		}
		payloadSHA256, err := testbackupruntime.BackupCheckpointDigest(input)
		if err != nil {
			t.Fatal(err)
		}
		requestValue, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		record := testbackupruntime.BackupCheckpointDedupRecord{
			TaskID: taskID, AssignmentID: assignmentID, StepID: stepID, ExecutionID: executionID,
			AssignmentGeneration: 1, AuthoritySHA256: taskPruneCheckpointDigest,
			PrecedingCheckpointRevision: preceding, Sequence: sequence,
			CheckpointTag: testbackupruntime.BackupCheckpointTag(request),
			PayloadSHA256: payloadSHA256, Request: requestValue,
		}
		value, err := testbackupruntime.EncodeBackupCheckpointDedupRecord(record)
		if err != nil {
			t.Fatalf("encodeBackupCheckpointDedupRecord() error = %v", err)
		}
		seedTaskRepositoryValue(
			t,
			store,
			testbackupruntime.BackupCheckpointDedupKey(testbackupruntime.BackupCheckpointInput{
				TaskID: record.TaskID, AssignmentID: record.AssignmentID,
				StepID: record.StepID, ExecutionID: record.ExecutionID, Sequence: record.Sequence,
			}),
			value,
		)
		clear(value)
	}
}

func pruneTaskCheckpointMarker(t *testing.T, store *memoryTaskStore, pruneAt time.Time) {
	t.Helper()
	repository := NewTestIdempotencyRetention(t, store)
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
		context.Background(), testkeyvalue.RangeRequest{Prefix: prefix, Limit: int64(want + 1)},
	)
	if err != nil || page == nil {
		t.Fatalf("Range(%s) = %#v, %v", prefix, page, err)
	}
	defer testkeyvalue.ClearRangeValues(page.Values)
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
) *testkeyvalue.KeyValue {
	t.Helper()
	result, err := store.Get(context.Background(), key)
	if err != nil || result.Entry == nil {
		t.Fatalf("Get(%s) = %#v, %v", key, result, err)
	}
	return result.Entry
}
