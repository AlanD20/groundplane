package etcd

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type backupKeyRotationFixture struct {
	repository  *BackupPolicyRepository
	store       *memoryHierarchyStore
	tasks       *TaskRepository
	environment Versioned[EnvironmentRecord]
	task        TaskRecord
}

func newBackupKeyRotationFixture(t *testing.T) backupKeyRotationFixture {
	t.Helper()
	repository, store, environment, _ := backupPolicyRepositoryTestHierarchy(t)
	now := taskJournalTime()
	currentRecord := BackupKeyRecord{
		EnvironmentID: environment.Record.ID,
		Recipient:     newTestBackupRecipient(t),
		KeyEra:        1,
		CreatedAt:     now,
		RotatedAt:     now,
	}
	currentValue := BackupKeyEncryptedValue{
		EnvironmentID: environment.Record.ID,
		KeyEra:        1,
		Ciphertext:    []byte("wrapped-current-identity"),
	}
	recordValue, err := encodeBackupKeyRecord(currentRecord)
	if err != nil {
		t.Fatal(err)
	}
	encryptedValue, err := encodeBackupKeyEncryptedValue(currentValue)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := store.Transact(context.Background(), nil, []Mutation{
		{Type: MutationPut, Key: backupKeyKey(environment.Record.ID), Value: recordValue},
		{Type: MutationPut, Key: backupKeyValueKey(environment.Record.ID), Value: encryptedValue},
	}); err != nil || !result.Succeeded {
		t.Fatalf("seed backup key = %#v, %v", result, err)
	}

	taskID := ids.NewAt(ids.KindTask, now, 701)
	operationID := ids.NewAt(ids.KindOperation, now, 702)
	planID := ids.NewAt(ids.KindPlan, now, 703)
	prepared, err := repository.PrepareBackupKeyRotation(context.Background(), BackupKeyRotationInput{
		EnvironmentID: environment.Record.ID,
		TaskID:        taskID,
		OperationID:   operationID,
		PlanID:        planID,
		CreatedAt:     now,
	}, BackupPolicyInitialKeyMaterial{
		Recipient:  newTestBackupRecipient(t),
		Ciphertext: []byte("wrapped-next-identity"),
	})
	if err != nil {
		t.Fatalf("PrepareBackupKeyRotation() error = %v", err)
	}
	task := newTaskRecord(
		taskID,
		operationID,
		prepared.Owner,
		TaskActorOperator,
		TaskRotate,
		environment.Record.ID,
		backupKeyRotationTimeoutSeconds,
		now,
	)
	task.Executor = TaskExecutorController
	task.IdempotencyKey = "rotate-key-test-0001"
	task.PlanID = planID
	task.PlanHash = strings.Repeat("a", 64)
	task.RenderGeneration = 1
	marker := pendingTaskMarker(task)
	marker.Locator.ScopeID = environment.Record.ID
	marker.Locator.Route = "/environments/{id}/rotate-key"
	marker.Locator.Key = task.IdempotencyKey
	if _, err := repository.PublishBackupKeyRotation(context.Background(), prepared, task, marker); err != nil {
		t.Fatalf("PublishBackupKeyRotation() error = %v", err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	return backupKeyRotationFixture{
		repository:  repository,
		store:       store,
		tasks:       tasks,
		environment: environment,
		task:        task,
	}
}

// Rationale: rotation completion must atomically swap both key records,
// terminalize the Task, release the lock, and replay without a second swap.
func TestBackupKeyRotationTerminalTransactionIsAtomicAndReplayable(t *testing.T) {
	fixture := newBackupKeyRotationFixture(t)
	ctx := context.Background()
	claim, found, err := fixture.tasks.ClaimNextControllerTask(ctx, fixture.task.CreatedAt.Add(time.Second))
	if err != nil || !found || claim.Task.Record.ID != fixture.task.ID {
		t.Fatalf("ClaimNextControllerTask() = %#v, %v, %v", claim, found, err)
	}
	if err := fixture.repository.ApplyBackupKeyRotation(ctx, fixture.task.ID); err != nil {
		t.Fatalf("ApplyBackupKeyRotation() error = %v", err)
	}

	failingStore := &backupTerminalTransactionFailureStore{
		hierarchyStore: fixture.store,
		failNext:       errs.New(errs.KindInternal, "injected storage failure"),
	}
	failingTasks, err := newTaskRepository(failingStore)
	if err != nil {
		t.Fatal(err)
	}
	terminalAt := fixture.task.CreatedAt.Add(2 * time.Second)
	if _, err := failingTasks.AcknowledgeControllerTask(
		ctx,
		fixture.task.ID,
		TaskStatusCompleted,
		terminalAt,
	); err == nil {
		t.Fatal("AcknowledgeControllerTask() error = nil, want injected failure")
	}
	assertBackupKeyRotationState(t, fixture, TaskStatusRunning, 1, true, BackupKeyRotationPrepared)

	terminal, err := failingTasks.AcknowledgeControllerTask(
		ctx,
		fixture.task.ID,
		TaskStatusCompleted,
		terminalAt,
	)
	if err != nil || terminal.Record.Status != TaskStatusCompleted {
		t.Fatalf("AcknowledgeControllerTask(retry) = %#v, %v", terminal, err)
	}
	assertBackupKeyRotationState(t, fixture, TaskStatusCompleted, 2, false, BackupKeyRotationApplied)
	replay, err := failingTasks.AcknowledgeControllerTask(
		ctx,
		fixture.task.ID,
		TaskStatusCompleted,
		terminalAt,
	)
	if err != nil || replay.Record.Status != TaskStatusCompleted {
		t.Fatalf("AcknowledgeControllerTask(replay) = %#v, %v", replay, err)
	}
	assertBackupKeyRotationState(t, fixture, TaskStatusCompleted, 2, false, BackupKeyRotationApplied)
}

// Rationale: the shared Environment lock must exclude overlapping rotation
// and no durable history value may contain plaintext age identity material.
func TestBackupKeyRotationLockExcludesOverlapAndHistoryRedactsIdentity(t *testing.T) {
	fixture := newBackupKeyRotationFixture(t)
	_, err := fixture.repository.PrepareBackupKeyRotation(context.Background(), BackupKeyRotationInput{
		EnvironmentID: fixture.environment.Record.ID,
		TaskID:        ids.NewAt(ids.KindTask, fixture.task.CreatedAt, 711),
		OperationID:   ids.NewAt(ids.KindOperation, fixture.task.CreatedAt, 712),
		PlanID:        ids.NewAt(ids.KindPlan, fixture.task.CreatedAt, 713),
		CreatedAt:     fixture.task.CreatedAt,
	}, BackupPolicyInitialKeyMaterial{
		Recipient:  newTestBackupRecipient(t),
		Ciphertext: []byte("wrapped-overlap"),
	})
	if !errors.Is(err, errs.New(errs.KindResourceInUse, "")) {
		t.Fatalf("PrepareBackupKeyRotation(overlap) error = %v, want resource_in_use", err)
	}
	for key, versions := range fixture.store.history {
		for _, version := range versions {
			if bytes.Contains(version.value, []byte("AGE-SECRET-KEY-1")) {
				t.Fatalf("plaintext identity persisted at %s", key)
			}
		}
	}
}

// Rationale: every unsuccessful terminal state must release the lock while
// retaining wrapped authority that an atomic retry can transfer to a new Task.
func TestBackupKeyRotationTerminalFailureStatesRemainRetryable(t *testing.T) {
	for _, status := range []TaskStatus{TaskStatusAborted, TaskStatusFailed, TaskStatusTimedOut} {
		t.Run(string(status), func(t *testing.T) {
			fixture := newBackupKeyRotationFixture(t)
			ctx := context.Background()
			terminalAt := fixture.task.CreatedAt.Add(2 * time.Second)
			var terminal Versioned[TaskRecord]
			var err error
			if status == TaskStatusAborted {
				terminal, err = fixture.tasks.AbortPendingTask(ctx, fixture.task.ID, terminalAt)
			} else {
				if _, found, claimErr := fixture.tasks.ClaimNextControllerTask(
					ctx,
					fixture.task.CreatedAt.Add(time.Second),
				); claimErr != nil || !found {
					t.Fatalf("ClaimNextControllerTask() found/error = %v/%v", found, claimErr)
				}
				terminal, err = fixture.tasks.AcknowledgeControllerTask(
					ctx,
					fixture.task.ID,
					status,
					terminalAt,
				)
			}
			if err != nil || terminal.Record.Status != status {
				t.Fatalf("terminalize(%s) = %#v, %v", status, terminal, err)
			}
			assertBackupKeyRotationState(t, fixture, status, 1, false, BackupKeyRotationPrepared)

			retryAt := terminalAt.Add(time.Second)
			retryID := ids.NewAt(ids.KindTask, retryAt, 720)
			marker := pendingRetryMarker(terminal.Record, retryID, retryAt, "rotate-retry-test-0001")
			marker.Locator.ScopeID = fixture.environment.Record.ID
			result, err := fixture.tasks.RetryTask(
				ctx,
				fixture.task.ID,
				retryID,
				TaskActorOperator,
				marker,
			)
			if err != nil {
				t.Fatalf("RetryTask(%s) error = %v", status, err)
			}
			outcome, _, conflict, classifyErr := result.Classify()
			if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
				t.Fatalf("RetryTask(%s) outcome/conflict/error = %v/%v/%v", status, outcome, conflict, classifyErr)
			}
			retry, err := fixture.tasks.GetTask(ctx, retryID)
			if err != nil || retry.Record.Status != TaskStatusPending ||
				retry.Record.RetryOf != fixture.task.ID ||
				retry.Record.OperationID != fixture.task.OperationID {
				t.Fatalf("retry Task = %#v, %v", retry, err)
			}
			lock, err := fixture.store.Get(
				ctx,
				environmentOperationLockKey(fixture.environment.Record.ID),
			)
			if err != nil || lock.Entry == nil {
				t.Fatalf("retry operation lock = %#v, %v", lock, err)
			}
			key, found, err := fixture.repository.GetBackupKey(ctx, fixture.environment.Record.ID)
			if err != nil || !found || key.Record.KeyEra != 1 {
				t.Fatalf("retry current key = %#v, %v, %v", key, found, err)
			}
			clear(key.Encrypted.Ciphertext)
			rotationValue, err := fixture.store.Get(ctx, backupKeyRotationKey(retryID))
			if err != nil || rotationValue.Entry == nil {
				t.Fatalf("retry rotation record = %#v, %v", rotationValue, err)
			}
			rotation, err := decodeBackupKeyRotationRecord(rotationValue.Entry.Value)
			if err != nil || rotation.State != BackupKeyRotationPrepared ||
				rotation.CurrentKeyEra != 1 || rotation.NextKeyEra != 2 {
				t.Fatalf("retry rotation = %#v, %v", rotation, err)
			}
			clear(rotation.NextEncryptedIdentity)
		})
	}
}

func assertBackupKeyRotationState(
	t *testing.T,
	fixture backupKeyRotationFixture,
	wantTask TaskStatus,
	wantEra int,
	wantLock bool,
	wantRotation BackupKeyRotationState,
) {
	t.Helper()
	task, err := fixture.tasks.GetTask(context.Background(), fixture.task.ID)
	if err != nil || task.Record.Status != wantTask {
		t.Fatalf("Task state = %#v, %v; want %s", task, err, wantTask)
	}
	key, found, err := fixture.repository.GetBackupKey(context.Background(), fixture.environment.Record.ID)
	if err != nil || !found || key.Record.KeyEra != wantEra || key.Encrypted.KeyEra != wantEra {
		t.Fatalf("Backup key = %#v, %v, %v; want era %d", key, found, err, wantEra)
	}
	clear(key.Encrypted.Ciphertext)
	lock, err := fixture.store.Get(context.Background(), environmentOperationLockKey(fixture.environment.Record.ID))
	if err != nil || (lock.Entry != nil) != wantLock {
		t.Fatalf("operation lock = %#v, %v; want present %v", lock, err, wantLock)
	}
	rotationValue, err := fixture.store.Get(context.Background(), backupKeyRotationKey(fixture.task.ID))
	if err != nil || rotationValue.Entry == nil {
		t.Fatalf("rotation record = %#v, %v", rotationValue, err)
	}
	rotation, err := decodeBackupKeyRotationRecord(rotationValue.Entry.Value)
	if err != nil || rotation.State != wantRotation {
		t.Fatalf("rotation state = %#v, %v; want %s", rotation, err, wantRotation)
	}
	if wantRotation == BackupKeyRotationApplied && len(rotation.NextEncryptedIdentity) != 0 {
		t.Fatalf("applied rotation retained wrapped identity: %d bytes", len(rotation.NextEncryptedIdentity))
	}
	clear(rotation.NextEncryptedIdentity)
}
