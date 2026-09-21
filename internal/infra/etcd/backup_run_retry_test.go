package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

func TestNewBackupRunRetryRecordPreservesOnlyIncompleteSnapshots(t *testing.T) {
	_, _, run := newBackupRuntimeBareFixture(t)
	makeAttempt := func(ordinal uint32, state testbackupruntime.BackupSourceAttemptState) testbackupruntime.BackupRunSourceAttemptRecord {
		attempt := run.Sources[0]
		attempt.Ordinal = ordinal
		attempt.SourceID = ids.NewAt(ids.KindBackupSource, run.CreatedAt, int64(5000+ordinal))
		attempt.RecoveryPointID = ids.NewAt(ids.KindRecoveryPoint, run.CreatedAt, int64(5100+ordinal))
		attempt.RecoveryPointCreatedAt = run.CreatedAt
		attempt.ObjectKey = run.ConnectorPrefix + run.EnvironmentID + "/" + attempt.SourceID + "/" +
			attempt.RecoveryPointID + "/artifact.bin"
		attempt.State = state
		attempt.Phase = testbackupruntime.BackupSourcePhaseCapture
		attempt.SizeBytes = 0
		attempt.SHA256 = ""
		attempt.FailureCode = ""
		return attempt
	}
	succeeded := makeAttempt(0, testbackupruntime.BackupSourceAttemptSucceeded)
	succeeded.Phase = testbackupruntime.BackupSourcePhaseRetention
	succeeded.SizeBytes = 1
	succeeded.SHA256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	failed := makeAttempt(1, testbackupruntime.BackupSourceAttemptFailed)
	failed.FailureCode = testbackupruntime.BackupFailureCapture
	unstarted := makeAttempt(2, testbackupruntime.BackupSourceAttemptUnstarted)
	run.Sources = []testbackupruntime.BackupRunSourceAttemptRecord{succeeded, failed, unstarted}
	run.State = testbackupruntime.BackupRunFailed
	retryAt := run.CreatedAt.Add(time.Second)
	retryTaskID := ids.NewAt(ids.KindTask, retryAt, 5200)

	retry, err := newBackupRunRetryRecord(run, retryTaskID, retryAt)
	if err != nil {
		t.Fatal(err)
	}
	if retry.TaskID != retryTaskID || retry.RetryOfTaskID != run.TaskID ||
		retry.OperationID != run.OperationID || retry.State != testbackupruntime.BackupRunQueued ||
		len(retry.Sources) != 2 {
		t.Fatalf("retry identity/checkpoint selection = %#v", retry)
	}
	for index, source := range retry.Sources {
		prior := run.Sources[index+1]
		if source.Ordinal != uint32(index) || source.SourceID != prior.SourceID ||
			source.TargetID != prior.TargetID || source.SourceRevision != prior.SourceRevision ||
			source.TargetRevision != prior.TargetRevision || source.RecoveryPointID == prior.RecoveryPointID ||
			source.State != testbackupruntime.BackupSourceAttemptPending || source.Phase != testbackupruntime.BackupSourcePhaseCapture ||
			source.SizeBytes != 0 || source.SHA256 != "" || source.FailureCode != "" {
			t.Fatalf("retry source %d = %#v, prior = %#v", index, source, prior)
		}
	}
}

func TestPrepareBackupRunRetryPublishesAtomicAttempt(t *testing.T) {
	runtime, store, tasks, run, claim := publishBackupTerminalLifecycleTask(t)
	result := completedComposeTaskResult()
	result.ExitCode = 1
	terminalAt := claim.Assignment.Record.AssignedAt.Add(time.Second)
	failed, err := tasks.AcknowledgeTask(
		context.Background(),
		claim.Assignment.Record.AgentID,
		claim.Assignment.Record.AgentGeneration,
		run.TaskID,
		claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusFailed, result,
		terminalAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	retryAt := terminalAt.Add(time.Second)
	retryTaskID := ids.NewAt(ids.KindTask, retryAt, 5300)
	prepared, err := runtime.PrepareBackupRunRetry(context.Background(), BackupRunRetryInput{
		SourceTaskID: run.TaskID,
		TaskID:       retryTaskID,
		CreatedAt:    retryAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Publication.Clear()
	if prepared.SourceTask.ID != run.TaskID || prepared.Run.RetryOfTaskID != run.TaskID ||
		prepared.Run.OperationID != run.OperationID ||
		prepared.Run.Sources[0].RecoveryPointID == run.Sources[0].RecoveryPointID {
		t.Fatalf("prepared retry = %#v", prepared.Run)
	}
	task, sealed, marker, _ := backupRuntimePublicationTask(t, store, prepared.Run)
	task.RetryOf = run.TaskID
	task.IdempotencyKey = failed.Record.IdempotencyKey
	marker.Locator.Route = "/tasks/{id}/retry"
	marker.Locator.Key = "backup-task-retry-0001"
	if _, err := prepared.Publication.Publish(context.Background(), task, sealed, marker); err != nil {
		t.Fatal(err)
	}

	storedRetry, err := tasks.GetTask(context.Background(), retryTaskID)
	if err != nil || storedRetry.Record.Status != testtaskjournal.TaskStatusPending ||
		storedRetry.Record.RetryOf != run.TaskID || storedRetry.Record.OperationID != run.OperationID {
		t.Fatalf("stored retry Task = %#v, %v", storedRetry, err)
	}
	storedRun, err := runtime.GetBackupRun(context.Background(), retryTaskID)
	if err != nil || storedRun.Record.State != testbackupruntime.BackupRunQueued ||
		storedRun.Record.RetryOfTaskID != run.TaskID {
		t.Fatalf("stored retry run = %#v, %v", storedRun, err)
	}
	storedSource, err := tasks.GetTask(context.Background(), run.TaskID)
	if err != nil || storedSource.Record.Status != testtaskjournal.TaskStatusFailed {
		t.Fatalf("stored source Task = %#v, %v", storedSource, err)
	}
	lockEntry := mustOptionalKey(t, store, testhierarchy.EnvironmentOperationLockKey(run.EnvironmentID))
	lock, err := testbackupruntime.DecodeBackupOperationLockRecord(lockEntry.Value)
	if err != nil || lock.TaskID != retryTaskID || lock.OperationID != run.OperationID {
		t.Fatalf("retry Environment lock = %#v, %v", lock, err)
	}
}
