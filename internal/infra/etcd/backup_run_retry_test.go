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
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
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
		attempt.Evidence = testbackupruntime.BackupArtifactEvidence{}
		attempt.Upload = testbackupruntime.BackupUploadOutcome{}
		attempt.Object = testbackupruntime.BackupObjectIdentity{}
		attempt.FailureCode = ""
		return attempt
	}
	succeeded := makeAttempt(0, testbackupruntime.BackupSourceAttemptSucceeded)
	succeeded.Phase = testbackupruntime.BackupSourcePhaseRetention
	backupRuntimeCompleteSourceArtifact(run, &succeeded)
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
			source.Evidence != (testbackupruntime.BackupArtifactEvidence{}) ||
			source.Object != (testbackupruntime.BackupObjectIdentity{}) ||
			source.Upload != (testbackupruntime.BackupUploadOutcome{}) || source.FailureCode != "" {
			t.Fatalf("retry source %d = %#v, prior = %#v", index, source, prior)
		}
	}
}

func TestPrepareBackupRunRetryPublishesAtomicAttempt(t *testing.T) {
	runtime, store, tasks, run, claim := publishBackupTerminalLifecycleTask(t)
	result := completedComposeTaskResult()
	result.Kind = testtaskjournal.TaskResultBackup
	result.ExecutionEpoch = claim.Assignment.Record.ExecutionEpoch
	result.AssignmentGeneration = claim.Assignment.Record.BackupAuthorityFence.AssignmentGeneration
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
	retryInput := BackupRunRetryInput{
		SourceTaskID: run.TaskID,
		TaskID:       retryTaskID,
		CreatedAt:    retryAt,
	}
	if _, err := runtime.PrepareBackupRunRetry(context.Background(), retryInput); !errors.Is(
		err,
		errs.New(errs.KindResourceInUse, ""),
	) {
		t.Fatalf("retry before PostgreSQL helper cleanup = %v", err)
	}
	acknowledgeRetryPostgresCleanup(t, tasks, failed.Record, run.Sources[0].RecoveryPointID)
	prepared, err := runtime.PrepareBackupRunRetry(context.Background(), retryInput)
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
	sealed.PlanHash = nil
	sealed.BackupScope = prepared.Scope
	sealed.Artifacts = prepared.Artifacts
	for index, authority := range prepared.Authority {
		sealed.Steps[index].StepId = authority.StepId
		sealed.Steps[index].Payload = &agentpb.ExecutionStep_BackupStep{BackupStep: authority}
		task.Steps[index].ID = authority.StepId
	}
	sealed, err = executionplan.Seal(sealed)
	if err != nil {
		t.Fatal(err)
	}
	task.PlanHash = hex.EncodeToString(sealed.PlanHash)
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

func acknowledgeRetryPostgresCleanup(t *testing.T, tasks *TaskRepository, task TaskRecord, pointID string) {
	t.Helper()
	assignment := task.TerminalAssignment
	key, err := executionplan.BackupStagingRecoveryKey(task.ID, task.Steps[0].ID, pointID)
	if err != nil {
		t.Fatal(err)
	}
	source, err := tasks.ReadBackupStagingSource(
		context.Background(),
		assignment.AgentID,
		assignment.AgentGeneration,
		key,
	)
	if err != nil {
		t.Fatal(err)
	}
	inventory := &agentpb.BackupStagingInventory{Entries: []*agentpb.BackupRecoveredStage{{
		RecoveryKeySha256: key, PostgresExecution: true,
	}}}
	digest, err := executionplan.BackupStagingInventorySHA256(inventory)
	if err != nil {
		t.Fatal(err)
	}
	plan := &agentpb.BackupStagingRecoveryPlan{
		InventorySha256: digest,
		Dispositions: []*agentpb.BackupStagingDisposition{{
			RecoveryKeySha256: key,
			Disposition: &agentpb.BackupStagingDisposition_DiscardRecovered{
				DiscardRecovered: &agentpb.BackupDiscardRecovered{},
			},
			PostgresGuard: &agentpb.BackupPostgresStagingGuard{TaskId: task.ID, Step: source.Step,
				TerminalCleanup: true, DatabaseService: source.Plan.BackupScope.Services[0], DatabaseArtifact: source.Plan.Artifacts[0]},
		}},
	}
	process := [16]byte{1}
	if err := tasks.PublishBackupStagingDelivery(context.Background(), testbackupruntime.BackupStagingDeliveryRecord{
		AgentID: assignment.AgentID, AgentGeneration: assignment.AgentGeneration, ProcessGeneration: process,
		Inventory: inventory, Plan: plan,
	}, []BackupStagingSource{source}); err != nil {
		t.Fatal(err)
	}
	planDigest, err := executionplan.BackupStagingRecoveryPlanSHA256(plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tasks.ApplyBackupStagingDelivery(context.Background(), assignment.AgentID, assignment.AgentGeneration,
		process, &agentpb.BackupStagingRecoveryAck{InventorySha256: digest, AppliedPlanSha256: planDigest,
			AppliedDispositionCount: 1}); err != nil {
		t.Fatal(err)
	}
}
