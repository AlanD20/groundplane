package etcd

import (
	"context"
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/common/ids"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupscheduling"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// validateExistingBackupRunPublication validates the committed winner named by
// the existing marker, never the losing candidate's freshly allocated ids.
func (repository *BackupRuntimeRepository) validateExistingBackupRunPublication(
	ctx context.Context,
	marker idempotencyrecord.IdempotencyMarker,
	readRevision int64,
	commitRevision int64,
) error {
	var response struct {
		TaskID string `json:"task_id"`
	}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerTask || marker.TaskID == "" ||
		marker.Response.Status != 202 ||
		json.Unmarshal(marker.Response.Body, &response) != nil ||
		response.TaskID != marker.TaskID {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	read, err := repository.readFixedKeys(ctx, []string{
		backupruntime.BackupRunKey(marker.TaskID),
		taskjournal.TaskStorageKey(marker.TaskID),
		hierarchyrecord.EnvironmentOperationLockKey(marker.Locator.ScopeID),
	}, readRevision)
	if err != nil {
		return err
	}
	defer clearKeyValues(read.Values)
	if len(read.Values) != 3 || read.Values[1] == nil {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	task, taskErr := decodeTaskRecord(read.Values[1].Value)
	if taskErr != nil || task.ID != marker.TaskID || task.Type != taskjournal.TaskBackup ||
		(task.Actor != taskjournal.TaskActorOperator && task.Actor != taskjournal.TaskActorSystem) || task.Executor != taskjournal.TaskExecutorAgent ||
		(task.RetryOf == "" && task.IdempotencyKey != marker.Locator.Key) ||
		task.idempotencyMarker == nil ||
		*task.idempotencyMarker != marker.Locator || task.Owner.EnvironmentID != marker.Locator.ScopeID {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	if marker.State != idempotencyrecord.IdempotencyMarkerPending {
		return repository.validateTerminalBackupRunPublication(
			ctx,
			marker,
			task,
			read.Values[0],
			read.Values[1],
			readRevision,
			commitRevision,
		)
	}
	if read.Values[0] == nil || read.Values[2] == nil {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	run, runErr := backupruntime.DecodeBackupRunRecord(read.Values[0].Value)
	lock, lockErr := backupruntime.DecodeBackupOperationLockRecord(read.Values[2].Value)
	if runErr != nil || lockErr != nil || validateBackupRunTaskBinding(task, run) != nil ||
		(run.RetryOfTaskID != "" && task.Actor != taskjournal.TaskActorOperator) ||
		(run.RetryOfTaskID == "" && run.Initiator == backupruntime.BackupRunInitiatorOperator && task.Actor != taskjournal.TaskActorOperator) ||
		(run.RetryOfTaskID == "" && run.Initiator == backupruntime.BackupRunInitiatorSchedule && task.Actor != taskjournal.TaskActorSystem) ||
		lock.TaskID != marker.TaskID || lock.OperationID != run.OperationID ||
		lock.EnvironmentID != run.EnvironmentID || lock.Kind != backupruntime.BackupOperationBackup ||
		!lock.CreatedAt.Equal(run.CreatedAt) || !lock.UpdatedAt.Equal(lock.CreatedAt) {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	switch task.Status {
	case taskjournal.TaskStatusPending:
		return repository.validateQueuedBackupRunPublication(
			ctx, run, task, read.Values, readRevision, commitRevision,
		)
	case taskjournal.TaskStatusRunning:
		return repository.validateRunningBackupRunPublication(
			ctx, run, task, read.Values, readRevision, commitRevision,
		)
	default:
		return backupruntime.CorruptBackupRuntimeRecord()
	}
}

// Rationale: before claim, every publication record is still the exact value
// written atomically with the pending marker and must retain that revision.
func (repository *BackupRuntimeRepository) validateQueuedBackupRunPublication(
	ctx context.Context,
	run backupruntime.BackupRunRecord,
	task TaskRecord,
	primary []*etcdstore.KeyValue,
	readRevision int64,
	commitRevision int64,
) error {
	for _, value := range primary {
		if value == nil || value.ModRevision != commitRevision {
			return backupruntime.CorruptBackupRuntimeRecord()
		}
	}
	if run.RetryOfTaskID == "" && run.Initiator == backupruntime.BackupRunInitiatorSchedule &&
		!backupscheduling.New(repository.store).HasExactPublishedOutcome(ctx, run, readRevision, commitRevision) {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	if run.State != backupruntime.BackupRunQueued || !run.UpdatedAt.Equal(run.CreatedAt) ||
		task.NextEventSequence != 1 || !task.UpdatedAt.Equal(task.CreatedAt) ||
		task.StartedAt != nil || task.FinishedAt != nil || task.RetainUntil != nil ||
		!repository.exactBackupRunPublicationSubordinates(
			ctx, run, task, readRevision, commitRevision,
		) || !repository.exactBackupRunConfigCompanions(
		ctx, run, readRevision, commitRevision,
	) {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	return nil
}

// Rationale: claim replaces only queue membership with one three-copy
// assignment. Later checkpoints may rewrite the run and Environment epoch, so
// replay proves those current records at one revision instead of requiring the
// pending publication revision everywhere.
func (repository *BackupRuntimeRepository) validateRunningBackupRunPublication(
	ctx context.Context,
	run backupruntime.BackupRunRecord,
	task TaskRecord,
	primary []*etcdstore.KeyValue,
	readRevision int64,
	publicationRevision int64,
) error {
	if primary[0].ModRevision < publicationRevision ||
		primary[1].ModRevision <= publicationRevision ||
		primary[2].ModRevision != publicationRevision ||
		(run.State != backupruntime.BackupRunQueued && run.State != backupruntime.BackupRunRunning) ||
		task.StartedAt == nil || task.FinishedAt != nil || task.RetainUntil != nil ||
		!repository.exactRunningBackupRunSubordinates(
			ctx,
			run,
			task,
			primary[0].ModRevision,
			primary[1].ModRevision,
			readRevision,
			publicationRevision,
		) || !repository.currentBackupRunConfigCompanions(
		ctx, run, readRevision, publicationRevision,
	) {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	return nil
}

// Rationale: terminal replay is authorized by the generic Task retention
// contract and the immutable Backup terminal receipt, not by queue, assignment,
// lock, or pending-marker records that the terminal transaction must remove.
func (repository *BackupRuntimeRepository) validateTerminalBackupRunPublication(
	ctx context.Context,
	marker idempotencyrecord.IdempotencyMarker,
	task TaskRecord,
	runValue *etcdstore.KeyValue,
	taskValue *etcdstore.KeyValue,
	readRevision int64,
	terminalRevision int64,
) error {
	if taskValue.ModRevision != terminalRevision || !taskjournal.IsTerminalTaskStatus(task.Status) ||
		task.FinishedAt == nil || task.RetainUntil == nil ||
		!marker.TerminalAt.Equal(*task.FinishedAt) ||
		!marker.RetainUntil.Equal(*task.RetainUntil) ||
		(marker.State == idempotencyrecord.IdempotencyMarkerCompleted && task.Status != taskjournal.TaskStatusCompleted) ||
		(marker.State == idempotencyrecord.IdempotencyMarkerFailed && task.Status == taskjournal.TaskStatusCompleted) ||
		(marker.State != idempotencyrecord.IdempotencyMarkerCompleted && marker.State != idempotencyrecord.IdempotencyMarkerFailed) {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	if runValue != nil && !repository.exactTerminalBackupRunSubordinates(
		ctx, marker, task, runValue, readRevision, terminalRevision,
	) {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	tasks, err := newTaskRepository(repository.store)
	if err != nil {
		return err
	}
	versioned := etcdstore.Versioned[TaskRecord]{
		Record: task, Revision: terminalRevision, ReadRevision: readRevision,
	}
	if err := tasks.validateTaskRetentionReplay(ctx, task, readRevision); err != nil {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	if err := tasks.validateBackupTerminalReceiptReplay(ctx, versioned); err != nil {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	return nil
}
