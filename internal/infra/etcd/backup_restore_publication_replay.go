package etcd

import (
	"context"
	"encoding/hex"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/postgresbackingguard"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// A losing publication must prove the winning Restore, not compare it with
// the losing request's newly allocated Task or Entry generation.
func (repository *BackupRuntimeRepository) validateExistingRestorePublication(ctx context.Context,
	marker idempotency.IdempotencyMarker, readRevision, markerRevision int64,
) error {
	var response struct {
		TaskID string `json:"task_id"`
	}
	if marker.Kind != idempotency.IdempotencyMarkerTask || marker.TaskID == "" ||
		marker.Response.Status != 202 || json.Unmarshal(marker.Response.Body, &response) != nil ||
		response.TaskID != marker.TaskID {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	membership, err := backupruntime.BackupRestoreEnvironmentIndexKey(marker.Locator.ScopeID, marker.TaskID)
	if err != nil {
		return err
	}
	keys := []string{taskjournal.TaskStorageKey(marker.TaskID), backupruntime.BackupRestoreKey(marker.TaskID),
		backupruntime.BackupExecutionPlanKey(marker.TaskID), membership}
	read, err := repository.ReadFixedKeys(ctx, keys, readRevision)
	if err != nil {
		return err
	}
	defer etcdstore.ClearValues(read.Values)
	if len(read.Values) != len(keys) || read.Values[0] == nil {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	taskValue := read.Values[0]
	task, err := DecodeTaskRecord(taskValue.Value)
	if err != nil || task.ID != marker.TaskID || task.Type != taskjournal.TaskRestore ||
		task.Actor != taskjournal.TaskActorOperator || task.Executor != taskjournal.TaskExecutorAgent ||
		task.Owner.EnvironmentID != marker.Locator.ScopeID || task.IdempotencyKey != marker.Locator.Key ||
		task.idempotencyMarker == nil || *task.idempotencyMarker != marker.Locator {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	if marker.State != idempotency.IdempotencyMarkerPending {
		return repository.validateTerminalRestorePublication(ctx, marker, task, taskValue, readRevision, markerRevision)
	}
	for index, value := range read.Values {
		if value == nil || value.Key != keys[index] || value.ModRevision < markerRevision {
			return backupruntime.CorruptBackupRuntimeRecord()
		}
	}
	native, nativeErr := backupruntime.DecodeBackupRestoreRecord(read.Values[1].Value)
	plan, planErr := backupruntime.DecodeBackupExecutionPlan(read.Values[2].Value)
	if nativeErr != nil || planErr != nil || native.TaskID != task.ID || native.OperationID != task.OperationID ||
		native.EnvironmentID != task.Owner.EnvironmentID || !native.CreatedAt.Equal(task.CreatedAt) ||
		backupruntime.ValidateRestoreExecutionPlan(native, plan) != nil ||
		task.PlanID != plan.PlanId || task.PlanHash != hex.EncodeToString(plan.PlanHash) ||
		read.Values[2].Version != 1 || read.Values[2].ModRevision != markerRevision ||
		read.Values[3].Version != 1 || read.Values[3].ModRevision != markerRevision ||
		string(read.Values[3].Value) != task.ID || task.FinishedAt != nil || task.RetainUntil != nil {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	switch task.Status {
	case taskjournal.TaskStatusPending:
		if taskValue.ModRevision != markerRevision || read.Values[1].ModRevision != markerRevision ||
			native.State != backupruntime.BackupRestoreQueued || task.StartedAt != nil ||
			task.NextEventSequence != 1 || !task.UpdatedAt.Equal(task.CreatedAt) ||
			!native.UpdatedAt.Equal(native.CreatedAt) {
			return backupruntime.CorruptBackupRuntimeRecord()
		}
	case taskjournal.TaskStatusRunning:
		if taskValue.ModRevision <= markerRevision || read.Values[1].ModRevision <= markerRevision ||
			task.StartedAt == nil || native.State == backupruntime.BackupRestoreQueued ||
			native.State == backupruntime.BackupRestoreCompleted || native.State == backupruntime.BackupRestoreFailedSafe ||
			native.State == backupruntime.BackupRestoreRecoveryRequired {
			return backupruntime.CorruptBackupRuntimeRecord()
		}
	default:
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	_, err = environmentfence.LoadOwned(ctx, repository.store, native.EnvironmentID, readRevision,
		environmentfence.Owner{Kind: backupruntime.BackupOperationRestore, OperationID: native.OperationID,
			TaskID: native.TaskID})
	if err != nil {
		return err
	}
	backingEnvironmentID, postgres, err := backupruntime.DatabaseRestoreBackingEnvironmentID(native)
	if err != nil {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	if !postgres {
		return nil
	}
	backingGuards, err := postgresbackingguard.PrepareOwnership(
		ctx,
		repository.store,
		[]string{backingEnvironmentID},
		native.EnvironmentID,
		postgresbackingguard.Owner(backupruntime.BackupOperationRestore, native.OperationID, native.TaskID),
		readRevision,
		false,
	)
	if err != nil {
		return err
	}
	defer backingGuards.Clear()
	for _, condition := range backingGuards.Conditions {
		if condition.ModRevision != markerRevision {
			return backupruntime.CorruptBackupRuntimeRecord()
		}
	}
	return nil
}

func (repository *BackupRuntimeRepository) validateTerminalRestorePublication(ctx context.Context,
	marker idempotency.IdempotencyMarker, task TaskRecord, value *etcdstore.KeyValue,
	readRevision, terminalRevision int64,
) error {
	if value.ModRevision != terminalRevision || !taskjournal.IsTerminalTaskStatus(task.Status) ||
		task.FinishedAt == nil || task.RetainUntil == nil || !marker.TerminalAt.Equal(*task.FinishedAt) ||
		!marker.RetainUntil.Equal(*task.RetainUntil) ||
		(marker.State == idempotency.IdempotencyMarkerCompleted && task.Status != taskjournal.TaskStatusCompleted) ||
		(marker.State == idempotency.IdempotencyMarkerFailed && task.Status == taskjournal.TaskStatusCompleted) ||
		(marker.State != idempotency.IdempotencyMarkerCompleted && marker.State != idempotency.IdempotencyMarkerFailed) {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	tasks, err := newTaskRepository(repository.store)
	if err != nil {
		return err
	}
	if err := tasks.validateTaskRetentionReplay(ctx, task, readRevision); err != nil {
		return err
	}
	return tasks.validateBackupTerminalReceiptReplay(ctx, etcdstore.Versioned[TaskRecord]{
		Record: task, Revision: terminalRevision, ReadRevision: readRevision})
}
