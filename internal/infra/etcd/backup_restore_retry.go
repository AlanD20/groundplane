package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type PreparedRestoreRetrySource struct {
	Task TaskRecord
	backupruntime.RestoreRetrySnapshot
}

func (repository *BackupRuntimeRepository) PrepareRestoreRetrySource(ctx context.Context,
	sourceTaskID, retryTaskID string, at time.Time,
) (PreparedRestoreRetrySource, error) {
	var zero PreparedRestoreRetrySource
	if repository == nil || repository.store == nil ||
		ids.Validate(ids.KindTask, sourceTaskID) != nil || ids.Validate(ids.KindTask, retryTaskID) != nil ||
		sourceTaskID == retryTaskID || !backupruntime.ValidBackupRuntimeInstant(at) {
		return zero, errs.New(errs.KindValidationFailed, "Restore retry source is invalid")
	}
	keys := []string{taskjournal.TaskStorageKey(sourceTaskID), backupruntime.BackupRestoreKey(sourceTaskID),
		backupruntime.BackupTerminalReceiptKey(sourceTaskID), backupruntime.BackupExecutionPlanKey(sourceTaskID)}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return zero, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != len(keys) || read.Values[0] == nil {
		return zero, errs.New(errs.KindTaskNotRetryable, "Restore retry source is incomplete")
	}
	defer etcdstore.ClearValues(read.Values)
	task, err := DecodeTaskRecord(read.Values[0].Value)
	if err != nil || task.ID != sourceTaskID || task.Result != nil && task.Result.ReconciliationRequired {
		return zero, errs.New(errs.KindTaskNotRetryable, "Restore retry Task is not safe")
	}
	evidence, err := backupTerminalTaskEvidence(task)
	if err != nil {
		return zero, err
	}
	snapshot, err := backupruntime.DecodeRestoreRetrySnapshot(evidence, retryTaskID, read.Values, at)
	if err != nil {
		return zero, err
	}
	tasks, err := newTaskRepository(repository.store)
	if err != nil {
		return zero, err
	}
	if err := tasks.validateBackupTerminalReceiptReplay(ctx, etcdstore.Versioned[TaskRecord]{
		Record: task, Revision: read.Values[0].ModRevision, ReadRevision: read.ReadRevision,
	}); err != nil {
		return zero, err
	}
	return PreparedRestoreRetrySource{Task: cloneTaskRecord(task), RestoreRetrySnapshot: snapshot}, nil
}
