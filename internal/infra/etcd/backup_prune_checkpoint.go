package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CheckpointBackupPrune publishes exact remote absence and relinquishes the
// matching point ownership in one transaction with the acknowledged checkpoint.
func (repository *BackupRuntimeRepository) CheckpointBackupPrune(ctx context.Context,
	input backupruntime.BackupCheckpointInput, at time.Time,
) (int64, error) {
	if err := backupruntime.ValidateBackupCheckpointInput(input); err != nil {
		return 0, err
	}
	deleted := input.Request.GetPruneObjectDeleted()
	if deleted == nil || !backupruntime.ValidBackupRuntimeInstant(at) {
		return 0, errs.New(errs.KindValidationFailed, "backup prune checkpoint requires exact object absence")
	}
	keys := []string{backupruntime.BackupRecoveryPointPruneDispatchKey(input.TaskID),
		backupruntime.BackupRecoveryPointPruneKey(deleted.PointId)}
	read, err := repository.ReadCurrentKeys(ctx, keys)
	if err != nil {
		return 0, err
	}
	defer etcdstore.ClearValues(read.Values)
	if read.Values[0] == nil || read.Values[1] == nil {
		return 0, errs.New(errs.KindStateConflict, "backup prune ownership is missing")
	}
	dispatch, err := backupruntime.DecodeBackupRecoveryPointPruneDispatchRecord(read.Values[0].Value)
	if err != nil || dispatch.TaskID != input.TaskID {
		return 0, backupruntime.CorruptBackupRuntimeRecord()
	}
	prune, err := backupruntime.DecodeBackupRecoveryPointPruneRecord(read.Values[1].Value)
	if err != nil || prune.Point.ID != deleted.PointId || prune.TaskID != input.TaskID {
		return 0, backupruntime.CorruptBackupRuntimeRecord()
	}
	next := prune
	next.State, next.UpdatedAt = backupruntime.BackupPruneVerifiedAbsent, at
	if !next.UpdatedAt.After(prune.UpdatedAt) {
		next.UpdatedAt = prune.UpdatedAt.Add(time.Nanosecond)
	}
	updated, err := repository.MarkBackupRecoveryPointPruneVerifiedAbsent(ctx, input,
		etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneDispatchRecord]{Record: dispatch,
			Revision: read.Values[0].ModRevision, ReadRevision: read.ReadRevision},
		etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{Record: prune,
			Revision: read.Values[1].ModRevision, ReadRevision: read.ReadRevision}, next, true)
	if err != nil {
		return 0, err
	}
	return updated.Revision, nil
}
