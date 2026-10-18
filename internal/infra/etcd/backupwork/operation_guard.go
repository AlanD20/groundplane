package backupwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// operationGuards binds execution to its live resource operation, independently
// of diagnostic Task history. It does not replace source/credential checks at
// the operation's effect boundaries.
func (repository *InputRepository) operationGuards(ctx context.Context, view workqueue.View,
	input *agentpb.BackupWorkInput,
) ([]keyvalue.Condition, error) {
	task := view.Task.Task
	var recordKey string
	var kind backupruntime.BackupOperationKind
	switch input.Operation {
	case agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_CAPTURE:
		recordKey, kind = backupruntime.BackupRunKey(task.ID), backupruntime.BackupOperationBackup
	case agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_RESTORE:
		recordKey, kind = backupruntime.BackupRestoreKey(task.ID), backupruntime.BackupOperationRestore
	case agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_PRUNE:
		recordKey, kind = backupruntime.BackupRecoveryPointPruneDispatchKey(task.ID), backupruntime.BackupOperationPrune
	default:
		return nil, conflict("Backup execution operation is unsupported")
	}
	keys := []string{hierarchy.EnvironmentOperationLockKey(input.EnvironmentId), recordKey}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: view.ReadRevision})
	if err != nil {
		return nil, err
	}
	if read == nil {
		return nil, conflict("Backup operation ownership is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != view.ReadRevision || len(read.Values) != len(keys) {
		return nil, conflict("Backup operation ownership snapshot is incomplete")
	}
	guards := make([]keyvalue.Condition, len(keys))
	for index, value := range read.Values {
		if value == nil || value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > read.ReadRevision {
			return nil, conflict("Backup operation no longer owns its Environment")
		}
		guards[index] = keyvalue.Condition{Key: value.Key, ModRevision: value.ModRevision}
	}
	lock, err := backupruntime.DecodeBackupOperationLockRecord(read.Values[0].Value)
	if err != nil {
		return nil, err
	}
	if lock.TaskID != task.ID || lock.EnvironmentID != input.EnvironmentId || lock.Kind != kind {
		return nil, conflict("Backup Environment lock belongs to another operation")
	}
	var operationID, environmentID, taskID string
	switch input.Operation {
	case agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_CAPTURE:
		record, err := backupruntime.DecodeBackupRunRecord(read.Values[1].Value)
		if err != nil {
			return nil, err
		}
		exclusions, err := repository.captureExclusionGuards(ctx, record, view.ReadRevision)
		if err != nil {
			return nil, err
		}
		guards = append(guards, exclusions...)
		operationID, environmentID, taskID = record.OperationID, record.EnvironmentID, record.TaskID
	case agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_RESTORE:
		record, err := backupruntime.DecodeBackupRestoreRecord(read.Values[1].Value)
		if err != nil {
			return nil, err
		}
		operationID, environmentID, taskID = record.OperationID, record.EnvironmentID, record.TaskID
	case agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_PRUNE:
		record, err := backupruntime.DecodeBackupRecoveryPointPruneDispatchRecord(read.Values[1].Value)
		if err != nil {
			return nil, err
		}
		operationID, environmentID, taskID = record.OperationID, record.EnvironmentID, record.TaskID
	}
	if operationID != lock.OperationID || environmentID != lock.EnvironmentID || taskID != lock.TaskID {
		return nil, conflict("Backup resource record differs from its Environment lock")
	}
	return guards, nil
}
