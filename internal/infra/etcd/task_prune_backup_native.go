package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

func (repository *TaskRepository) backupHierarchyDeletionTaskPruneHeld(ctx context.Context,
	task TaskRecord,
	readRevision int64,
) (bool, error) {
	if task.Type != taskjournal.TaskBackup && task.Type != taskjournal.TaskRestore &&
		task.Type != taskjournal.TaskRotate {
		return false, nil
	}
	keys := make([]string, 0, 3)
	if task.Owner.EnvironmentID != "" {
		keys = append(keys, hierarchydeletion.HierarchyDeletionTombstoneKey("environment", task.Owner.EnvironmentID))
	}
	if task.Owner.ProjectID != "" {
		keys = append(keys, hierarchydeletion.HierarchyDeletionTombstoneKey("project", task.Owner.ProjectID))
		keys = append(keys, hierarchydeletion.HierarchyDeletionTombstoneKey("backing-service", task.Owner.ProjectID))
	}
	if task.Owner.TenantID != "" {
		keys = append(keys, hierarchydeletion.HierarchyDeletionTombstoneKey("tenant", task.Owner.TenantID))
	}
	if len(keys) == 0 {
		return false, nil
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: readRevision})
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision != readRevision || len(read.Values) != len(keys) {
		return false, taskjournal.CorruptPruneIntent()
	}
	defer keyvalue.ClearValues(read.Values)
	for _, value := range read.Values {
		if value != nil {
			return true, nil
		}
	}
	return false, nil
}

func (repository *TaskRepository) prepareBackupNativeTaskPrune(ctx context.Context,
	task TaskRecord,
	readRevision int64,
) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	var primaryKey, indexKey string
	switch task.Type {
	case taskjournal.TaskBackup:
		primaryKey = backupruntime.BackupRunKey(task.ID)
		indexKey, _ = backupruntime.BackupRunEnvironmentIndexKey(task.Owner.EnvironmentID, task.ID)
	case taskjournal.TaskRestore:
		primaryKey = backupruntime.BackupRestoreKey(task.ID)
		indexKey, _ = backupruntime.BackupRestoreEnvironmentIndexKey(task.Owner.EnvironmentID, task.ID)
	case taskjournal.TaskRotate:
		primaryKey = backupruntime.BackupKeyRotationKey(task.ID)
		indexKey, _ = backupruntime.BackupKeyRotationEnvironmentIndexKey(task.Owner.EnvironmentID, task.ID)
	default:
		return nil, nil, nil
	}
	if primaryKey == "" || indexKey == "" {
		return nil, nil, taskjournal.CorruptPruneIntent()
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{primaryKey, indexKey}, Revision: readRevision,
	})
	if err != nil {
		return nil, nil, err
	}
	if read == nil || read.ReadRevision != readRevision || len(read.Values) != 2 || read.Values[0] == nil {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return nil, nil, taskjournal.CorruptPruneIntent()
	}
	defer keyvalue.ClearValues(read.Values)
	valid := false
	switch task.Type {
	case taskjournal.TaskBackup:
		record, decodeErr := backupruntime.DecodeBackupRunRecord(read.Values[0].Value)
		valid = decodeErr == nil && record.TaskID == task.ID && record.OperationID == task.OperationID &&
			record.EnvironmentID == task.Owner.EnvironmentID && backupruntime.TerminalBackupRunState(record.State)
	case taskjournal.TaskRestore:
		record, decodeErr := backupruntime.DecodeBackupRestoreRecord(read.Values[0].Value)
		valid = decodeErr == nil && record.TaskID == task.ID && record.OperationID == task.OperationID &&
			record.EnvironmentID == task.Owner.EnvironmentID &&
			(record.State == backupruntime.BackupRestoreCompleted || record.State == backupruntime.BackupRestoreFailedSafe ||
				record.State == backupruntime.BackupRestoreRecoveryRequired)
	case taskjournal.TaskRotate:
		record, decodeErr := backupruntime.DecodeBackupKeyRotationRecord(read.Values[0].Value)
		valid = decodeErr == nil && record.TaskID == task.ID && record.OperationID == task.OperationID &&
			record.EnvironmentID == task.Owner.EnvironmentID
	}
	if !valid || read.Values[1] != nil && string(read.Values[1].Value) != task.ID {
		return nil, nil, taskjournal.CorruptPruneIntent()
	}
	conditions := []keyvalue.Condition{{Key: primaryKey, ModRevision: read.Values[0].ModRevision},
		{Key: indexKey, ModRevision: keyvalue.RevisionOf(read.Values[1])}}
	mutations := []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: primaryKey}}
	if read.Values[1] != nil {
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: indexKey})
	}
	return conditions, mutations, nil
}
