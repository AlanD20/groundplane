package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// A recovery-required Restore still owns immutable procedure and checkpoint
// evidence. A failed PostgreSQL Backup or safe-failed Restore also retains its
// Task while an exact backing guard awaits attested startup cleanup.
func (repository *TaskRepository) backupRestoreTaskPruneHeld(ctx context.Context,
	task TaskRecord, taskRevision, readRevision int64,
) (bool, error) {
	if task.Type != taskjournal.TaskBackup && task.Type != taskjournal.TaskRestore {
		return false, nil
	}
	nativeKey := backupruntime.BackupRestoreKey(task.ID)
	if task.Type == taskjournal.TaskBackup {
		nativeKey = backupruntime.BackupRunKey(task.ID)
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{nativeKey}, Revision: readRevision,
	})
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision != readRevision || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].ModRevision != taskRevision {
		return false, errs.New(errs.KindStateConflict, "Backup retention authority is incomplete")
	}
	defer etcdstore.ClearValues(read.Values)
	var environmentIDs []string
	var ownerKind backupruntime.BackupOperationKind
	if task.Type == taskjournal.TaskBackup {
		run, err := backupruntime.DecodeBackupRunRecord(read.Values[0].Value)
		if err != nil || ValidateBackupRunTaskBinding(task, run) != nil ||
			!backupruntime.TerminalBackupRunState(run.State) {
			return false, errs.New(errs.KindStateConflict, "Backup retention authority changed")
		}
		_, environmentIDs, err = backupruntime.PostgresBackingEnvironmentTerminalGuards(
			run,
			task.TerminalAssignment == nil,
		)
		if err != nil {
			return false, err
		}
		ownerKind = backupruntime.BackupOperationBackup
	} else {
		restored, err := backupruntime.DecodeBackupRestoreRecord(read.Values[0].Value)
		if err != nil || restored.TaskID != task.ID || restored.OperationID != task.OperationID ||
			restored.EnvironmentID != task.Owner.EnvironmentID {
			return false, errs.New(errs.KindStateConflict, "Restore retention authority changed")
		}
		if restored.State == backupruntime.BackupRestoreRecoveryRequired {
			return true, nil
		}
		if restored.State != backupruntime.BackupRestoreFailedSafe || task.TerminalAssignment == nil {
			return false, nil
		}
		environmentID, postgres, err := backupruntime.PostgresRestoreBackingEnvironmentID(restored)
		if err != nil {
			return false, err
		}
		if postgres {
			environmentIDs = []string{environmentID}
		}
		ownerKind = backupruntime.BackupOperationRestore
	}
	if len(environmentIDs) == 0 {
		return false, nil
	}
	keys := make([]string, len(environmentIDs))
	for index, environmentID := range environmentIDs {
		keys[index] = hierarchy.EnvironmentOperationLockKey(environmentID)
	}
	locks, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: readRevision})
	if err != nil {
		return false, err
	}
	if locks == nil || locks.ReadRevision != readRevision || len(locks.Values) != len(keys) {
		return false, errs.New(errs.KindStateConflict, "PostgreSQL backing guard retention authority is incomplete")
	}
	defer etcdstore.ClearValues(locks.Values)
	for index, value := range locks.Values {
		if value == nil {
			continue
		}
		lock, err := backupruntime.DecodeBackupOperationLockRecord(value.Value)
		if err != nil {
			return false, err
		}
		if lock.EnvironmentID == environmentIDs[index] && lock.OperationID == task.OperationID &&
			lock.TaskID == task.ID && lock.Kind == ownerKind {
			return true, nil
		}
	}
	return false, nil
}
