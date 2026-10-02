package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) validateCurrentRestoreTerminalAuthority(ctx context.Context,
	task TaskRecord, terminalRevision int64, receipt backupruntime.BackupTerminalReceiptRecord,
) error {
	if receipt.Restore == nil || receipt.Restore.TaskID != task.ID || receipt.Restore.OperationID != task.OperationID ||
		receipt.Restore.EnvironmentID != task.Owner.EnvironmentID {
		return errs.New(errs.KindStateConflict, "Restore terminal receipt binding changed")
	}
	environmentID := receipt.Restore.EnvironmentID
	membership, err := backupruntime.BackupRestoreEnvironmentIndexKey(environmentID, task.ID)
	if err != nil {
		return err
	}
	keys := []string{hierarchy.EnvironmentKey(environmentID), hierarchy.EnvironmentMutationEpochKey(environmentID),
		hierarchy.EnvironmentOperationLockKey(environmentID),
		deletions.TombstoneKey(string(deletions.DeletionTargetEnvironment), environmentID),
		backupruntime.BackupRecoveryPointPruneDispatchKey(task.ID), backupruntime.BackupRestoreKey(task.ID), membership}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != len(keys) {
		return errs.New(errs.KindInternal, "Restore terminal authority read is incomplete")
	}
	defer etcdstore.ClearValues(read.Values)
	ownerPresent, _, deletionOwner, err := repository.validateBackupTerminalOwnerSnapshot(
		ctx, read.ReadRevision, terminalRevision, receipt, read.Values)
	if err != nil {
		return err
	}
	if !ownerPresent {
		return nil
	}
	primary, member := read.Values[5], read.Values[6]
	if primary == nil || member == nil {
		if primary != nil || member != nil || !deletionOwner {
			return errs.New(errs.KindStateConflict, "Restore terminal authority is torn")
		}
		return nil
	}
	if primary.Key != keys[5] || primary.ModRevision != terminalRevision || member.Key != membership ||
		member.Version != 1 || member.ModRevision >= terminalRevision || string(member.Value) != task.ID {
		return errs.New(errs.KindStateConflict, "Restore terminal native revision changed")
	}
	native, err := backupruntime.DecodeBackupRestoreRecord(primary.Value)
	if err != nil || !backupruntime.BackupRestoreRecordsEqual(native, *receipt.Restore) {
		return errs.New(errs.KindStateConflict, "Restore terminal native outcome changed")
	}
	digest, err := backupruntime.BackupRestoreTerminalDomainDigest(native)
	if err != nil || digest != receipt.DomainDigest {
		return errs.New(errs.KindStateConflict, "Restore terminal native digest changed")
	}
	return nil
}
