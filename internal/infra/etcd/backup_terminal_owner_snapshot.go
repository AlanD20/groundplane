package etcd

import (
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) validateBackupTerminalOwnerSnapshot(
	ctx context.Context,
	readRevision int64,
	terminalRevision int64,
	receipt backupruntime.BackupTerminalReceiptRecord,
	values []*etcdstore.KeyValue,
) (bool, *backupruntime.BackupOperationLockRecord, bool, error) {
	if len(values) < 5 {
		return false, nil, false, errs.New(errs.KindInternal, "backup terminal owner snapshot is incomplete")
	}
	environmentValue, epochValue, lockValue, tombstoneValue, dispatchValue :=
		values[0], values[1], values[2], values[3], values[4]
	if environmentValue == nil {
		if !allBackupRuntimeValuesAbsent(values[1:]) {
			return false, nil, false, errs.New(
				errs.KindStateConflict,
				"deleted backup owner retained terminal authority",
			)
		}
		return false, nil, false, nil
	}
	environment, err := hierarchyrecord.DecodeEnvironment(environmentValue.Value)
	if err != nil {
		return false, nil, false, err
	}
	if environment.ID != receipt.Task.Owner.EnvironmentID {
		return false, nil, false, errs.New(errs.KindStateConflict, "terminal backup Environment binding changed")
	}
	if epochValue == nil || epochValue.ModRevision < terminalRevision {
		return false, nil, false, errs.New(errs.KindStateConflict, "terminal backup Environment epoch changed")
	}
	epoch, err := backupruntime.DecodeEnvironmentMutationEpochRecord(epochValue.Value)
	if err != nil {
		return false, nil, false, err
	}
	if epoch.EnvironmentID != environment.ID {
		return false, nil, false, errs.New(errs.KindStateConflict, "terminal backup Environment epoch changed")
	}
	if dispatchValue != nil {
		return false, nil, false, errs.New(errs.KindStateConflict, "terminal backup prune dispatch remains")
	}
	if lockValue == nil {
		if tombstoneValue != nil {
			return false, nil, false, errs.New(errs.KindStateConflict, "terminal backup deletion authority is torn")
		}
		return true, nil, false, nil
	}
	lock, err := backupruntime.DecodeBackupOperationLockRecord(lockValue.Value)
	if err != nil {
		return false, nil, false, err
	}
	if lock.EnvironmentID != environment.ID || lockValue.ModRevision <= terminalRevision ||
		lock.TaskID == receipt.Task.TaskID {
		return false, nil, false, errs.New(errs.KindStateConflict, "stale terminal backup lock remains")
	}
	if lock.Kind != backupruntime.BackupOperationDeletion {
		if tombstoneValue != nil {
			return false, nil, false, errs.New(errs.KindStateConflict, "terminal backup deletion authority is torn")
		}
		return true, &lock, false, nil
	}
	if tombstoneValue == nil {
		return false, nil, false, errs.New(errs.KindStateConflict, "terminal backup deletion authority is torn")
	}
	tombstone, err := deletionrecord.DecodeDeletionTombstone(tombstoneValue.Value)
	if err != nil {
		return false, nil, false, err
	}
	if tombstone.TargetKind != deletionrecord.DeletionTargetEnvironment || tombstone.TargetID != environment.ID ||
		tombstone.TargetRevision != environmentValue.ModRevision || tombstone.TaskID != lock.TaskID {
		return false, nil, false, errs.New(errs.KindStateConflict, "terminal backup deletion authority changed")
	}
	intentRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{environmentDeletionIntentKey(lock.OperationID)}, Revision: readRevision,
	})
	if err != nil {
		return false, nil, false, err
	}
	if intentRead == nil || intentRead.ReadRevision != readRevision || len(intentRead.Values) != 1 ||
		intentRead.Values[0] == nil {
		if intentRead != nil {
			clearKeyValues(intentRead.Values)
		}
		return false, nil, false, errs.New(errs.KindStateConflict, "terminal backup deletion intent is missing")
	}
	defer clearKeyValues(intentRead.Values)
	intent, err := decodeEnvironmentDeletionIntent(intentRead.Values[0].Value)
	if err != nil {
		return false, nil, false, err
	}
	if intent.EnvironmentID != environment.ID || intent.OperationID != lock.OperationID ||
		intent.TaskID != lock.TaskID || intent.TargetRevision != tombstone.TargetRevision ||
		!intent.CreatedAt.Equal(tombstone.CreatedAt) {
		return false, nil, false, errs.New(errs.KindStateConflict, "terminal backup deletion intent changed")
	}
	return true, &lock, true, nil
}
