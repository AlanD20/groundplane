package etcd

import (
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	coordinationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentcoordination"
	environmentfence "github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *BackupRuntimeRepository) loadOwnedEvidence(
	ctx context.Context,
	run backupruntime.BackupRunRecord,
	revision int64,
) (backupRuntimeOwnedEvidence, error) {
	fence, err := environmentfence.LoadOwned(
		ctx,
		repository.store,
		run.EnvironmentID,
		revision,
		environmentfence.Owner{
			Kind: backupruntime.BackupOperationBackup, OperationID: run.OperationID, TaskID: run.TaskID,
		},
	)
	if err != nil {
		return backupRuntimeOwnedEvidence{}, err
	}
	return backupRuntimeOwnedEvidence{fence: fence}, nil
}

func (repository *BackupRuntimeRepository) loadManualBackupPolicyFence(
	ctx context.Context,
	record backupruntime.BackupRunRecord,
	fixedRevision int64,
) ([]etcdstore.Condition, error) {
	if record.Initiator != backupruntime.BackupRunInitiatorOperator {
		return nil, nil
	}
	key := coordinationrecord.Key(record.EnvironmentID)
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{key}, Revision: fixedRevision,
	})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != fixedRevision || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].ModRevision <= 0 {
		return nil, errs.New(errs.KindStateConflict, "backup policy schedule coordination changed")
	}
	defer etcdstore.ClearValues(read.Values)
	coordination, err := coordinationrecord.Decode(read.Values[0].Value)
	if err != nil || coordination.EnvironmentID != record.EnvironmentID ||
		coordination.CurrentBackupScheduleState == nil {
		return nil, errs.New(errs.KindStateConflict, "backup policy schedule coordination is invalid")
	}
	return []etcdstore.Condition{{Key: key, ModRevision: read.Values[0].ModRevision}}, nil
}
