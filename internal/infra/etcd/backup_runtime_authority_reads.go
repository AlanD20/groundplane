package etcd

import (
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	coordinationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentcoordination"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *BackupRuntimeRepository) GetBackupSourceTargetExclusion(
	ctx context.Context,
	kind backupruntime.BackupSourceTargetKind,
	targetID string,
) (etcdstore.Versioned[backupruntime.BackupSourceTargetExclusionRecord], bool, error) {
	key, err := backupruntime.BackupSourceTargetExclusionKey(kind, targetID)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupSourceTargetExclusionRecord]{}, false, err
	}
	result, err := repository.store.Get(ctx, key)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupSourceTargetExclusionRecord]{}, false, err
	}
	if result == nil {
		return etcdstore.Versioned[backupruntime.BackupSourceTargetExclusionRecord]{}, false, errs.New(
			errs.KindInternal,
			"backup source-target exclusion read is empty",
		)
	}
	if result.Entry == nil {
		return etcdstore.Versioned[backupruntime.BackupSourceTargetExclusionRecord]{
			ReadRevision: result.ReadRevision,
		}, false, nil
	}
	defer clear(result.Entry.Value)
	record, err := backupruntime.DecodeBackupSourceTargetExclusionRecord(result.Entry.Value)
	if err != nil || record.TargetKind != kind || record.TargetID != targetID {
		return etcdstore.Versioned[backupruntime.BackupSourceTargetExclusionRecord]{}, false, backupruntime.CorruptBackupRuntimeRecord()
	}
	return etcdstore.Versioned[backupruntime.BackupSourceTargetExclusionRecord]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}

func (repository *BackupRuntimeRepository) loadOwnedEvidence(
	ctx context.Context,
	run backupruntime.BackupRunRecord,
	revision int64,
) (backupRuntimeOwnedEvidence, error) {
	fence, err := loadOwnedEnvironmentMutationFence(
		ctx,
		repository.store,
		run.EnvironmentID,
		revision,
		environmentMutationFenceOwner{
			Kind: backupruntime.BackupOperationBackup, OperationID: run.OperationID, TaskID: run.TaskID,
		},
	)
	if err != nil {
		return backupRuntimeOwnedEvidence{}, err
	}
	return backupRuntimeOwnedEvidence{fence: fence}, nil
}

func (repository *BackupRuntimeRepository) readCurrentKeys(
	ctx context.Context,
	keys []string,
) (*etcdstore.GetManyResult, error) {
	if len(keys) == 0 || len(keys) > etcdstore.MaximumOperations {
		return nil, errs.New(
			errs.KindValidationFailed,
			"backup runtime fixed read key count is invalid",
		)
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return nil, err
	}
	if result == nil || result.ReadRevision <= 0 || len(result.Values) != len(keys) {
		return nil, errs.New(errs.KindInternal, "backup runtime fixed-revision read is incomplete")
	}
	for index, value := range result.Values {
		if value != nil && value.Key != keys[index] {
			clearKeyValues(result.Values)
			return nil, errs.New(errs.KindInternal, "backup runtime fixed-revision read is corrupt")
		}
	}
	return result, nil
}

func (repository *BackupRuntimeRepository) readFixedKeys(
	ctx context.Context,
	keys []string,
	revision int64,
) (*etcdstore.GetManyResult, error) {
	if len(keys) == 0 || len(keys) > etcdstore.MaximumOperations || revision <= 0 {
		return nil, errs.New(
			errs.KindValidationFailed,
			"backup runtime fixed read input is invalid",
		)
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if result == nil || result.ReadRevision != revision || len(result.Values) != len(keys) {
		return nil, errs.New(errs.KindInternal, "backup runtime fixed-revision read is incomplete")
	}
	for index, value := range result.Values {
		if value != nil && value.Key != keys[index] {
			clearKeyValues(result.Values)
			return nil, errs.New(errs.KindInternal, "backup runtime fixed-revision read is corrupt")
		}
	}
	return result, nil
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
	defer clearKeyValues(read.Values)
	coordination, err := coordinationrecord.Decode(read.Values[0].Value)
	if err != nil || coordination.EnvironmentID != record.EnvironmentID ||
		coordination.CurrentBackupScheduleState == nil {
		return nil, errs.New(errs.KindStateConflict, "backup policy schedule coordination is invalid")
	}
	return []etcdstore.Condition{{Key: key, ModRevision: read.Values[0].ModRevision}}, nil
}
