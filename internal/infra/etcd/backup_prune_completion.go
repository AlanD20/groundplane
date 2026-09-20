package etcd

import (
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *BackupRuntimeRepository) MarkBackupRecoveryPointPruneVerifiedAbsent(
	ctx context.Context,
	checkpointInput BackupCheckpointInput,
	dispatch etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneDispatchRecord],
	current etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord],
	next backupruntime.BackupRecoveryPointPruneRecord,
	verifiedRemoteAbsent bool,
) (etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord], error) {
	if !verifiedRemoteAbsent || dispatch.Revision <= 0 || current.Revision <= 0 ||
		backupruntime.ValidateBackupRecoveryPointPruneDispatchRecord(dispatch.Record) != nil ||
		current.Record.State != backupruntime.BackupPruneAssigned || next.State != backupruntime.BackupPruneVerifiedAbsent ||
		current.Record.Point != next.Point || current.Record.OperationID != next.OperationID ||
		current.Record.TaskID != next.TaskID || current.Record.CreatedAt != next.CreatedAt ||
		!next.UpdatedAt.After(current.Record.UpdatedAt) ||
		next.OperationID != dispatch.Record.OperationID || next.TaskID != dispatch.Record.TaskID ||
		next.Point.EnvironmentID != dispatch.Record.EnvironmentID ||
		!backupPruneDispatchContains(dispatch.Record, next.Point.ID) {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{}, errs.New(
			errs.KindValidationFailed,
			"backup prune absence checkpoint is invalid",
		)
	}
	if checkpointInput.TaskID != dispatch.Record.TaskID ||
		checkpointInput.Payload.Kind != BackupCheckpointRemoteObjectAbsent ||
		checkpointInput.Payload.PointID != next.Point.ID {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{}, errs.New(
			errs.KindValidationFailed,
			"backup prune checkpoint is invalid",
		)
	}
	checkpointOrdinal, found := backupPruneDispatchPointOrdinal(
		dispatch.Record,
		next.Point.ID,
	)
	if !found {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{}, errs.New(
			errs.KindValidationFailed,
			"backup prune checkpoint point order is invalid",
		)
	}
	value, err := backupruntime.EncodeBackupRecoveryPointPruneRecord(next)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{}, err
	}
	defer clear(value)
	authorityKeys, err := backupPruneAuthorityKeys(current.Record.Point)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{}, err
	}
	keys := append(
		[]string{backupruntime.BackupRecoveryPointPruneDispatchKey(dispatch.Record.TaskID)},
		authorityKeys...)
	keys = append(keys, backupruntime.BackupRetentionKey(current.Record.Point.SourceID, current.Record.Point.ID))
	anchor, err := repository.readCurrentKeys(ctx, keys)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{}, err
	}
	defer clearKeyValues(anchor.Values)
	if err := validateExactBackupPruneDispatchValue(anchor.Values[0], dispatch); err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{}, err
	}
	checkpointPlan, err := repository.loadBackupCheckpointPlan(
		ctx,
		checkpointInput,
		anchor.ReadRevision,
		backupCheckpointBinding{
			taskType: TaskBackupPrune, ordinal: checkpointOrdinal, pointID: next.Point.ID,
		},
	)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{}, err
	}
	defer checkpointPlan.clear()
	if anchor.Values[1] != nil {
		stored, decodeErr := backupruntime.DecodeBackupRecoveryPointPruneRecord(anchor.Values[1].Value)
		if decodeErr == nil && stored == next && anchor.Values[1].ModRevision > current.Revision &&
			checkpointPlan.duplicate &&
			anchor.Values[1].ModRevision == checkpointPlan.commitRevision &&
			allBackupRuntimeValuesAbsent(anchor.Values[2:]) {
			return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{
				Record:       stored,
				Revision:     anchor.Values[1].ModRevision,
				ReadRevision: anchor.ReadRevision,
			}, nil
		}
	}
	if checkpointPlan.duplicate {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{}, errs.New(
			errs.KindStateConflict,
			"backup prune checkpoint domain state is incomplete",
		)
	}
	if err := validatePendingBackupPruneAuthority(anchor.Values[1:6], current); err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{}, err
	}
	if err := validateCompletedBackupRetentionSweep(
		anchor.Values[6],
		current.Record.Point,
		anchor.Values[2].ModRevision,
	); err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{}, err
	}
	fence, err := loadOwnedEnvironmentMutationFence(
		ctx,
		repository.store,
		dispatch.Record.EnvironmentID,
		anchor.ReadRevision,
		environmentMutationFenceOwner{
			Kind: backupruntime.BackupOperationPrune, OperationID: dispatch.Record.OperationID,
			TaskID: dispatch.Record.TaskID,
		},
	)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{}, err
	}
	conditions := []etcdstore.Condition{{Key: keys[0], ModRevision: dispatch.Revision}}
	for index := 1; index < len(keys); index++ {
		conditions = append(
			conditions,
			etcdstore.Condition{Key: keys[index], ModRevision: anchor.Values[index].ModRevision},
		)
	}
	conditions = append(conditions, fence.transactionConditions()...)
	mutations := []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: keys[1], Value: value}}
	for _, key := range keys[2:] {
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: key})
	}
	epoch, err := fence.epochRewriteMutation()
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{}, err
	}
	defer clear(epoch.Value)
	mutations = append(mutations, epoch)
	conditions = append(conditions, checkpointPlan.conditions...)
	for _, mutation := range checkpointPlan.mutations {
		copyOfMutation := mutation
		copyOfMutation.Value = append([]byte(nil), mutation.Value...)
		mutations = append(mutations, copyOfMutation)
	}
	result, err := repository.transact(ctx, conditions, mutations)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{}, err
	}
	if !result.Succeeded {
		defer clearKeyValues(result.FailureReads)
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{}, errs.New(
			errs.KindStateConflict,
			"backup prune authority changed",
		)
	}
	return etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{
		Record: next, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

// prepareBackupPruneCompletion leaves room for the future Task terminal
// conditions and mutations while guaranteeing lock release in that same txn.
func (repository *BackupRuntimeRepository) prepareBackupPruneCompletion(
	ctx context.Context,
	dispatch etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneDispatchRecord],
	prunes []etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord],
) (backupPruneTransactionPlan, error) {
	if dispatch.Revision <= 0 ||
		backupruntime.ValidateBackupRecoveryPointPruneDispatchRecord(dispatch.Record) != nil ||
		len(prunes) != len(dispatch.Record.RecoveryPointIDs) {
		return backupPruneTransactionPlan{}, errs.New(
			errs.KindValidationFailed,
			"backup prune completion is invalid",
		)
	}
	keys := []string{backupruntime.BackupRecoveryPointPruneDispatchKey(dispatch.Record.TaskID)}
	for index, prune := range prunes {
		if prune.Revision <= 0 || prune.Record.State != backupruntime.BackupPruneVerifiedAbsent ||
			prune.Record.OperationID != dispatch.Record.OperationID ||
			prune.Record.TaskID != dispatch.Record.TaskID ||
			prune.Record.Point.ID != dispatch.Record.RecoveryPointIDs[index] ||
			prune.Record.Point.EnvironmentID != dispatch.Record.EnvironmentID {
			return backupPruneTransactionPlan{}, errs.New(
				errs.KindValidationFailed,
				"backup prune completion authority is invalid",
			)
		}
		authorityKeys, err := backupPruneAuthorityKeys(prune.Record.Point)
		if err != nil {
			return backupPruneTransactionPlan{}, err
		}
		keys = append(keys, authorityKeys...)
	}
	anchor, err := repository.readCurrentKeys(ctx, keys)
	if err != nil {
		return backupPruneTransactionPlan{}, err
	}
	defer clearKeyValues(anchor.Values)
	if err := validateExactBackupPruneDispatchValue(anchor.Values[0], dispatch); err != nil {
		return backupPruneTransactionPlan{}, err
	}
	for index, prune := range prunes {
		start := 1 + index*5
		if anchor.Values[start] == nil || anchor.Values[start].ModRevision != prune.Revision {
			return backupPruneTransactionPlan{}, errs.New(
				errs.KindStateConflict,
				"backup prune completion checkpoint changed",
			)
		}
		if !allBackupRuntimeValuesAbsent(anchor.Values[start+1 : start+5]) {
			return backupPruneTransactionPlan{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		stored, decodeErr := backupruntime.DecodeBackupRecoveryPointPruneRecord(anchor.Values[start].Value)
		if decodeErr != nil || stored != prune.Record {
			return backupPruneTransactionPlan{}, backupruntime.CorruptBackupRuntimeRecord()
		}
	}
	fence, err := loadOwnedEnvironmentMutationFence(
		ctx,
		repository.store,
		dispatch.Record.EnvironmentID,
		anchor.ReadRevision,
		environmentMutationFenceOwner{
			Kind: backupruntime.BackupOperationPrune, OperationID: dispatch.Record.OperationID,
			TaskID: dispatch.Record.TaskID,
		},
	)
	if err != nil {
		return backupPruneTransactionPlan{}, err
	}
	conditions := []etcdstore.Condition{{Key: keys[0], ModRevision: dispatch.Revision}}
	mutations := []etcdstore.Mutation{{Type: etcdstore.MutationDelete, Key: keys[0]}}
	for index, prune := range prunes {
		start := 1 + index*5
		conditions = append(conditions, etcdstore.Condition{Key: keys[start], ModRevision: prune.Revision})
		for offset := 1; offset < 5; offset++ {
			conditions = append(conditions, etcdstore.Condition{Key: keys[start+offset]})
		}
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: keys[start]})
	}
	conditions = append(conditions, fence.transactionConditions()...)
	mutations = append(mutations, etcdstore.Mutation{
		Type: etcdstore.MutationDelete, Key: hierarchyrecord.EnvironmentOperationLockKey(dispatch.Record.EnvironmentID),
	})
	epoch, err := fence.epochRewriteMutation()
	if err != nil {
		return backupPruneTransactionPlan{}, err
	}
	mutations = append(mutations, epoch)
	if err := validateBackupRuntimeTransactionBounds(conditions, mutations); err != nil {
		clearBackupRuntimeMutations(mutations)
		return backupPruneTransactionPlan{}, err
	}
	return backupPruneTransactionPlan{conditions: conditions, mutations: mutations}, nil
}
