package etcd

import (
	"context"
	backupplanning "github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	environmentfence "github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// prepareBackupPrunePublication composes retained prune authority and the
// exact Environment lock with the future Task publication transaction.
func (repository *BackupRuntimeRepository) prepareBackupPrunePublication(
	ctx context.Context,
	pending []etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord],
	dispatch backupruntime.BackupRecoveryPointPruneDispatchRecord,
	lock backupruntime.BackupOperationLockRecord,
) (backupPruneTransactionPlan, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return backupPruneTransactionPlan{}, err
	}
	if backupruntime.ValidateBackupRecoveryPointPruneDispatchRecord(dispatch) != nil ||
		len(pending) == 0 || len(pending) > backupruntime.MaximumBackupPruneBatch ||
		len(pending) != len(dispatch.RecoveryPointIDs) ||
		lock.EnvironmentID != dispatch.EnvironmentID || lock.OperationID != dispatch.OperationID ||
		lock.TaskID != dispatch.TaskID || lock.Kind != backupruntime.BackupOperationPrune ||
		lock.CreatedAt != dispatch.CreatedAt || lock.UpdatedAt != dispatch.CreatedAt {
		return backupPruneTransactionPlan{}, errs.New(
			errs.KindValidationFailed,
			"backup prune publication is invalid",
		)
	}
	dispatchValue, err := backupruntime.EncodeBackupRecoveryPointPruneDispatchRecord(dispatch)
	if err != nil {
		return backupPruneTransactionPlan{}, err
	}
	lockValue, err := backupruntime.EncodeBackupOperationLockRecord(lock)
	if err != nil {
		clear(dispatchValue)
		return backupPruneTransactionPlan{}, err
	}
	dispatchKey := backupruntime.BackupRecoveryPointPruneDispatchKey(dispatch.TaskID)
	keys := []string{dispatchKey}
	assignedValues := make([][]byte, len(pending))
	defer func() {
		for index := range assignedValues {
			clear(assignedValues[index])
		}
	}()
	for index := range pending {
		version := pending[index]
		record := version.Record
		if version.Revision <= 0 || record.State != backupruntime.BackupPrunePending || record.TaskID != "" ||
			record.DispatchAttempts >= backupruntime.MaximumBackupPruneDispatchAttempts ||
			record.OperationID != dispatch.OperationID ||
			(index > 0 && (record.PolicyRevision != pending[0].Record.PolicyRevision || record.PolicySHA256 != pending[0].Record.PolicySHA256)) ||
			record.Point.EnvironmentID != dispatch.EnvironmentID ||
			record.Point.ID != dispatch.RecoveryPointIDs[index] ||
			dispatch.CreatedAt.Before(record.UpdatedAt) {
			clear(dispatchValue)
			clear(lockValue)
			return backupPruneTransactionPlan{}, errs.New(
				errs.KindValidationFailed,
				"backup prune publication authority is invalid",
			)
		}
		authorityKeys, keyErr := backupruntime.BackupPruneAuthorityKeys(record.Point)
		if keyErr != nil {
			clear(dispatchValue)
			clear(lockValue)
			return backupPruneTransactionPlan{}, keyErr
		}
		keys = append(keys, authorityKeys...)
		assigned := record
		assigned.State = backupruntime.BackupPruneAssigned
		assigned.DispatchAttempts++
		assigned.TaskID = dispatch.TaskID
		assigned.UpdatedAt = dispatch.CreatedAt
		assignedValues[index], err = backupruntime.EncodeBackupRecoveryPointPruneRecord(assigned)
		if err != nil {
			clear(dispatchValue)
			clear(lockValue)
			return backupPruneTransactionPlan{}, err
		}
	}
	anchor, err := repository.ReadCurrentKeys(ctx, keys)
	if err != nil {
		clear(dispatchValue)
		clear(lockValue)
		return backupPruneTransactionPlan{}, err
	}
	defer etcdstore.ClearValues(anchor.Values)
	if anchor.Values[0] != nil {
		clear(dispatchValue)
		clear(lockValue)
		return backupPruneTransactionPlan{}, errs.New(
			errs.KindStateConflict,
			"backup prune dispatch already exists",
		)
	}
	for index := range pending {
		start := 1 + index*5
		if err := backupruntime.ValidatePendingBackupPruneAuthority(
			anchor.Values[start:start+5],
			pending[index],
		); err != nil {
			clear(dispatchValue)
			clear(lockValue)
			return backupPruneTransactionPlan{}, err
		}
	}
	planEvidence, err := repository.loadBackupPruneExecutionEvidence(
		ctx,
		pending,
		anchor.Values,
		anchor.ReadRevision,
	)
	if err != nil {
		clear(dispatchValue)
		clear(lockValue)
		return backupPruneTransactionPlan{}, err
	}
	fence, err := environmentfence.LoadOrdinary(
		ctx,
		repository.store,
		dispatch.EnvironmentID,
		anchor.ReadRevision,
	)
	if err != nil {
		clear(dispatchValue)
		clear(lockValue)
		return backupPruneTransactionPlan{}, err
	}
	conditions := []etcdstore.Condition{{Key: dispatchKey}}
	mutations := make([]etcdstore.Mutation, 0, len(pending)+3)
	for index := range pending {
		start := 1 + index*5
		for offset := range 5 {
			conditions = append(conditions, etcdstore.Condition{
				Key: keys[start+offset], ModRevision: anchor.Values[start+offset].ModRevision,
			})
		}
		mutations = append(mutations, etcdstore.Mutation{
			Type:  etcdstore.MutationPut,
			Key:   keys[start],
			Value: append([]byte(nil), assignedValues[index]...),
		})
	}
	conditions = append(conditions, fence.TransactionConditions()...)
	mutations = append(mutations,
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: dispatchKey, Value: dispatchValue},
		etcdstore.Mutation{
			Type:  etcdstore.MutationPut,
			Key:   hierarchyrecord.EnvironmentOperationLockKey(dispatch.EnvironmentID),
			Value: lockValue,
		},
	)
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		etcdstore.ClearMutationValues(mutations)
		return backupPruneTransactionPlan{}, err
	}
	mutations = append(mutations, epoch)
	if err := backupruntime.ValidateBackupRuntimeTransactionBounds(conditions, mutations); err != nil {
		etcdstore.ClearMutationValues(mutations)
		return backupPruneTransactionPlan{}, err
	}
	return backupPruneTransactionPlan{
		conditions: conditions,
		mutations:  mutations,
		evidence:   planEvidence,
		authority: &backupTaskPublicationAuthority{
			taskID: dispatch.TaskID, operationID: dispatch.OperationID,
			environmentID: dispatch.EnvironmentID, taskType: taskjournal.TaskBackupPrune,
			createdAt: dispatch.CreatedAt,
			validatePlan: func(value *agentpb.ExecutionPlan) error {
				return backupplanning.ValidateBackupPruneExecutionPlan(dispatch, planEvidence, value)
			},
		},
		readRevision: anchor.ReadRevision,
	}, nil
}
