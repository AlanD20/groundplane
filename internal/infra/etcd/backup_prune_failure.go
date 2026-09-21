package etcd

import (
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	environmentfence "github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

// Rationale: a non-success prune Task may not leave Task-owned domain state.
// Surviving objects return to stable operation-owned pending tombstones while
// already verified-absent authorities are removed before the lock is released.
func (repository *BackupRuntimeRepository) prepareBackupPruneFailure(
	ctx context.Context,
	dispatch etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneDispatchRecord],
	prunes []etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord],
	terminalAt time.Time,
) (backupPruneTransactionPlan, error) {
	if dispatch.Revision <= 0 || backupruntime.ValidateBackupRecoveryPointPruneDispatchRecord(dispatch.Record) != nil ||
		len(prunes) != len(dispatch.Record.RecoveryPointIDs) ||
		!backupruntime.ValidBackupRuntimeInstant(terminalAt) || !terminalAt.After(dispatch.Record.CreatedAt) {
		return backupPruneTransactionPlan{}, errs.New(
			errs.KindValidationFailed,
			"backup prune failure is invalid",
		)
	}
	keys := []string{backupruntime.BackupRecoveryPointPruneDispatchKey(dispatch.Record.TaskID)}
	for index, prune := range prunes {
		if prune.Revision <= 0 ||
			(prune.Record.State != backupruntime.BackupPruneAssigned && prune.Record.State != backupruntime.BackupPruneVerifiedAbsent) ||
			prune.Record.OperationID != dispatch.Record.OperationID ||
			prune.Record.TaskID != dispatch.Record.TaskID ||
			prune.Record.Point.ID != dispatch.Record.RecoveryPointIDs[index] ||
			prune.Record.Point.EnvironmentID != dispatch.Record.EnvironmentID ||
			!terminalAt.After(prune.Record.UpdatedAt) {
			return backupPruneTransactionPlan{}, errs.New(
				errs.KindValidationFailed,
				"backup prune failure authority is invalid",
			)
		}
		authorityKeys, err := backupPruneAuthorityKeys(prune.Record.Point)
		if err != nil {
			return backupPruneTransactionPlan{}, err
		}
		keys = append(keys, authorityKeys...)
	}
	anchor, err := repository.ReadCurrentKeys(ctx, keys)
	if err != nil {
		return backupPruneTransactionPlan{}, err
	}
	defer etcdstore.ClearValues(anchor.Values)
	if err := validateExactBackupPruneDispatchValue(anchor.Values[0], dispatch); err != nil {
		return backupPruneTransactionPlan{}, err
	}
	for index, prune := range prunes {
		start := 1 + index*5
		if prune.Record.State == backupruntime.BackupPruneAssigned {
			if err := validatePendingBackupPruneAuthority(anchor.Values[start:start+5], prune); err != nil {
				return backupPruneTransactionPlan{}, err
			}
			continue
		}
		if anchor.Values[start] == nil || anchor.Values[start].ModRevision != prune.Revision {
			return backupPruneTransactionPlan{}, errs.New(
				errs.KindStateConflict,
				"backup prune verified-absent authority changed",
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
	fence, err := environmentfence.LoadOwned(
		ctx,
		repository.store,
		dispatch.Record.EnvironmentID,
		anchor.ReadRevision,
		environmentfence.Owner{
			Kind: backupruntime.BackupOperationPrune, OperationID: dispatch.Record.OperationID,
			TaskID: dispatch.Record.TaskID,
		},
	)
	if err != nil {
		return backupPruneTransactionPlan{}, err
	}
	conditions := make([]etcdstore.Condition, 0, len(keys)+fence.ConditionCount())
	for index, key := range keys {
		condition := etcdstore.Condition{Key: key}
		if anchor.Values[index] != nil {
			condition.ModRevision = anchor.Values[index].ModRevision
		}
		conditions = append(conditions, condition)
	}
	mutations := make([]etcdstore.Mutation, 0, len(prunes)+3)
	for index, prune := range prunes {
		key := keys[1+index*5]
		if prune.Record.State == backupruntime.BackupPruneVerifiedAbsent {
			mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: key})
			continue
		}
		pending := prune.Record
		pending.State = backupruntime.BackupPrunePending
		pending.TaskID = ""
		pending.UpdatedAt = terminalAt
		value, encodeErr := backupruntime.EncodeBackupRecoveryPointPruneRecord(pending)
		if encodeErr != nil {
			clearBackupRuntimeMutations(mutations)
			return backupPruneTransactionPlan{}, encodeErr
		}
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: key, Value: value})
	}
	conditions = append(conditions, fence.TransactionConditions()...)
	mutations = append(
		mutations,
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: keys[0]},
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: hierarchyrecord.EnvironmentOperationLockKey(dispatch.Record.EnvironmentID)},
	)
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		clearBackupRuntimeMutations(mutations)
		return backupPruneTransactionPlan{}, err
	}
	mutations = append(mutations, epoch)
	if err := validateBackupRuntimeTransactionBounds(conditions, mutations); err != nil {
		clearBackupRuntimeMutations(mutations)
		return backupPruneTransactionPlan{}, err
	}
	return backupPruneTransactionPlan{conditions: conditions, mutations: mutations}, nil
}
