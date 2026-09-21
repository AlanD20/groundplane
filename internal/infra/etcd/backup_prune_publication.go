package etcd

import (
	"context"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
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
			record.OperationID != dispatch.OperationID ||
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
		authorityKeys, keyErr := backupPruneAuthorityKeys(record.Point)
		if keyErr != nil {
			clear(dispatchValue)
			clear(lockValue)
			return backupPruneTransactionPlan{}, keyErr
		}
		keys = append(keys, authorityKeys...)
		assigned := record
		assigned.State = backupruntime.BackupPruneAssigned
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
		if err := validatePendingBackupPruneAuthority(
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
		clearBackupRuntimeMutations(mutations)
		return backupPruneTransactionPlan{}, err
	}
	mutations = append(mutations, epoch)
	if err := validateBackupRuntimeTransactionBounds(conditions, mutations); err != nil {
		clearBackupRuntimeMutations(mutations)
		return backupPruneTransactionPlan{}, err
	}
	return backupPruneTransactionPlan{
		conditions: conditions,
		mutations:  mutations,
		authority: &backupTaskPublicationAuthority{
			taskID: dispatch.TaskID, operationID: dispatch.OperationID,
			environmentID: dispatch.EnvironmentID, taskType: taskjournal.TaskBackupPrune,
			createdAt: dispatch.CreatedAt,
			validatePlan: func(value *agentpb.ExecutionPlan) error {
				return validateBackupPruneExecutionPlan(dispatch, planEvidence, value)
			},
		},
		readRevision: anchor.ReadRevision,
	}, nil
}

func (repository *BackupRuntimeRepository) loadBackupPruneExecutionEvidence(
	ctx context.Context,
	pending []etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord],
	authorityValues []*etcdstore.KeyValue,
	readRevision int64,
) ([]backupPruneExecutionEvidence, error) {
	evidence := make([]backupPruneExecutionEvidence, len(pending))
	for index, version := range pending {
		start := 1 + index*5
		if start+4 >= len(authorityValues) || authorityValues[start+1] == nil {
			return nil, backupruntime.CorruptBackupRuntimeRecord()
		}
		point, err := backupruntime.DecodeBackupRecoveryPointRecord(authorityValues[start+1].Value)
		if err != nil || point.BackupRecoveryPointSnapshot != version.Record.Point {
			return nil, backupruntime.CorruptBackupRuntimeRecord()
		}
		fixed, err := repository.ReadFixedKeys(ctx, []string{
			backuppolicy.BackupSourceKey(point.SourceID),
			hierarchyrecord.EnvironmentKey(point.EnvironmentID),
			connectorrecord.RecordKey(point.ConnectorID),
		}, readRevision)
		if err != nil {
			return nil, err
		}
		if len(fixed.Values) != 3 || fixed.Values[0] == nil || fixed.Values[1] == nil || fixed.Values[2] == nil {
			etcdstore.ClearValues(fixed.Values)
			return nil, errs.New(errs.KindStateConflict, "backup prune plan evidence is missing")
		}
		source, sourceErr := backuppolicy.DecodeBackupSourceRecord(fixed.Values[0].Value)
		environment, environmentErr := hierarchyrecord.DecodeEnvironment(fixed.Values[1].Value)
		connector, connectorErr := connectorrecord.DecodeRecord(fixed.Values[2].Value)
		if sourceErr != nil || environmentErr != nil || connectorErr != nil ||
			source.ID != point.SourceID || source.EnvironmentID != point.EnvironmentID ||
			environment.ID != point.EnvironmentID || connector.Connector.ID != point.ConnectorID ||
			connector.Connector.EnvironmentID != point.EnvironmentID {
			etcdstore.ClearValues(fixed.Values)
			return nil, errs.New(errs.KindStateConflict, "backup prune plan evidence changed")
		}
		evidence[index] = backupPruneExecutionEvidence{
			prune: version.Record, pruneRevision: version.Revision,
			pointRevision:       authorityValues[start+1].ModRevision,
			sourceRevision:      fixed.Values[0].ModRevision,
			environmentRevision: fixed.Values[1].ModRevision,
			connectorRevision:   fixed.Values[2].ModRevision,
			connectorEndpoint:   connector.Connector.Endpoint,
			connectorBucket:     connector.Connector.Bucket,
			connectorPrefix:     connector.Connector.Prefix,
			connectorRegion:     connector.Connector.Region,
			connectorPathStyle:  connector.Connector.PathStyle,
		}
		etcdstore.ClearValues(fixed.Values)
	}
	return evidence, nil
}
