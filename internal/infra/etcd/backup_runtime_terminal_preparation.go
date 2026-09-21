package etcd

import (
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	environmentfence "github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// prepareBackupRunTerminal composes the terminal run checkpoint, complete
// exclusion release, exact operation-lock release, and Environment epoch
// advance with the caller's terminal Task and idempotency mutations.
func (repository *BackupRuntimeRepository) prepareBackupRunTerminal(
	ctx context.Context,
	current etcdstore.Versioned[backupruntime.BackupRunRecord],
	next backupruntime.BackupRunRecord,
) (backupRunPublicationPlan, error) {
	return repository.prepareBackupRunTerminalPlan(ctx, current, next, nil, nil)
}

func (repository *BackupRuntimeRepository) prepareBackupRunTerminalPlan(
	ctx context.Context,
	current etcdstore.Versioned[backupruntime.BackupRunRecord],
	next backupruntime.BackupRunRecord,
	absentOrphan *etcdstore.Versioned[backupruntime.BackupOrphanRecord],
	checkpoint *backupruntime.BackupCheckpointInput,
) (backupRunPublicationPlan, error) {
	transitionMode := backupruntime.BackupRunTransitionOrdinary
	needsTerminalOrphan := backupruntime.BackupRunNeedsTerminalOrphan(current.Record, next)
	retainsTerminalOrphan := absentOrphan == nil &&
		backupruntime.BackupRunRetainsTerminalOrphan(current.Record, next)
	if absentOrphan != nil {
		transitionMode = backupruntime.BackupRunTransitionOrphanDelete
	} else if needsTerminalOrphan {
		transitionMode = backupruntime.BackupRunTransitionOrphanCreate
	} else if retainsTerminalOrphan {
		transitionMode = backupruntime.BackupRunTransitionOrphanTerminal
	}
	if backupruntime.BackupRunRequiresTerminalOrphan(current.Record) && !needsTerminalOrphan {
		return backupRunPublicationPlan{}, errs.New(
			errs.KindValidationFailed,
			"terminal backup must preserve upload intent as an orphan",
		)
	}
	if current.Revision <= 0 || !backupruntime.TerminalBackupRunState(next.State) ||
		backupruntime.ValidateBackupRunTransition(current.Record, next, transitionMode) != nil {
		return backupRunPublicationPlan{}, errs.New(
			errs.KindValidationFailed,
			"terminal backup run transition is invalid",
		)
	}
	if next.State == backupruntime.BackupRunCompleted &&
		!backupruntime.BackupRunReadyForSuccessfulTerminal(current.Record, next) {
		return backupRunPublicationPlan{}, errs.New(
			errs.KindValidationFailed,
			"successful backup terminal requires completed cleanup checkpoints",
		)
	}
	records, err := backupRunExclusionRecords(current.Record, current.Record.CreatedAt)
	if err != nil {
		return backupRunPublicationPlan{}, err
	}
	keys := make([]string, len(records)+1)
	keys[0] = backupruntime.BackupRunKey(current.Record.TaskID)
	for index, record := range records {
		keys[index+1], err = backupruntime.BackupSourceTargetExclusionKey(record.TargetKind, record.TargetID)
		if err != nil {
			return backupRunPublicationPlan{}, err
		}
	}
	orphanOffset := len(keys)
	var retainedOrphan backupruntime.BackupOrphanRecord
	if retainsTerminalOrphan || absentOrphan != nil {
		ordinal, _ := backupruntime.ChangedBackupSourceOrdinal(current.Record, next)
		pointID := current.Record.Sources[ordinal].RecoveryPointID
		connectorIndex, keyErr := backupruntime.BackupOrphanConnectorIndexKey(current.Record.ConnectorID, pointID)
		if keyErr != nil {
			return backupRunPublicationPlan{}, keyErr
		}
		environmentIndex, keyErr := backupruntime.BackupOrphanEnvironmentIndexKey(
			current.Record.EnvironmentID,
			pointID,
		)
		if keyErr != nil {
			return backupRunPublicationPlan{}, keyErr
		}
		keys = append(keys, backupruntime.BackupOrphanKey(pointID), connectorIndex, environmentIndex)
	}
	anchor, err := repository.ReadCurrentKeys(ctx, keys)
	if err != nil {
		return backupRunPublicationPlan{}, err
	}
	defer etcdstore.ClearValues(anchor.Values)
	if anchor.Values[0] == nil || anchor.Values[0].ModRevision != current.Revision {
		return backupRunPublicationPlan{}, errs.New(errs.KindStateConflict, "backup run changed")
	}
	stored, err := backupruntime.DecodeBackupRunRecord(anchor.Values[0].Value)
	if err != nil || !backupruntime.BackupRunRecordsEqual(stored, current.Record) {
		return backupRunPublicationPlan{}, errs.New(errs.KindStateConflict, "backup run changed")
	}
	for index, expected := range records {
		value := anchor.Values[index+1]
		if value == nil {
			return backupRunPublicationPlan{}, errs.New(
				errs.KindInternal,
				"backup source target exclusion set is incomplete",
			)
		}
		exclusion, decodeErr := backupruntime.DecodeBackupSourceTargetExclusionRecord(value.Value)
		if decodeErr != nil || !sameBackupExclusionOwner(exclusion, expected) {
			return backupRunPublicationPlan{}, errs.New(
				errs.KindResourceInUse,
				"backup source target exclusion ownership changed",
			)
		}
	}
	if retainsTerminalOrphan || absentOrphan != nil {
		values := anchor.Values[orphanOffset : orphanOffset+3]
		if values[0] == nil {
			return backupRunPublicationPlan{}, errs.New(
				errs.KindStateConflict,
				"terminal backup orphan authority changed",
			)
		}
		retainedOrphan, err = backupruntime.DecodeBackupOrphanRecord(values[0].Value)
		ordinal, _ := backupruntime.ChangedBackupSourceOrdinal(current.Record, next)
		expectedState := backupruntime.BackupOrphanInspect
		if absentOrphan != nil {
			expectedState = backupruntime.BackupOrphanDelete
		}
		if err != nil || retainedOrphan.TaskID != current.Record.TaskID ||
			retainedOrphan.State != expectedState ||
			!backupruntime.BackupOrphanMatchesRunSource(retainedOrphan, current.Record, ordinal) {
			return backupRunPublicationPlan{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		if absentOrphan != nil &&
			(absentOrphan.Revision <= 0 || absentOrphan.Revision != values[0].ModRevision ||
				absentOrphan.Record != retainedOrphan) {
			return backupRunPublicationPlan{}, errs.New(
				errs.KindStateConflict,
				"terminal backup orphan authority changed",
			)
		}
		if err := backupruntime.ValidateBackupOrphanCompanionEvidence(values, retainedOrphan); err != nil {
			return backupRunPublicationPlan{}, err
		}
	}
	var checkpointPlan backupCheckpointPlan
	if checkpoint != nil {
		if checkpoint.TaskID != current.Record.TaskID {
			return backupRunPublicationPlan{}, errs.New(
				errs.KindValidationFailed,
				"backup checkpoint task does not match its run",
			)
		}
		ordinal, changed := backupruntime.ChangedBackupSourceOrdinal(current.Record, next)
		if !changed {
			return backupRunPublicationPlan{}, errs.New(
				errs.KindValidationFailed,
				"terminal backup checkpoint source is invalid",
			)
		}
		checkpointPlan, err = repository.loadBackupCheckpointPlan(
			ctx,
			*checkpoint,
			anchor.ReadRevision,
			backupRunCheckpointBinding(current.Record, ordinal),
		)
		if err != nil {
			return backupRunPublicationPlan{}, err
		}
		defer checkpointPlan.clear()
		if checkpointPlan.duplicate {
			return backupRunPublicationPlan{}, errs.New(
				errs.KindStateConflict,
				"backup checkpoint domain state is incomplete",
			)
		}
	}
	evidence, err := environmentfence.LoadBackupRunOwned(ctx, repository.store, current.Record, anchor.ReadRevision)
	if err != nil {
		return backupRunPublicationPlan{}, err
	}
	value, err := backupruntime.EncodeBackupRunRecord(next)
	if err != nil {
		return backupRunPublicationPlan{}, err
	}
	conditions := []etcdstore.Condition{{Key: keys[0], ModRevision: current.Revision}}
	mutations := []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: keys[0], Value: value}}
	for index, key := range keys[1 : len(records)+1] {
		conditions = append(conditions, etcdstore.Condition{
			Key: key, ModRevision: anchor.Values[index+1].ModRevision,
		})
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: key})
	}
	if retainsTerminalOrphan || absentOrphan != nil {
		for index, value := range anchor.Values[orphanOffset : orphanOffset+3] {
			conditions = append(conditions, etcdstore.Condition{
				Key: keys[orphanOffset+index], ModRevision: value.ModRevision,
			})
		}
		if absentOrphan != nil {
			for _, key := range keys[orphanOffset : orphanOffset+3] {
				mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: key})
			}
		}
	}
	if transitionMode == backupruntime.BackupRunTransitionOrphanCreate {
		ordinal, changed := backupruntime.ChangedBackupSourceOrdinal(current.Record, next)
		if !changed {
			etcdstore.ClearMutationValues(mutations)
			return backupRunPublicationPlan{}, errs.New(
				errs.KindValidationFailed,
				"terminal backup orphan transition is invalid",
			)
		}
		orphan := backupruntime.BackupOrphanRecordFromRun(next, ordinal)
		orphanValue, encodeErr := backupruntime.EncodeBackupOrphanRecord(orphan)
		if encodeErr != nil {
			etcdstore.ClearMutationValues(mutations)
			return backupRunPublicationPlan{}, encodeErr
		}
		connectorIndex, keyErr := backupruntime.BackupOrphanConnectorIndexKey(
			orphan.Point.ConnectorID,
			orphan.Point.ID,
		)
		if keyErr != nil {
			clear(orphanValue)
			etcdstore.ClearMutationValues(mutations)
			return backupRunPublicationPlan{}, keyErr
		}
		environmentIndex, keyErr := backupruntime.BackupOrphanEnvironmentIndexKey(
			orphan.Point.EnvironmentID,
			orphan.Point.ID,
		)
		if keyErr != nil {
			clear(orphanValue)
			etcdstore.ClearMutationValues(mutations)
			return backupRunPublicationPlan{}, keyErr
		}
		orphanKey := backupruntime.BackupOrphanKey(orphan.Point.ID)
		conditions = append(
			conditions,
			etcdstore.Condition{Key: orphanKey},
			etcdstore.Condition{Key: connectorIndex},
			etcdstore.Condition{Key: environmentIndex},
		)
		mutations = append(
			mutations,
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: orphanKey, Value: orphanValue},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: connectorIndex, Value: []byte(orphan.Point.ID)},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: environmentIndex, Value: []byte(orphan.Point.ID)},
		)
	}
	conditions = append(conditions, evidence.TransactionConditions()...)
	conditions = append(conditions, checkpointPlan.conditions...)
	mutations = append(mutations, etcdstore.Mutation{
		Type: etcdstore.MutationDelete, Key: hierarchyrecord.EnvironmentOperationLockKey(current.Record.EnvironmentID),
	})
	epoch, err := evidence.EpochRewriteMutation()
	if err != nil {
		etcdstore.ClearMutationValues(mutations)
		return backupRunPublicationPlan{}, err
	}
	mutations = append(mutations, epoch)
	for _, mutation := range checkpointPlan.mutations {
		mutations = append(mutations, etcdstore.Mutation{
			Type: mutation.Type, Key: mutation.Key, Value: append([]byte(nil), mutation.Value...),
		})
	}
	if err := backupruntime.ValidateBackupRuntimeTransactionBounds(conditions, mutations); err != nil {
		etcdstore.ClearMutationValues(mutations)
		return backupRunPublicationPlan{}, err
	}
	return backupRunPublicationPlan{conditions: conditions, mutations: mutations, record: next}, nil
}
