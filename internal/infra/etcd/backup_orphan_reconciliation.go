package etcd

import (
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// TransitionReconciledBackupOrphan advances Controller-owned orphan repair
// without depending on the originating Task, assignment, or Backup run.
func (repository *BackupRuntimeRepository) TransitionReconciledBackupOrphan(
	ctx context.Context,
	current etcdstore.Versioned[backupruntime.BackupOrphanRecord],
	next backupruntime.BackupOrphanRecord,
) (etcdstore.Versioned[backupruntime.BackupOrphanRecord], error) {
	if current.Revision <= 0 || current.Record.State != backupruntime.BackupOrphanInspect ||
		next.State != backupruntime.BackupOrphanDelete || current.Record.Point != next.Point ||
		current.Record.TaskID != next.TaskID ||
		current.Record.Reconciliation != next.Reconciliation ||
		!next.UpdatedAt.After(current.Record.UpdatedAt) || next.CreatedAt != current.Record.CreatedAt {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, errs.New(
			errs.KindValidationFailed,
			"backup orphan reconciliation transition is invalid",
		)
	}
	value, err := backupruntime.EncodeBackupOrphanRecord(next)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, err
	}
	defer clear(value)
	connectorIndex, err := backupruntime.BackupOrphanConnectorIndexKey(next.Point.ConnectorID, next.Point.ID)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, err
	}
	environmentIndex, err := backupruntime.BackupOrphanEnvironmentIndexKey(next.Point.EnvironmentID, next.Point.ID)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, err
	}
	keys := []string{backupruntime.BackupOrphanKey(next.Point.ID), connectorIndex, environmentIndex}
	anchor, err := repository.readCurrentKeys(ctx, keys)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, err
	}
	defer etcdstore.ClearValues(anchor.Values)
	if anchor.Values[0] != nil {
		stored, decodeErr := backupruntime.DecodeBackupOrphanRecord(anchor.Values[0].Value)
		if decodeErr == nil && stored == next &&
			backupruntime.ValidateBackupOrphanCompanionEvidence(anchor.Values, next) == nil {
			return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{
				Record: stored, Revision: anchor.Values[0].ModRevision,
				ReadRevision: anchor.ReadRevision,
			}, nil
		}
	}
	if anchor.Values[0] == nil || anchor.Values[0].ModRevision != current.Revision {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, errs.New(
			errs.KindStateConflict,
			"backup orphan reconciliation authority changed",
		)
	}
	if err := backupruntime.ValidateBackupOrphanCompanionEvidence(anchor.Values, current.Record); err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, err
	}
	conditions := []etcdstore.Condition{
		{Key: keys[0], ModRevision: current.Revision},
		{Key: keys[1], ModRevision: current.Revision},
		{Key: keys[2], ModRevision: current.Revision},
	}
	result, err := repository.transact(ctx, conditions, []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: keys[0], Value: value},
		{Type: etcdstore.MutationPut, Key: keys[1], Value: []byte(next.Point.ID)},
		{Type: etcdstore.MutationPut, Key: keys[2], Value: []byte(next.Point.ID)},
	})
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, err
	}
	if !result.Succeeded {
		etcdstore.ClearValues(result.FailureReads)
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, errs.New(
			errs.KindStateConflict,
			"backup orphan reconciliation authority changed",
		)
	}
	return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{
		Record: next, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

// DeleteReconciledBackupOrphan consumes verified-absent Controller authority.
func (repository *BackupRuntimeRepository) DeleteReconciledBackupOrphan(
	ctx context.Context,
	current etcdstore.Versioned[backupruntime.BackupOrphanRecord],
) error {
	if current.Revision <= 0 || current.Record.State != backupruntime.BackupOrphanDelete {
		return errs.New(errs.KindValidationFailed, "backup orphan reconciliation deletion is invalid")
	}
	connectorIndex, err := backupruntime.BackupOrphanConnectorIndexKey(
		current.Record.Point.ConnectorID,
		current.Record.Point.ID,
	)
	if err != nil {
		return err
	}
	environmentIndex, err := backupruntime.BackupOrphanEnvironmentIndexKey(
		current.Record.Point.EnvironmentID,
		current.Record.Point.ID,
	)
	if err != nil {
		return err
	}
	keys := []string{backupruntime.BackupOrphanKey(current.Record.Point.ID), connectorIndex, environmentIndex}
	anchor, err := repository.readCurrentKeys(ctx, keys)
	if err != nil {
		return err
	}
	defer etcdstore.ClearValues(anchor.Values)
	if anchor.Values[0] == nil && anchor.Values[1] == nil && anchor.Values[2] == nil {
		return nil
	}
	if anchor.Values[0] == nil || anchor.Values[0].ModRevision != current.Revision {
		return errs.New(errs.KindStateConflict, "backup orphan reconciliation authority changed")
	}
	if err := backupruntime.ValidateBackupOrphanCompanionEvidence(anchor.Values, current.Record); err != nil {
		return err
	}
	result, err := repository.transact(ctx, []etcdstore.Condition{
		{Key: keys[0], ModRevision: current.Revision},
		{Key: keys[1], ModRevision: current.Revision},
		{Key: keys[2], ModRevision: current.Revision},
	}, []etcdstore.Mutation{
		{Type: etcdstore.MutationDelete, Key: keys[0]},
		{Type: etcdstore.MutationDelete, Key: keys[1]},
		{Type: etcdstore.MutationDelete, Key: keys[2]},
	})
	if err != nil {
		return err
	}
	if !result.Succeeded {
		etcdstore.ClearValues(result.FailureReads)
		return errs.New(errs.KindStateConflict, "backup orphan reconciliation authority changed")
	}
	return nil
}

// AdoptReconciledBackupOrphan promotes a verified artifact and starts its
// captured retention sweep without consulting the originating Task or run.
func (repository *BackupRuntimeRepository) AdoptReconciledBackupOrphan(
	ctx context.Context,
	current etcdstore.Versioned[backupruntime.BackupOrphanRecord],
	point backupruntime.BackupRecoveryPointRecord,
	sweep backupruntime.BackupRetentionSweepRecord,
) (etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord], error) {
	if current.Revision <= 0 || current.Record.State != backupruntime.BackupOrphanInspect ||
		point.BackupRecoveryPointSnapshot != current.Record.Point ||
		sweep.SourceID != point.SourceID || sweep.TriggerRecoveryPointID != point.ID ||
		sweep.Revision != current.Record.Reconciliation.PolicyRevision ||
		sweep.Keep != current.Record.Reconciliation.RetentionKeep ||
		sweep.State != backupruntime.BackupRetentionPending || sweep.CreatedAt != point.VerifiedAt ||
		sweep.UpdatedAt != point.VerifiedAt {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, errs.New(
			errs.KindValidationFailed,
			"backup orphan reconciliation adoption is invalid",
		)
	}
	pointValue, err := backupruntime.EncodeBackupRecoveryPointRecord(point)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	defer clear(pointValue)
	sweepValue, err := backupruntime.EncodeBackupRetentionSweepRecord(sweep)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	defer clear(sweepValue)
	orphanConnectorIndex, err := backupruntime.BackupOrphanConnectorIndexKey(point.ConnectorID, point.ID)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	orphanEnvironmentIndex, err := backupruntime.BackupOrphanEnvironmentIndexKey(point.EnvironmentID, point.ID)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	environmentIndex, err := backupruntime.BackupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	sourceIndex, err := backupruntime.BackupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	connectorIndex, err := backupruntime.BackupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	keys := []string{
		backupruntime.BackupOrphanKey(point.ID), orphanConnectorIndex, orphanEnvironmentIndex,
		backupruntime.BackupRecoveryPointKey(point.ID), environmentIndex, sourceIndex, connectorIndex,
		backupruntime.BackupRetentionKey(point.SourceID, point.ID),
	}
	anchor, err := repository.readCurrentKeys(ctx, keys)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	defer etcdstore.ClearValues(anchor.Values)
	if anchor.Values[0] == nil {
		if anchor.Values[1] != nil || anchor.Values[2] != nil {
			return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		allTargetsAbsent := true
		for _, value := range anchor.Values[3:] {
			allTargetsAbsent = allTargetsAbsent && value == nil
		}
		if allTargetsAbsent {
			return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, errs.New(
				errs.KindStateConflict,
				"backup orphan reconciliation authority disappeared",
			)
		}
		if err := validateReconciledBackupPointReplay(anchor.Values[3:], point, sweep); err != nil {
			return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, err
		}
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{
			Record: point, Revision: anchor.Values[3].ModRevision, ReadRevision: anchor.ReadRevision,
		}, nil
	}
	if anchor.Values[0].ModRevision != current.Revision {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, errs.New(
			errs.KindStateConflict,
			"backup orphan reconciliation authority changed",
		)
	}
	if err := backupruntime.ValidateBackupOrphanCompanionEvidence(anchor.Values[:3], current.Record); err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	for _, value := range anchor.Values[3:] {
		if value != nil {
			return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, errs.New(
				errs.KindStateConflict,
				"backup orphan adoption target already exists",
			)
		}
	}
	conditions := make([]etcdstore.Condition, 0, len(keys))
	for position, key := range keys {
		condition := etcdstore.Condition{Key: key}
		if position < 3 {
			condition.ModRevision = current.Revision
		}
		conditions = append(conditions, condition)
	}
	result, err := repository.transact(ctx, conditions, []etcdstore.Mutation{
		{Type: etcdstore.MutationDelete, Key: keys[0]},
		{Type: etcdstore.MutationDelete, Key: keys[1]},
		{Type: etcdstore.MutationDelete, Key: keys[2]},
		{Type: etcdstore.MutationPut, Key: keys[3], Value: pointValue},
		{Type: etcdstore.MutationPut, Key: keys[4], Value: []byte(point.ID)},
		{Type: etcdstore.MutationPut, Key: keys[5], Value: []byte(point.ID)},
		{Type: etcdstore.MutationPut, Key: keys[6], Value: []byte(point.ID)},
		{Type: etcdstore.MutationPut, Key: keys[7], Value: sweepValue},
	})
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	if !result.Succeeded {
		etcdstore.ClearValues(result.FailureReads)
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, errs.New(
			errs.KindStateConflict,
			"backup orphan reconciliation authority changed",
		)
	}
	return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{
		Record: point, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func validateReconciledBackupPointReplay(
	values []*etcdstore.KeyValue,
	point backupruntime.BackupRecoveryPointRecord,
	sweep backupruntime.BackupRetentionSweepRecord,
) error {
	if len(values) != 5 || values[0] == nil || values[1] == nil || values[2] == nil ||
		values[3] == nil || values[4] == nil || values[0].Version != 1 ||
		values[1].Version != 1 || values[2].Version != 1 || values[3].Version != 1 ||
		values[4].Version != 1 || values[1].ModRevision != values[0].ModRevision ||
		values[2].ModRevision != values[0].ModRevision || values[3].ModRevision != values[0].ModRevision ||
		values[4].ModRevision != values[0].ModRevision || string(values[1].Value) != point.ID ||
		string(values[2].Value) != point.ID || string(values[3].Value) != point.ID {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	storedPoint, pointErr := backupruntime.DecodeBackupRecoveryPointRecord(values[0].Value)
	storedSweep, sweepErr := backupruntime.DecodeBackupRetentionSweepRecord(values[4].Value)
	if pointErr != nil || sweepErr != nil ||
		storedPoint.BackupRecoveryPointSnapshot != point.BackupRecoveryPointSnapshot ||
		storedSweep.SourceID != storedPoint.SourceID ||
		storedSweep.TriggerRecoveryPointID != storedPoint.ID ||
		storedSweep.Keep != sweep.Keep || storedSweep.Revision != sweep.Revision ||
		storedSweep.State != backupruntime.BackupRetentionPending ||
		storedSweep.CreatedAt != storedPoint.VerifiedAt ||
		storedSweep.UpdatedAt != storedPoint.VerifiedAt {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	if storedPoint != point || storedSweep != sweep {
		return errs.New(
			errs.KindStateConflict,
			"backup orphan adoption was completed by another reconciler",
		)
	}
	return nil
}
