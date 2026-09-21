package etcd

import (
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *BackupRuntimeRepository) CommitBackupRecoveryPoint(
	ctx context.Context,
	authority backupruntime.BackupAssignmentInput,
	currentRun etcdstore.Versioned[backupruntime.BackupRunRecord],
	nextRun backupruntime.BackupRunRecord,
	ordinal uint32,
	point backupruntime.BackupRecoveryPointRecord,
	orphan *etcdstore.Versioned[backupruntime.BackupOrphanRecord],
	sweep backupruntime.BackupRetentionSweepRecord,
) (etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord], etcdstore.Versioned[backupruntime.BackupRunRecord], error) {
	changedOrdinal, changed := backupruntime.ChangedBackupSourceOrdinal(currentRun.Record, nextRun)
	if int(ordinal) >= len(currentRun.Record.Sources) || !changed || changedOrdinal != ordinal ||
		backupruntime.ValidateBackupRunTransition(
			currentRun.Record,
			nextRun,
			backupruntime.BackupRunTransitionPointCommit,
		) != nil ||
		!backupPointMatchesRunSource(point.BackupRecoveryPointSnapshot, nextRun, ordinal) ||
		sweep.SourceID != point.SourceID || sweep.TriggerRecoveryPointID != point.ID ||
		sweep.Revision != nextRun.PolicyRevision || sweep.Keep != nextRun.RetentionKeep ||
		sweep.State != backupruntime.BackupRetentionPending {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
			errs.KindValidationFailed,
			"recovery point commit is invalid",
		)
	}
	if currentRun.Record.Sources[ordinal].State == backupruntime.BackupSourceAttemptOrphaned {
		if orphan == nil || orphan.Revision <= 0 ||
			!backupOrphanMatchesRunSource(orphan.Record, currentRun.Record, ordinal) ||
			orphan.Record.Point != point.BackupRecoveryPointSnapshot ||
			orphan.Record.State != backupruntime.BackupOrphanInspect ||
			orphan.Record.TaskID != currentRun.Record.TaskID {
			return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
				errs.KindValidationFailed,
				"orphan-backed Recovery Point commit is invalid",
			)
		}
	} else if orphan != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
			errs.KindValidationFailed,
			"direct Recovery Point commit cannot carry an orphan",
		)
	}
	pointValue, err := backupruntime.EncodeBackupRecoveryPointRecord(point)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
	}
	defer clear(pointValue)
	sweepValue, err := backupruntime.EncodeBackupRetentionSweepRecord(sweep)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
	}
	defer clear(sweepValue)
	environmentIndex, err := backupruntime.BackupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
	}
	sourceIndex, err := backupruntime.BackupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
	}
	connectorIndex, err := backupruntime.BackupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
	}
	orphanConnectorIndex, err := backupruntime.BackupOrphanConnectorIndexKey(point.ConnectorID, point.ID)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
	}
	orphanEnvironmentIndex, err := backupruntime.BackupOrphanEnvironmentIndexKey(point.EnvironmentID, point.ID)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
	}
	conditions := []etcdstore.Condition{
		{
			Key:         connectorrecord.RecordKey(point.ConnectorID),
			ModRevision: currentRun.Record.ConnectorRevision,
		},
		{
			Key:         connectorrecord.CredentialValueKey(point.ConnectorID),
			ModRevision: currentRun.Record.ConnectorCredentialsRevision,
		},
		{Key: backupruntime.BackupRecoveryPointKey(point.ID)},
		{Key: environmentIndex},
		{Key: sourceIndex},
		{Key: connectorIndex},
		{Key: backupruntime.BackupRetentionKey(point.SourceID, point.ID)},
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: backupruntime.BackupRecoveryPointKey(point.ID), Value: pointValue},
		{Type: etcdstore.MutationPut, Key: environmentIndex, Value: []byte(point.ID)},
		{Type: etcdstore.MutationPut, Key: sourceIndex, Value: []byte(point.ID)},
		{Type: etcdstore.MutationPut, Key: connectorIndex, Value: []byte(point.ID)},
		{Type: etcdstore.MutationPut, Key: backupruntime.BackupRetentionKey(point.SourceID, point.ID), Value: sweepValue},
	}
	if orphan != nil {
		conditions = append(conditions,
			etcdstore.Condition{Key: backupruntime.BackupOrphanKey(point.ID), ModRevision: orphan.Revision},
			etcdstore.Condition{Key: orphanConnectorIndex, ModRevision: orphan.Revision},
			etcdstore.Condition{Key: orphanEnvironmentIndex, ModRevision: orphan.Revision},
		)
		mutations = append(
			mutations,
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: backupruntime.BackupOrphanKey(point.ID)},
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: orphanConnectorIndex},
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: orphanEnvironmentIndex},
		)
	}
	validateCompanions := func(values []*etcdstore.KeyValue) error {
		if err := validateBackupConnectorSnapshotEvidence(
			values[:2],
			currentRun.Record,
		); err != nil {
			return err
		}
		if orphan == nil {
			return nil
		}
		return validateBackupOrphanCompanionEvidence(values[len(values)-3:], orphan.Record)
	}
	updatedRun, err := repository.replaceBackupRun(
		ctx,
		currentRun,
		nextRun,
		conditions,
		mutations,
		validateCompanions,
		&authority,
		nil,
	)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
	}
	return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{
		Record: point, Revision: updatedRun.Revision, ReadRevision: updatedRun.ReadRevision,
	}, updatedRun, nil
}
