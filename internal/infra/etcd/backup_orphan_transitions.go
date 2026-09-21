package etcd

import (
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *BackupRuntimeRepository) TransitionBackupOrphan(
	ctx context.Context,
	authority backupruntime.BackupAssignmentInput,
	run etcdstore.Versioned[backupruntime.BackupRunRecord],
	current etcdstore.Versioned[backupruntime.BackupOrphanRecord],
	next backupruntime.BackupOrphanRecord,
) (etcdstore.Versioned[backupruntime.BackupOrphanRecord], error) {
	next.Reconciliation = current.Record.Reconciliation
	if current.Revision <= 0 || current.Record.State != backupruntime.BackupOrphanInspect ||
		next.State != backupruntime.BackupOrphanDelete ||
		current.Record.Point != next.Point ||
		current.Record.TaskID != next.TaskID ||
		!next.UpdatedAt.After(current.Record.UpdatedAt) ||
		next.CreatedAt != current.Record.CreatedAt ||
		next.TaskID != run.Record.TaskID ||
		!runContainsOrphanedPoint(run.Record, current.Record) {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, errs.New(
			errs.KindValidationFailed,
			"backup orphan transition is invalid",
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
	environmentIndex, err := backupruntime.BackupOrphanEnvironmentIndexKey(
		next.Point.EnvironmentID,
		next.Point.ID,
	)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, err
	}
	anchor, err := repository.readCurrentKeys(ctx, []string{
		backupruntime.BackupRunKey(
			run.Record.TaskID,
		),
		backupruntime.BackupOrphanKey(next.Point.ID),
		connectorIndex,
		environmentIndex,
	})
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, err
	}
	defer etcdstore.ClearValues(anchor.Values)
	if anchor.Values[0] == nil || anchor.Values[0].ModRevision != run.Revision {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, errs.New(
			errs.KindStateConflict,
			"backup orphan state changed",
		)
	}
	storedRun, decodeErr := backupruntime.DecodeBackupRunRecord(anchor.Values[0].Value)
	if decodeErr != nil || !backupruntime.BackupRunRecordsEqual(storedRun, run.Record) {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	if authority.TaskID != run.Record.TaskID {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, errs.New(
			errs.KindValidationFailed,
			"backup orphan assignment does not match its run",
		)
	}
	assignmentConditions, err := repository.loadBackupAssignmentFence(
		ctx,
		authority,
		anchor.ReadRevision,
	)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, err
	}
	if anchor.Values[1] != nil {
		storedOrphan, orphanErr := backupruntime.DecodeBackupOrphanRecord(anchor.Values[1].Value)
		if orphanErr == nil && storedOrphan == next &&
			validateBackupOrphanCompanionEvidence(anchor.Values[1:], next) == nil {
			return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{
				Record: storedOrphan, Revision: anchor.Values[1].ModRevision,
				ReadRevision: anchor.ReadRevision,
			}, nil
		}
	}
	if anchor.Values[1] == nil || anchor.Values[1].ModRevision != current.Revision {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, errs.New(
			errs.KindStateConflict,
			"backup orphan companion state changed",
		)
	}
	if err := validateBackupOrphanCompanionEvidence(anchor.Values[1:], current.Record); err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, err
	}
	evidence, err := repository.loadOwnedEvidence(ctx, run.Record, anchor.ReadRevision)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, err
	}
	conditions := []etcdstore.Condition{
		{Key: backupruntime.BackupRunKey(run.Record.TaskID), ModRevision: run.Revision},
		{Key: backupruntime.BackupOrphanKey(next.Point.ID), ModRevision: current.Revision},
		{Key: connectorIndex, ModRevision: current.Revision},
		{Key: environmentIndex, ModRevision: current.Revision},
	}
	conditions = append(conditions, evidence.fence.TransactionConditions()...)
	conditions = append(conditions, assignmentConditions...)
	epoch, err := evidence.fence.EpochRewriteMutation()
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, err
	}
	defer clear(epoch.Value)
	result, err := repository.transact(ctx, conditions, []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: backupruntime.BackupOrphanKey(next.Point.ID), Value: value},
		{Type: etcdstore.MutationPut, Key: connectorIndex, Value: []byte(next.Point.ID)},
		{Type: etcdstore.MutationPut, Key: environmentIndex, Value: []byte(next.Point.ID)},
		epoch,
	})
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, err
	}
	if !result.Succeeded {
		defer etcdstore.ClearValues(result.FailureReads)
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, errs.New(
			errs.KindStateConflict,
			"backup orphan state changed",
		)
	}
	return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{
		Record:       next,
		Revision:     result.Revision,
		ReadRevision: result.Revision,
	}, nil
}

func (repository *BackupRuntimeRepository) prepareBackupOrphanAbsentTerminal(
	ctx context.Context,
	checkpoint backupruntime.BackupCheckpointInput,
	currentRun etcdstore.Versioned[backupruntime.BackupRunRecord],
	nextRun backupruntime.BackupRunRecord,
	ordinal uint32,
	orphan etcdstore.Versioned[backupruntime.BackupOrphanRecord],
) (backupRunPublicationPlan, error) {
	changedOrdinal, changed := backupruntime.ChangedBackupSourceOrdinal(currentRun.Record, nextRun)
	if int(ordinal) >= len(currentRun.Record.Sources) || !changed || changedOrdinal != ordinal ||
		orphan.Revision <= 0 ||
		orphan.Record.State != backupruntime.BackupOrphanDelete || orphan.Record.TaskID != currentRun.Record.TaskID ||
		orphan.Record.Point.ID != currentRun.Record.Sources[ordinal].RecoveryPointID ||
		backupruntime.ValidateBackupRunTransition(
			currentRun.Record,
			nextRun,
			backupruntime.BackupRunTransitionOrphanDelete,
		) != nil {
		return backupRunPublicationPlan{}, errs.New(
			errs.KindValidationFailed,
			"backup orphan deletion is invalid",
		)
	}
	if checkpoint.Payload.Kind != backupruntime.BackupCheckpointRemoteObjectAbsent ||
		checkpoint.Payload.PointID != orphan.Record.Point.ID {
		return backupRunPublicationPlan{}, errs.New(
			errs.KindValidationFailed,
			"backup orphan absence checkpoint is invalid",
		)
	}
	return repository.prepareBackupRunTerminalPlan(ctx, currentRun, nextRun, &orphan, &checkpoint)
}

// advanceBackupRunAfterRetention is the only point-committed to cleanup seam.
// It pins the exact completed sweep under the owning Backup lock.
func (repository *BackupRuntimeRepository) advanceBackupRunAfterRetention(
	ctx context.Context,
	currentRun etcdstore.Versioned[backupruntime.BackupRunRecord],
	nextRun backupruntime.BackupRunRecord,
	ordinal uint32,
	sweep etcdstore.Versioned[backupruntime.BackupRetentionSweepRecord],
) (etcdstore.Versioned[backupruntime.BackupRunRecord], error) {
	changedOrdinal, changed := backupruntime.ChangedBackupSourceOrdinal(currentRun.Record, nextRun)
	if int(ordinal) >= len(currentRun.Record.Sources) || !changed || changedOrdinal != ordinal ||
		sweep.Revision <= 0 || sweep.Record.State != backupruntime.BackupRetentionCompleted ||
		!backupRetentionSweepMatchesRun(currentRun.Record, sweep.Record) ||
		sweep.Record.SourceID != currentRun.Record.Sources[ordinal].SourceID ||
		sweep.Record.TriggerRecoveryPointID != currentRun.Record.Sources[ordinal].RecoveryPointID ||
		backupruntime.ValidateBackupRunTransition(
			currentRun.Record,
			nextRun,
			backupruntime.BackupRunTransitionRetentionComplete,
		) != nil {
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
			errs.KindValidationFailed,
			"backup retention completion is invalid",
		)
	}
	key := backupruntime.BackupRetentionKey(sweep.Record.SourceID, sweep.Record.TriggerRecoveryPointID)
	return repository.replaceBackupRun(
		ctx,
		currentRun,
		nextRun,
		[]etcdstore.Condition{{Key: key, ModRevision: sweep.Revision}},
		nil,
		func(values []*etcdstore.KeyValue) error {
			if len(values) != 1 || values[0] == nil || values[0].ModRevision != sweep.Revision {
				return errs.New(errs.KindStateConflict, "backup retention authority changed")
			}
			stored, err := backupruntime.DecodeBackupRetentionSweepRecord(values[0].Value)
			if err != nil || stored != sweep.Record || stored.State != backupruntime.BackupRetentionCompleted ||
				!backupRetentionSweepMatchesRun(currentRun.Record, stored) {
				return backupruntime.CorruptBackupRuntimeRecord()
			}
			return nil
		},
		nil,
		nil,
	)
}
