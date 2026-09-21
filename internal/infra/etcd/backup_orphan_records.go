package etcd

import (
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *BackupRuntimeRepository) CreateBackupOrphan(
	ctx context.Context,
	authority backupruntime.BackupAssignmentInput,
	current etcdstore.Versioned[backupruntime.BackupRunRecord],
	next backupruntime.BackupRunRecord,
	ordinal uint32,
	orphan backupruntime.BackupOrphanRecord,
) (etcdstore.Versioned[backupruntime.BackupRunRecord], error) {
	changedOrdinal, changed := backupruntime.ChangedBackupSourceOrdinal(current.Record, next)
	if int(ordinal) >= len(current.Record.Sources) || !changed || changedOrdinal != ordinal ||
		backupruntime.ValidateBackupRunTransition(current.Record, next, backupruntime.BackupRunTransitionOrphanCreate) != nil ||
		!backupruntime.BackupPointMatchesRunSource(orphan.Point, next, ordinal) || orphan.TaskID != next.TaskID ||
		orphan.State != backupruntime.BackupOrphanInspect {
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
			errs.KindValidationFailed,
			"backup orphan creation is invalid",
		)
	}
	orphan.Reconciliation = backupruntime.BackupOrphanReconciliationAuthority{
		OperationID:    next.OperationID,
		PolicyRevision: next.PolicyRevision,
		RetentionKeep:  next.RetentionKeep,
	}
	value, err := backupruntime.EncodeBackupOrphanRecord(orphan)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
	}
	defer clear(value)
	connectorIndex, err := backupruntime.BackupOrphanConnectorIndexKey(orphan.Point.ConnectorID, orphan.Point.ID)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
	}
	environmentIndex, err := backupruntime.BackupOrphanEnvironmentIndexKey(
		orphan.Point.EnvironmentID,
		orphan.Point.ID,
	)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
	}
	conditions := []etcdstore.Condition{
		{Key: backupruntime.BackupOrphanKey(orphan.Point.ID)},
		{Key: connectorIndex},
		{Key: environmentIndex},
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: backupruntime.BackupOrphanKey(orphan.Point.ID), Value: value},
		{
			Type:  etcdstore.MutationPut,
			Key:   connectorIndex,
			Value: []byte(orphan.Point.ID),
		},
		{Type: etcdstore.MutationPut, Key: environmentIndex, Value: []byte(orphan.Point.ID)},
	}
	return repository.replaceBackupRun(
		ctx,
		current,
		next,
		conditions,
		mutations,
		nil,
		&authority,
		nil,
	)
}
