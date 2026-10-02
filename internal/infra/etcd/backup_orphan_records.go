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
	orphan.Reconciliation = backupruntime.BackupOrphanReconciliationAuthority{
		OperationID:    next.OperationID,
		PolicyRevision: next.PolicyRevision,
		PolicySHA256:   next.PolicySHA256,
		RetentionKeep:  next.RetentionKeep,
	}
	changedOrdinal, changed := backupruntime.ChangedBackupSourceOrdinal(current.Record, next)
	if int(ordinal) >= len(current.Record.Sources) || !changed || changedOrdinal != ordinal ||
		backupruntime.ValidateBackupRunTransition(
			current.Record,
			next,
			backupruntime.BackupRunTransitionOrphanCreate,
		) != nil ||
		!backupruntime.BackupOrphanMatchesRunSource(orphan, next, ordinal) || orphan.TaskID != next.TaskID ||
		orphan.State != backupruntime.BackupOrphanInspect {
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
			errs.KindValidationFailed,
			"backup orphan creation is invalid",
		)
	}
	value, err := backupruntime.EncodeBackupOrphanRecord(orphan)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
	}
	defer clear(value)
	connectorIndex, err := backupruntime.BackupOrphanConnectorIndexKey(orphan.Target.ConnectorID, orphan.Target.ID)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
	}
	environmentIndex, err := backupruntime.BackupOrphanEnvironmentIndexKey(
		orphan.Target.EnvironmentID,
		orphan.Target.ID,
	)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
	}
	conditions := []etcdstore.Condition{
		{Key: backupruntime.BackupOrphanKey(orphan.Target.ID)},
		{Key: connectorIndex},
		{Key: environmentIndex},
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: backupruntime.BackupOrphanKey(orphan.Target.ID), Value: value},
		{
			Type:  etcdstore.MutationPut,
			Key:   connectorIndex,
			Value: []byte(orphan.Target.ID),
		},
		{Type: etcdstore.MutationPut, Key: environmentIndex, Value: []byte(orphan.Target.ID)},
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
