package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *BackupRuntimeRepository) GetBackupOrphan(
	ctx context.Context,
	recoveryPointID string,
) (etcdstore.Versioned[backupruntime.BackupOrphanRecord], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, false, err
	}
	if err := recordcodec.ValidateID(ids.KindRecoveryPoint, recoveryPointID); err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, false, err
	}
	primaryKey := backupruntime.BackupOrphanKey(recoveryPointID)
	initial, err := repository.store.Get(ctx, primaryKey)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, false, err
	}
	if initial == nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, false, errs.New(
			errs.KindInternal,
			"backup orphan read is empty",
		)
	}
	if initial.Entry == nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{ReadRevision: initial.ReadRevision}, false, nil
	}
	defer clear(initial.Entry.Value)
	record, err := backupruntime.DecodeBackupOrphanRecord(initial.Entry.Value)
	if err != nil || record.Point.ID != recoveryPointID {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, false, backupruntime.CorruptBackupRuntimeRecord()
	}
	membershipKey, err := backupruntime.BackupOrphanEnvironmentIndexKey(
		record.Point.EnvironmentID,
		recoveryPointID,
	)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, false, backupruntime.CorruptBackupRuntimeRecord()
	}
	connectorMembershipKey, err := backupruntime.BackupOrphanConnectorIndexKey(
		record.Point.ConnectorID,
		recoveryPointID,
	)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, false, backupruntime.CorruptBackupRuntimeRecord()
	}
	authority, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys:     []string{primaryKey, membershipKey, connectorMembershipKey},
		Revision: initial.ReadRevision,
	})
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, false, err
	}
	if authority == nil || authority.ReadRevision != initial.ReadRevision ||
		len(authority.Values) != 3 || authority.Values[0] == nil ||
		authority.Values[1] == nil || authority.Values[2] == nil {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, false, backupruntime.CorruptBackupRuntimeRecord()
	}
	defer etcdstore.ClearValues(authority.Values)
	stored, err := backupruntime.DecodeBackupOrphanRecord(authority.Values[0].Value)
	expectedVersion := int64(1)
	if stored.State == backupruntime.BackupOrphanDelete {
		expectedVersion = 2
	}
	if err != nil || stored != record ||
		authority.Values[0].ModRevision != initial.Entry.ModRevision ||
		authority.Values[1].Key != membershipKey ||
		authority.Values[2].Key != connectorMembershipKey ||
		authority.Values[0].Version != expectedVersion ||
		authority.Values[1].Version != expectedVersion ||
		authority.Values[2].Version != expectedVersion ||
		authority.Values[1].ModRevision != authority.Values[0].ModRevision ||
		authority.Values[2].ModRevision != authority.Values[0].ModRevision ||
		string(authority.Values[1].Value) != recoveryPointID ||
		string(authority.Values[2].Value) != recoveryPointID {
		return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{}, false, backupruntime.CorruptBackupRuntimeRecord()
	}
	return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{
		Record:       stored,
		Revision:     authority.Values[0].ModRevision,
		ReadRevision: authority.ReadRevision,
	}, true, nil
}

func (repository *BackupRuntimeRepository) CreateBackupOrphan(
	ctx context.Context,
	authority BackupAssignmentInput,
	current etcdstore.Versioned[backupruntime.BackupRunRecord],
	next backupruntime.BackupRunRecord,
	ordinal uint32,
	orphan backupruntime.BackupOrphanRecord,
) (etcdstore.Versioned[backupruntime.BackupRunRecord], error) {
	changedOrdinal, changed := changedBackupSourceOrdinal(current.Record, next)
	if int(ordinal) >= len(current.Record.Sources) || !changed || changedOrdinal != ordinal ||
		validateBackupRunTransition(current.Record, next, backupRunTransitionOrphanCreate) != nil ||
		!backupPointMatchesRunSource(orphan.Point, next, ordinal) || orphan.TaskID != next.TaskID ||
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
