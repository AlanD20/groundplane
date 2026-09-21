package backupruntime

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Reader) GetBackupOrphan(
	ctx context.Context,
	recoveryPointID string,
) (etcdstore.Versioned[BackupOrphanRecord], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[BackupOrphanRecord]{}, false, err
	}
	if err := recordcodec.ValidateID(ids.KindRecoveryPoint, recoveryPointID); err != nil {
		return etcdstore.Versioned[BackupOrphanRecord]{}, false, err
	}
	primaryKey := BackupOrphanKey(recoveryPointID)
	initial, err := repository.store.Get(ctx, primaryKey)
	if err != nil {
		return etcdstore.Versioned[BackupOrphanRecord]{}, false, err
	}
	if initial == nil {
		return etcdstore.Versioned[BackupOrphanRecord]{}, false, errs.New(
			errs.KindInternal,
			"backup orphan read is empty",
		)
	}
	if initial.Entry == nil {
		return etcdstore.Versioned[BackupOrphanRecord]{ReadRevision: initial.ReadRevision}, false, nil
	}
	defer clear(initial.Entry.Value)
	record, err := DecodeBackupOrphanRecord(initial.Entry.Value)
	if err != nil || record.Point.ID != recoveryPointID {
		return etcdstore.Versioned[BackupOrphanRecord]{}, false, CorruptBackupRuntimeRecord()
	}
	membershipKey, err := BackupOrphanEnvironmentIndexKey(
		record.Point.EnvironmentID,
		recoveryPointID,
	)
	if err != nil {
		return etcdstore.Versioned[BackupOrphanRecord]{}, false, CorruptBackupRuntimeRecord()
	}
	connectorMembershipKey, err := BackupOrphanConnectorIndexKey(
		record.Point.ConnectorID,
		recoveryPointID,
	)
	if err != nil {
		return etcdstore.Versioned[BackupOrphanRecord]{}, false, CorruptBackupRuntimeRecord()
	}
	authority, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys:     []string{primaryKey, membershipKey, connectorMembershipKey},
		Revision: initial.ReadRevision,
	})
	if err != nil {
		return etcdstore.Versioned[BackupOrphanRecord]{}, false, err
	}
	if authority == nil || authority.ReadRevision != initial.ReadRevision ||
		len(authority.Values) != 3 || authority.Values[0] == nil ||
		authority.Values[1] == nil || authority.Values[2] == nil {
		return etcdstore.Versioned[BackupOrphanRecord]{}, false, CorruptBackupRuntimeRecord()
	}
	defer etcdstore.ClearValues(authority.Values)
	stored, err := DecodeBackupOrphanRecord(authority.Values[0].Value)
	expectedVersion := int64(1)
	if stored.State == BackupOrphanDelete {
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
		return etcdstore.Versioned[BackupOrphanRecord]{}, false, CorruptBackupRuntimeRecord()
	}
	return etcdstore.Versioned[BackupOrphanRecord]{
		Record:       stored,
		Revision:     authority.Values[0].ModRevision,
		ReadRevision: authority.ReadRevision,
	}, true, nil
}
