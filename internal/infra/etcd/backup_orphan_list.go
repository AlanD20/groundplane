package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *BackupRuntimeRepository) ListBackupOrphansByEnvironment(
	ctx context.Context,
	environmentID string,
	request BackupRuntimeListRequest,
) (BackupRuntimePage[backupruntime.BackupOrphanRecord], error) {
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return BackupRuntimePage[backupruntime.BackupOrphanRecord]{}, err
	}
	prefix := backupruntime.BackupOrphanEnvironmentPrefix + environmentID + "/"
	if err := validateBackupRuntimeListRequest(prefix, request); err != nil {
		return BackupRuntimePage[backupruntime.BackupOrphanRecord]{}, err
	}
	index, err := repository.store.Range(ctx, etcdstore.RangeRequest{
		Prefix: prefix, StartExclusive: request.StartExclusive,
		Limit: int64(request.Limit), Revision: request.Revision,
	})
	if err != nil {
		return BackupRuntimePage[backupruntime.BackupOrphanRecord]{}, err
	}
	if index == nil || index.ReadRevision <= 0 {
		return BackupRuntimePage[backupruntime.BackupOrphanRecord]{}, errs.New(
			errs.KindInternal,
			"backup orphan environment index page is incomplete",
		)
	}
	defer clearRangeValues(index.Values)
	keys := make([]string, len(index.Values))
	pointIDs := make([]string, len(index.Values))
	for position, item := range index.Values {
		pointID := string(item.Value)
		if recordcodec.ValidateID(ids.KindRecoveryPoint, pointID) != nil {
			return BackupRuntimePage[backupruntime.BackupOrphanRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		expected, keyErr := backupruntime.BackupOrphanEnvironmentIndexKey(environmentID, pointID)
		if keyErr != nil || expected != item.Key {
			return BackupRuntimePage[backupruntime.BackupOrphanRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		keys[position] = backupruntime.BackupOrphanKey(pointID)
		pointIDs[position] = pointID
	}
	page := BackupRuntimePage[backupruntime.BackupOrphanRecord]{Revision: index.ReadRevision}
	if len(keys) == 0 {
		return page, nil
	}
	primaries, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: keys, Revision: index.ReadRevision},
	)
	if err != nil {
		return BackupRuntimePage[backupruntime.BackupOrphanRecord]{}, err
	}
	if primaries == nil || primaries.ReadRevision != index.ReadRevision ||
		len(primaries.Values) != len(keys) {
		return BackupRuntimePage[backupruntime.BackupOrphanRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	defer clearKeyValues(primaries.Values)
	records := make([]backupruntime.BackupOrphanRecord, len(keys))
	connectorKeys := make([]string, len(keys))
	for position, value := range primaries.Values {
		if value == nil {
			return BackupRuntimePage[backupruntime.BackupOrphanRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		record, decodeErr := backupruntime.DecodeBackupOrphanRecord(value.Value)
		expectedVersion := int64(1)
		if record.State == backupruntime.BackupOrphanDelete {
			expectedVersion = 2
		}
		if decodeErr != nil || record.Point.ID != pointIDs[position] ||
			record.Point.EnvironmentID != environmentID || value.Version != expectedVersion ||
			index.Values[position].Version != expectedVersion ||
			index.Values[position].ModRevision != value.ModRevision {
			return BackupRuntimePage[backupruntime.BackupOrphanRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		connectorKey, keyErr := backupruntime.BackupOrphanConnectorIndexKey(
			record.Point.ConnectorID,
			record.Point.ID,
		)
		if keyErr != nil {
			return BackupRuntimePage[backupruntime.BackupOrphanRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		records[position] = record
		connectorKeys[position] = connectorKey
	}
	connectors, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: connectorKeys, Revision: index.ReadRevision,
	})
	if err != nil {
		return BackupRuntimePage[backupruntime.BackupOrphanRecord]{}, err
	}
	if connectors == nil || connectors.ReadRevision != index.ReadRevision ||
		len(connectors.Values) != len(connectorKeys) {
		return BackupRuntimePage[backupruntime.BackupOrphanRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	defer clearKeyValues(connectors.Values)
	page.Items = make([]etcdstore.Versioned[backupruntime.BackupOrphanRecord], len(keys))
	for position, connector := range connectors.Values {
		record := records[position]
		value := primaries.Values[position]
		expectedVersion := int64(1)
		if record.State == backupruntime.BackupOrphanDelete {
			expectedVersion = 2
		}
		if connector == nil || connector.Key != connectorKeys[position] ||
			connector.Version != expectedVersion || connector.ModRevision != value.ModRevision ||
			string(connector.Value) != record.Point.ID {
			return BackupRuntimePage[backupruntime.BackupOrphanRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		page.Items[position] = etcdstore.Versioned[backupruntime.BackupOrphanRecord]{
			Record: record, Revision: value.ModRevision, ReadRevision: index.ReadRevision,
		}
	}
	if index.More {
		page.Next = index.Values[len(index.Values)-1].Key
	}
	return page, nil
}
