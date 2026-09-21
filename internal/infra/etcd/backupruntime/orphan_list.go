package backupruntime

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Reader) ListBackupOrphansByEnvironment(
	ctx context.Context,
	environmentID string,
	request BackupRuntimeListRequest,
) (BackupRuntimePage[BackupOrphanRecord], error) {
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return BackupRuntimePage[BackupOrphanRecord]{}, err
	}
	prefix := BackupOrphanEnvironmentPrefix + environmentID + "/"
	if err := ValidateBackupRuntimeListRequest(prefix, request); err != nil {
		return BackupRuntimePage[BackupOrphanRecord]{}, err
	}
	index, err := repository.store.Range(ctx, etcdstore.RangeRequest{
		Prefix: prefix, StartExclusive: request.StartExclusive,
		Limit: int64(request.Limit), Revision: request.Revision,
	})
	if err != nil {
		return BackupRuntimePage[BackupOrphanRecord]{}, err
	}
	if index == nil || index.ReadRevision <= 0 {
		return BackupRuntimePage[BackupOrphanRecord]{}, errs.New(
			errs.KindInternal,
			"backup orphan environment index page is incomplete",
		)
	}
	defer etcdstore.ClearRangeValues(index.Values)
	keys := make([]string, len(index.Values))
	pointIDs := make([]string, len(index.Values))
	for position, item := range index.Values {
		pointID := string(item.Value)
		if recordcodec.ValidateID(ids.KindRecoveryPoint, pointID) != nil {
			return BackupRuntimePage[BackupOrphanRecord]{}, CorruptBackupRuntimeRecord()
		}
		expected, keyErr := BackupOrphanEnvironmentIndexKey(environmentID, pointID)
		if keyErr != nil || expected != item.Key {
			return BackupRuntimePage[BackupOrphanRecord]{}, CorruptBackupRuntimeRecord()
		}
		keys[position] = BackupOrphanKey(pointID)
		pointIDs[position] = pointID
	}
	page := BackupRuntimePage[BackupOrphanRecord]{Revision: index.ReadRevision}
	if len(keys) == 0 {
		return page, nil
	}
	primaries, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: keys, Revision: index.ReadRevision},
	)
	if err != nil {
		return BackupRuntimePage[BackupOrphanRecord]{}, err
	}
	if primaries == nil || primaries.ReadRevision != index.ReadRevision ||
		len(primaries.Values) != len(keys) {
		return BackupRuntimePage[BackupOrphanRecord]{}, CorruptBackupRuntimeRecord()
	}
	defer etcdstore.ClearValues(primaries.Values)
	records := make([]BackupOrphanRecord, len(keys))
	connectorKeys := make([]string, len(keys))
	for position, value := range primaries.Values {
		if value == nil {
			return BackupRuntimePage[BackupOrphanRecord]{}, CorruptBackupRuntimeRecord()
		}
		record, decodeErr := DecodeBackupOrphanRecord(value.Value)
		expectedVersion := int64(1)
		if record.State == BackupOrphanDelete {
			expectedVersion = 2
		}
		if decodeErr != nil || record.Point.ID != pointIDs[position] ||
			record.Point.EnvironmentID != environmentID || value.Version != expectedVersion ||
			index.Values[position].Version != expectedVersion ||
			index.Values[position].ModRevision != value.ModRevision {
			return BackupRuntimePage[BackupOrphanRecord]{}, CorruptBackupRuntimeRecord()
		}
		connectorKey, keyErr := BackupOrphanConnectorIndexKey(
			record.Point.ConnectorID,
			record.Point.ID,
		)
		if keyErr != nil {
			return BackupRuntimePage[BackupOrphanRecord]{}, CorruptBackupRuntimeRecord()
		}
		records[position] = record
		connectorKeys[position] = connectorKey
	}
	connectors, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: connectorKeys, Revision: index.ReadRevision,
	})
	if err != nil {
		return BackupRuntimePage[BackupOrphanRecord]{}, err
	}
	if connectors == nil || connectors.ReadRevision != index.ReadRevision ||
		len(connectors.Values) != len(connectorKeys) {
		return BackupRuntimePage[BackupOrphanRecord]{}, CorruptBackupRuntimeRecord()
	}
	defer etcdstore.ClearValues(connectors.Values)
	page.Items = make([]etcdstore.Versioned[BackupOrphanRecord], len(keys))
	for position, connector := range connectors.Values {
		record := records[position]
		value := primaries.Values[position]
		expectedVersion := int64(1)
		if record.State == BackupOrphanDelete {
			expectedVersion = 2
		}
		if connector == nil || connector.Key != connectorKeys[position] ||
			connector.Version != expectedVersion || connector.ModRevision != value.ModRevision ||
			string(connector.Value) != record.Point.ID {
			return BackupRuntimePage[BackupOrphanRecord]{}, CorruptBackupRuntimeRecord()
		}
		page.Items[position] = etcdstore.Versioned[BackupOrphanRecord]{
			Record: record, Revision: value.ModRevision, ReadRevision: index.ReadRevision,
		}
	}
	if index.More {
		page.Next = index.Values[len(index.Values)-1].Key
	}
	return page, nil
}
