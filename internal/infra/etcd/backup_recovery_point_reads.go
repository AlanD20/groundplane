package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *BackupRuntimeRepository) GetBackupRecoveryPoint(
	ctx context.Context,
	recoveryPointID string,
) (etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord], error) {
	if err := recordcodec.ValidateID(ids.KindRecoveryPoint, recoveryPointID); err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	record, found, err := getOptionalBackupRuntimeRecord(
		ctx,
		repository.store,
		backupruntime.BackupRecoveryPointKey(recoveryPointID),
		recoveryPointID,
		backupruntime.DecodeBackupRecoveryPointRecord,
		func(point backupruntime.BackupRecoveryPointRecord) string { return point.ID },
	)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	if !found {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, errs.New(
			errs.KindRecoveryPointNotFound,
			"recovery point was not found",
		)
	}
	environmentIndex, err := backupruntime.BackupRecoveryPointEnvironmentIndexKey(
		record.Record.EnvironmentID,
		recoveryPointID,
	)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	sourceIndex, err := backupruntime.BackupRecoveryPointSourceIndexKey(record.Record.SourceID, recoveryPointID)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	connectorIndex, err := backupruntime.BackupRecoveryPointConnectorIndexKey(
		record.Record.ConnectorID,
		recoveryPointID,
	)
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	authority, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			backupruntime.BackupRecoveryPointKey(recoveryPointID),
			backupruntime.BackupRecoveryPointPruneKey(recoveryPointID),
			environmentIndex,
			sourceIndex,
			connectorIndex,
		},
		Revision: record.ReadRevision,
	})
	if err != nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	if authority == nil || authority.ReadRevision != record.ReadRevision ||
		len(authority.Values) != 5 ||
		authority.Values[0] == nil ||
		authority.Values[0].Version != 1 ||
		authority.Values[0].ModRevision != record.Revision ||
		authority.Values[2] == nil || authority.Values[3] == nil || authority.Values[4] == nil {
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	defer clearKeyValues(authority.Values)
	for index, expectedKey := range []string{environmentIndex, sourceIndex, connectorIndex} {
		value := authority.Values[index+2]
		if value.Key != expectedKey || value.Version != 1 || value.ModRevision != record.Revision ||
			string(value.Value) != recoveryPointID {
			return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
	}
	if authority.Values[1] != nil {
		prune, decodeErr := backupruntime.DecodeBackupRecoveryPointPruneRecord(authority.Values[1].Value)
		if decodeErr != nil || prune.Point != record.Record.BackupRecoveryPointSnapshot ||
			prune.State == backupruntime.BackupPruneVerifiedAbsent {
			return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		return etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{}, errs.New(
			errs.KindRecoveryPointNotFound,
			"recovery point was not found",
		)
	}
	return record, nil
}

func (repository *BackupRuntimeRepository) ListBackupRecoveryPointsByEnvironment(
	ctx context.Context,
	environmentID string,
	request BackupRuntimeListRequest,
) (BackupRuntimePage[backupruntime.BackupRecoveryPointRecord], error) {
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	return repository.listBackupRecoveryPoints(
		ctx,
		backupruntime.BackupRecoveryPointEnvironmentPrefix+environmentID+"/",
		request,
		func(point backupruntime.BackupRecoveryPointRecord) bool { return point.EnvironmentID == environmentID },
		func(point backupruntime.BackupRecoveryPointRecord) (string, error) {
			return backupruntime.BackupRecoveryPointEnvironmentIndexKey(environmentID, point.ID)
		},
	)
}

// ListVerifiedRecoveryPointsByEnvironment keeps every storage-layout detail
// inside the repository while preserving the verified-only fixed revision.
type backupRecoveryPointPageReader func(
	context.Context,
	string,
	BackupRuntimeListRequest,
) (BackupRuntimePage[backupruntime.BackupRecoveryPointRecord], error)

func (repository *BackupRuntimeRepository) ListVerifiedRecoveryPointsByEnvironment(
	ctx context.Context,
	environmentID string,
	request BackupRecoveryPointPageRequest,
) (BackupRecoveryPointPage, error) {
	return collectVerifiedRecoveryPointPage(
		ctx,
		environmentID,
		request,
		repository.ListBackupRecoveryPointsByEnvironment,
	)
}

func collectVerifiedRecoveryPointPage(
	ctx context.Context,
	environmentID string,
	request BackupRecoveryPointPageRequest,
	read backupRecoveryPointPageReader,
) (BackupRecoveryPointPage, error) {
	storageRequest := BackupRuntimeListRequest{Limit: request.Limit, Revision: request.Revision}
	if request.AfterID != "" {
		boundary, err := backupruntime.BackupRecoveryPointEnvironmentIndexKey(environmentID, request.AfterID)
		if err != nil {
			return BackupRecoveryPointPage{}, err
		}
		storageRequest.StartExclusive = boundary
	}

	result := BackupRecoveryPointPage{}
	for {
		storageRequest.Limit = request.Limit - len(result.Items)
		page, err := read(ctx, environmentID, storageRequest)
		if err != nil {
			return BackupRecoveryPointPage{}, err
		}
		if page.Revision <= 0 {
			return BackupRecoveryPointPage{}, errs.New(
				errs.KindInternal,
				"recovery point list returned no fixed revision",
			)
		}
		if result.Revision == 0 {
			result.Revision = page.Revision
		} else if page.Revision != result.Revision {
			return BackupRecoveryPointPage{}, errs.New(
				errs.KindInternal,
				"recovery point list changed fixed revision",
			)
		}
		if len(page.Items) > storageRequest.Limit {
			return BackupRecoveryPointPage{}, errs.New(
				errs.KindInternal,
				"recovery point list exceeded the visible page limit",
			)
		}
		result.Items = append(result.Items, page.Items...)
		if len(result.Items) == request.Limit || page.Next == "" {
			if page.Next != "" {
				result.NextID, err = backupruntime.BackupRecoveryPointIDFromEnvironmentIndexKey(environmentID, page.Next)
				if err != nil {
					return BackupRecoveryPointPage{}, err
				}
			}
			return result, nil
		}
		if page.Next == storageRequest.StartExclusive {
			return BackupRecoveryPointPage{}, errs.New(
				errs.KindInternal,
				"recovery point list boundary did not advance",
			)
		}
		storageRequest.StartExclusive = page.Next
		storageRequest.Revision = result.Revision
	}
}

func (repository *BackupRuntimeRepository) ListBackupRecoveryPointsBySource(
	ctx context.Context,
	sourceID string,
	request BackupRuntimeListRequest,
) (BackupRuntimePage[backupruntime.BackupRecoveryPointRecord], error) {
	if err := recordcodec.ValidateID(ids.KindBackupSource, sourceID); err != nil {
		return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	return repository.listBackupRecoveryPoints(
		ctx,
		backupruntime.BackupRecoveryPointSourcePrefix+sourceID+"/",
		request,
		func(point backupruntime.BackupRecoveryPointRecord) bool { return point.SourceID == sourceID },
		func(point backupruntime.BackupRecoveryPointRecord) (string, error) {
			return backupruntime.BackupRecoveryPointSourceIndexKey(sourceID, point.ID)
		},
	)
}

func (repository *BackupRuntimeRepository) ListBackupRecoveryPointsByConnector(
	ctx context.Context,
	connectorID string,
	request BackupRuntimeListRequest,
) (BackupRuntimePage[backupruntime.BackupRecoveryPointRecord], error) {
	if err := recordcodec.ValidateID(ids.KindConnector, connectorID); err != nil {
		return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	return repository.listBackupRecoveryPoints(
		ctx,
		backupruntime.BackupRecoveryPointConnectorPrefix+connectorID+"/",
		request,
		func(point backupruntime.BackupRecoveryPointRecord) bool { return point.ConnectorID == connectorID },
		func(point backupruntime.BackupRecoveryPointRecord) (string, error) {
			return backupruntime.BackupRecoveryPointConnectorIndexKey(connectorID, point.ID)
		},
	)
}

func (repository *BackupRuntimeRepository) listBackupRecoveryPoints(
	ctx context.Context,
	prefix string,
	request BackupRuntimeListRequest,
	belongs func(backupruntime.BackupRecoveryPointRecord) bool,
	indexKey func(backupruntime.BackupRecoveryPointRecord) (string, error),
) (BackupRuntimePage[backupruntime.BackupRecoveryPointRecord], error) {
	if err := validateBackupRuntimeListRequest(prefix, request); err != nil {
		return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	index, err := repository.store.Range(ctx, etcdstore.RangeRequest{
		Prefix: prefix, StartExclusive: request.StartExclusive,
		Limit: int64(request.Limit), Revision: request.Revision,
	})
	if err != nil {
		return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	if index == nil || index.ReadRevision <= 0 {
		return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, errs.New(
			errs.KindInternal,
			"recovery point index page is incomplete",
		)
	}
	defer clearRangeValues(index.Values)
	keys := make([]string, 0, len(index.Values)*2)
	pointIDs := make([]string, len(index.Values))
	for position, item := range index.Values {
		pointID := string(item.Value)
		if recordcodec.ValidateID(ids.KindRecoveryPoint, pointID) != nil {
			return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		keys = append(keys, backupruntime.BackupRecoveryPointKey(pointID), backupruntime.BackupRecoveryPointPruneKey(pointID))
		pointIDs[position] = pointID
	}
	page := BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{Revision: index.ReadRevision}
	if len(keys) == 0 {
		return page, nil
	}
	points, err := repository.readBackupRecoveryPointPageChunks(ctx, keys, index.ReadRevision)
	if err != nil {
		return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	if points == nil || points.ReadRevision != index.ReadRevision ||
		len(points.Values) != len(keys) {
		return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, errs.New(
			errs.KindInternal,
			"recovery point fixed-revision page is incomplete",
		)
	}
	defer clearKeyValues(points.Values)
	records := make([]backupruntime.BackupRecoveryPointRecord, len(pointIDs))
	visible := make([]bool, len(pointIDs))
	companionKeys := make([]string, 0, len(pointIDs)*3)
	for position, pointID := range pointIDs {
		item := points.Values[position*2]
		if item == nil {
			return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		point, decodeErr := backupruntime.DecodeBackupRecoveryPointRecord(item.Value)
		if decodeErr != nil || point.ID != pointID || !belongs(point) || item.Version != 1 ||
			index.Values[position].Version != 1 ||
			index.Values[position].ModRevision != item.ModRevision {
			return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		expectedIndexKey, keyErr := indexKey(point)
		if keyErr != nil || expectedIndexKey != index.Values[position].Key {
			return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		environmentIndex, keyErr := backupruntime.BackupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
		if keyErr != nil {
			return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		sourceIndex, keyErr := backupruntime.BackupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
		if keyErr != nil {
			return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		connectorIndex, keyErr := backupruntime.BackupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
		if keyErr != nil {
			return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		records[position] = point
		companionKeys = append(companionKeys, environmentIndex, sourceIndex, connectorIndex)
		if pruneValue := points.Values[position*2+1]; pruneValue != nil {
			prune, pruneErr := backupruntime.DecodeBackupRecoveryPointPruneRecord(pruneValue.Value)
			if pruneErr != nil || prune.Point != point.BackupRecoveryPointSnapshot ||
				prune.State == backupruntime.BackupPruneVerifiedAbsent {
				return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
			}
			continue
		}
		visible[position] = true
	}
	companions, err := repository.readBackupRecoveryPointPageChunks(
		ctx,
		companionKeys,
		index.ReadRevision,
	)
	if err != nil {
		return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, err
	}
	if companions == nil || companions.ReadRevision != index.ReadRevision ||
		len(companions.Values) != len(companionKeys) {
		return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, errs.New(
			errs.KindInternal,
			"recovery point companion page is incomplete",
		)
	}
	defer clearKeyValues(companions.Values)
	page.Items = make([]etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord], 0, len(pointIDs))
	for position, point := range records {
		primary := points.Values[position*2]
		for offset := range 3 {
			companion := companions.Values[position*3+offset]
			expectedKey := companionKeys[position*3+offset]
			if companion == nil || companion.Key != expectedKey || companion.Version != 1 ||
				companion.ModRevision != primary.ModRevision || string(companion.Value) != point.ID {
				return BackupRuntimePage[backupruntime.BackupRecoveryPointRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
			}
		}
		if visible[position] {
			page.Items = append(page.Items, etcdstore.Versioned[backupruntime.BackupRecoveryPointRecord]{
				Record: point, Revision: primary.ModRevision, ReadRevision: index.ReadRevision,
			})
		}
	}
	if index.More {
		page.Next = index.Values[len(index.Values)-1].Key
	}
	return page, nil
}

func (repository *BackupRuntimeRepository) readBackupRecoveryPointPageChunks(
	ctx context.Context,
	keys []string,
	revision int64,
) (*etcdstore.GetManyResult, error) {
	if len(keys) == 0 || revision <= 0 {
		return nil, errs.New(errs.KindValidationFailed, "recovery point page chunk input is invalid")
	}
	combined := &etcdstore.GetManyResult{ReadRevision: revision, Values: make([]*etcdstore.KeyValue, 0, len(keys))}
	for start := 0; start < len(keys); start += etcdstore.MaximumOperations {
		end := min(start+etcdstore.MaximumOperations, len(keys))
		chunk, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: keys[start:end], Revision: revision,
		})
		if err != nil {
			clearKeyValues(combined.Values)
			return nil, err
		}
		if chunk == nil || chunk.ReadRevision != revision || len(chunk.Values) != end-start {
			if chunk != nil {
				clearKeyValues(chunk.Values)
			}
			clearKeyValues(combined.Values)
			return nil, errs.New(
				errs.KindInternal,
				"recovery point fixed-revision page chunk is incomplete",
			)
		}
		for position, value := range chunk.Values {
			if value != nil && value.Key != keys[start+position] {
				clearKeyValues(chunk.Values)
				clearKeyValues(combined.Values)
				return nil, errs.New(
					errs.KindInternal,
					"recovery point fixed-revision page chunk is corrupt",
				)
			}
		}
		combined.Values = append(combined.Values, chunk.Values...)
		chunk.Values = nil
	}
	return combined, nil
}
