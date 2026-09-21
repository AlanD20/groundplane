package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *BackupRuntimeRepository) ListBackupRunsByEnvironment(
	ctx context.Context,
	environmentID string,
	request BackupRuntimeListRequest,
) (BackupRuntimePage[backupruntime.BackupRunRecord], error) {
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return BackupRuntimePage[backupruntime.BackupRunRecord]{}, err
	}
	prefix := backupruntime.BackupRunEnvironmentPrefix + environmentID + "/"
	if err := validateBackupRuntimeListRequest(prefix, request); err != nil {
		return BackupRuntimePage[backupruntime.BackupRunRecord]{}, err
	}
	index, err := repository.store.Range(ctx, etcdstore.RangeRequest{
		Prefix: prefix, StartExclusive: request.StartExclusive,
		Limit: int64(request.Limit), Revision: request.Revision,
	})
	if err != nil {
		return BackupRuntimePage[backupruntime.BackupRunRecord]{}, err
	}
	if index == nil || index.ReadRevision <= 0 {
		return BackupRuntimePage[backupruntime.BackupRunRecord]{}, errs.New(
			errs.KindInternal,
			"backup run environment index page is incomplete",
		)
	}
	defer clearRangeValues(index.Values)
	keys := make([]string, len(index.Values))
	for position, item := range index.Values {
		taskID := string(item.Value)
		if recordcodec.ValidateID(ids.KindTask, taskID) != nil ||
			item.Key != prefix+taskID {
			return BackupRuntimePage[backupruntime.BackupRunRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		keys[position] = backupruntime.BackupRunKey(taskID)
	}
	return repository.readBackupRunMembershipPage(ctx, environmentID, keys, index)
}

func (repository *BackupRuntimeRepository) readBackupRunMembershipPage(
	ctx context.Context,
	environmentID string,
	keys []string,
	index *etcdstore.RangeResult,
) (BackupRuntimePage[backupruntime.BackupRunRecord], error) {
	page := BackupRuntimePage[backupruntime.BackupRunRecord]{Revision: index.ReadRevision}
	if len(keys) == 0 {
		return page, nil
	}
	primaries, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: keys, Revision: index.ReadRevision},
	)
	if err != nil {
		return BackupRuntimePage[backupruntime.BackupRunRecord]{}, err
	}
	if primaries == nil || primaries.ReadRevision != index.ReadRevision ||
		len(primaries.Values) != len(keys) {
		return BackupRuntimePage[backupruntime.BackupRunRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	defer etcdstore.ClearValues(primaries.Values)
	page.Items = make([]etcdstore.Versioned[backupruntime.BackupRunRecord], len(keys))
	for position, value := range primaries.Values {
		if value == nil {
			return BackupRuntimePage[backupruntime.BackupRunRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		record, decodeErr := backupruntime.DecodeBackupRunRecord(value.Value)
		expectedIndex, keyErr := backupruntime.BackupRunEnvironmentIndexKey(environmentID, record.TaskID)
		if decodeErr != nil || keyErr != nil || record.EnvironmentID != environmentID ||
			expectedIndex != index.Values[position].Key || index.Values[position].Version != 1 ||
			index.Values[position].ModRevision > value.ModRevision ||
			string(index.Values[position].Value) != record.TaskID {
			return BackupRuntimePage[backupruntime.BackupRunRecord]{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		page.Items[position] = etcdstore.Versioned[backupruntime.BackupRunRecord]{
			Record: record, Revision: value.ModRevision, ReadRevision: index.ReadRevision,
		}
	}
	if index.More {
		page.Next = index.Values[len(index.Values)-1].Key
	}
	return page, nil
}
