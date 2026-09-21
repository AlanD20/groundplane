package backupruntime

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Reader) ListBackupRunsByEnvironment(
	ctx context.Context,
	environmentID string,
	request BackupRuntimeListRequest,
) (BackupRuntimePage[BackupRunRecord], error) {
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return BackupRuntimePage[BackupRunRecord]{}, err
	}
	prefix := BackupRunEnvironmentPrefix + environmentID + "/"
	if err := ValidateBackupRuntimeListRequest(prefix, request); err != nil {
		return BackupRuntimePage[BackupRunRecord]{}, err
	}
	index, err := repository.store.Range(ctx, etcdstore.RangeRequest{
		Prefix: prefix, StartExclusive: request.StartExclusive,
		Limit: int64(request.Limit), Revision: request.Revision,
	})
	if err != nil {
		return BackupRuntimePage[BackupRunRecord]{}, err
	}
	if index == nil || index.ReadRevision <= 0 {
		return BackupRuntimePage[BackupRunRecord]{}, errs.New(
			errs.KindInternal,
			"backup run environment index page is incomplete",
		)
	}
	defer etcdstore.ClearRangeValues(index.Values)
	keys := make([]string, len(index.Values))
	for position, item := range index.Values {
		taskID := string(item.Value)
		if recordcodec.ValidateID(ids.KindTask, taskID) != nil ||
			item.Key != prefix+taskID {
			return BackupRuntimePage[BackupRunRecord]{}, CorruptBackupRuntimeRecord()
		}
		keys[position] = BackupRunKey(taskID)
	}
	return repository.readBackupRunMembershipPage(ctx, environmentID, keys, index)
}

func (repository *Reader) readBackupRunMembershipPage(
	ctx context.Context,
	environmentID string,
	keys []string,
	index *etcdstore.RangeResult,
) (BackupRuntimePage[BackupRunRecord], error) {
	page := BackupRuntimePage[BackupRunRecord]{Revision: index.ReadRevision}
	if len(keys) == 0 {
		return page, nil
	}
	primaries, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: keys, Revision: index.ReadRevision},
	)
	if err != nil {
		return BackupRuntimePage[BackupRunRecord]{}, err
	}
	if primaries == nil || primaries.ReadRevision != index.ReadRevision ||
		len(primaries.Values) != len(keys) {
		return BackupRuntimePage[BackupRunRecord]{}, CorruptBackupRuntimeRecord()
	}
	defer etcdstore.ClearValues(primaries.Values)
	page.Items = make([]etcdstore.Versioned[BackupRunRecord], len(keys))
	for position, value := range primaries.Values {
		if value == nil {
			return BackupRuntimePage[BackupRunRecord]{}, CorruptBackupRuntimeRecord()
		}
		record, decodeErr := DecodeBackupRunRecord(value.Value)
		expectedIndex, keyErr := BackupRunEnvironmentIndexKey(environmentID, record.TaskID)
		if decodeErr != nil || keyErr != nil || record.EnvironmentID != environmentID ||
			expectedIndex != index.Values[position].Key || index.Values[position].Version != 1 ||
			index.Values[position].ModRevision > value.ModRevision ||
			string(index.Values[position].Value) != record.TaskID {
			return BackupRuntimePage[BackupRunRecord]{}, CorruptBackupRuntimeRecord()
		}
		page.Items[position] = etcdstore.Versioned[BackupRunRecord]{
			Record: record, Revision: value.ModRevision, ReadRevision: index.ReadRevision,
		}
	}
	if index.More {
		page.Next = index.Values[len(index.Values)-1].Key
	}
	return page, nil
}
