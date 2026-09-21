package backupruntime

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Reader) GetBackupSourceTargetExclusion(
	ctx context.Context,
	kind BackupSourceTargetKind,
	targetID string,
) (etcdstore.Versioned[BackupSourceTargetExclusionRecord], bool, error) {
	key, err := BackupSourceTargetExclusionKey(kind, targetID)
	if err != nil {
		return etcdstore.Versioned[BackupSourceTargetExclusionRecord]{}, false, err
	}
	result, err := repository.store.Get(ctx, key)
	if err != nil {
		return etcdstore.Versioned[BackupSourceTargetExclusionRecord]{}, false, err
	}
	if result == nil {
		return etcdstore.Versioned[BackupSourceTargetExclusionRecord]{}, false, errs.New(
			errs.KindInternal,
			"backup source-target exclusion read is empty",
		)
	}
	if result.Entry == nil {
		return etcdstore.Versioned[BackupSourceTargetExclusionRecord]{
			ReadRevision: result.ReadRevision,
		}, false, nil
	}
	defer clear(result.Entry.Value)
	record, err := DecodeBackupSourceTargetExclusionRecord(result.Entry.Value)
	if err != nil || record.TargetKind != kind || record.TargetID != targetID {
		return etcdstore.Versioned[BackupSourceTargetExclusionRecord]{}, false, CorruptBackupRuntimeRecord()
	}
	return etcdstore.Versioned[BackupSourceTargetExclusionRecord]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}

func (repository *Reader) ReadCurrentKeys(
	ctx context.Context,
	keys []string,
) (*etcdstore.GetManyResult, error) {
	if len(keys) == 0 || len(keys) > etcdstore.MaximumOperations {
		return nil, errs.New(
			errs.KindValidationFailed,
			"backup runtime fixed read key count is invalid",
		)
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return nil, err
	}
	if result == nil || result.ReadRevision <= 0 || len(result.Values) != len(keys) {
		return nil, errs.New(errs.KindInternal, "backup runtime fixed-revision read is incomplete")
	}
	for index, value := range result.Values {
		if value != nil && value.Key != keys[index] {
			etcdstore.ClearValues(result.Values)
			return nil, errs.New(errs.KindInternal, "backup runtime fixed-revision read is corrupt")
		}
	}
	return result, nil
}

func (repository *Reader) ReadFixedKeys(
	ctx context.Context,
	keys []string,
	revision int64,
) (*etcdstore.GetManyResult, error) {
	if len(keys) == 0 || len(keys) > etcdstore.MaximumOperations || revision <= 0 {
		return nil, errs.New(
			errs.KindValidationFailed,
			"backup runtime fixed read input is invalid",
		)
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if result == nil || result.ReadRevision != revision || len(result.Values) != len(keys) {
		return nil, errs.New(errs.KindInternal, "backup runtime fixed-revision read is incomplete")
	}
	for index, value := range result.Values {
		if value != nil && value.Key != keys[index] {
			etcdstore.ClearValues(result.Values)
			return nil, errs.New(errs.KindInternal, "backup runtime fixed-revision read is corrupt")
		}
	}
	return result, nil
}
