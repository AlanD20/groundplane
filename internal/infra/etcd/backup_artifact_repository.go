package etcd

import (
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func allBackupRuntimeValuesAbsent(values []*etcdstore.KeyValue) bool {
	for _, value := range values {
		if value != nil {
			return false
		}
	}
	return true
}

func getOptionalBackupRuntimeRecord[T any](
	ctx context.Context,
	store hierarchyStore,
	key string,
	stableID string,
	decode func([]byte) (T, error),
	id func(T) string,
) (etcdstore.Versioned[T], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[T]{}, false, err
	}
	result, err := store.Get(ctx, key)
	if err != nil {
		return etcdstore.Versioned[T]{}, false, err
	}
	if result == nil {
		return etcdstore.Versioned[T]{}, false, errs.New(
			errs.KindInternal,
			"backup runtime record read is empty",
		)
	}
	if result.Entry == nil {
		return etcdstore.Versioned[T]{ReadRevision: result.ReadRevision}, false, nil
	}
	defer clear(result.Entry.Value)
	record, err := decode(result.Entry.Value)
	if err != nil || id(record) != stableID {
		return etcdstore.Versioned[T]{}, false, backupruntime.CorruptBackupRuntimeRecord()
	}
	return etcdstore.Versioned[T]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}

func clearRangeValues(values []etcdstore.KeyValue) {
	for index := range values {
		clear(values[index].Value)
		values[index].Value = nil
	}
}
