package backupruntime

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func getOptionalBackupRuntimeRecord[T any](
	ctx context.Context,
	store readStore,
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
		return etcdstore.Versioned[T]{}, false, CorruptBackupRuntimeRecord()
	}
	return etcdstore.Versioned[T]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}
