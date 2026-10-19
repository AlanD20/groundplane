package etcd

import (
	"context"
	"io"

	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type releaseLogMemoryStore struct {
	*memoryHierarchyStore
}

func (store *releaseLogMemoryStore) Health(context.Context) error { return nil }
func (store *releaseLogMemoryStore) Put(ctx context.Context, key string, value []byte) (int64, error) {
	result, err := store.Transact(
		ctx,
		nil,
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: key, Value: value}},
	)
	return result.Revision, err
}
func (store *releaseLogMemoryStore) Delete(ctx context.Context, key string) (int64, error) {
	result, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationDelete, Key: key}})
	return result.Revision, err
}
func (store *releaseLogMemoryStore) Watch(context.Context, string, int64) (*testkeyvalue.WatchStream, error) {
	return nil, errs.New(errs.KindInternal, "memory fixture does not implement watches")
}
func (store *releaseLogMemoryStore) Snapshot(context.Context, io.Writer) error {
	return errs.New(errs.KindInternal, "memory fixture does not implement snapshots")
}
func (store *releaseLogMemoryStore) Close() error { return nil }
