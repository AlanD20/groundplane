package app

import (
	"context"
	"errors"
	"io"

	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type releasePlanningTestStore struct {
	*memoryHierarchyStore
}

func (*memoryHierarchyStore) Health(context.Context) error { return nil }

func (store *memoryHierarchyStore) Put(ctx context.Context, key string, value []byte) (int64, error) {
	result, err := store.Transact(
		ctx,
		nil,
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: key, Value: value}},
	)
	return result.Revision, err
}

func (store *memoryHierarchyStore) Delete(ctx context.Context, key string) (int64, error) {
	result, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationDelete, Key: key}})
	return result.Revision, err
}

func (*memoryHierarchyStore) Watch(context.Context, string, int64) (*testkeyvalue.WatchStream, error) {
	return nil, errors.New("release planning test store does not support watches")
}

func (*memoryHierarchyStore) Snapshot(context.Context, io.Writer) error {
	return errors.New("release planning test store does not support snapshots")
}

func (*memoryHierarchyStore) Close() error { return nil }
