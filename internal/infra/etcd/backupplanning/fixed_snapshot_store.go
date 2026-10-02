package backupplanning

import (
	"context"

	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// fixedSnapshotStore confines existing projection readers, including their
// immutable-key reads, to the one MVCC view selected by preparation's anchor.
type fixedSnapshotStore struct {
	store    readStore
	revision int64
}

func (store fixedSnapshotStore) Get(ctx context.Context, key string) (*etcdstore.GetResult, error) {
	read, err := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}})
	if err != nil {
		return nil, err
	}
	return &etcdstore.GetResult{Entry: read.Values[0], ReadRevision: read.ReadRevision}, nil
}

func (store fixedSnapshotStore) GetMany(
	ctx context.Context,
	request etcdstore.GetManyRequest,
) (*etcdstore.GetManyResult, error) {
	if store.revision <= 0 || request.Revision != 0 && request.Revision != store.revision {
		return nil, errs.New(errs.KindInternal, "backup snapshot read revision diverged")
	}
	request.Revision = store.revision
	read, err := store.store.GetMany(ctx, request)
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != store.revision || len(read.Values) != len(request.Keys) {
		if read != nil {
			etcdstore.ClearValues(read.Values)
		}
		return nil, errs.New(errs.KindInternal, "backup snapshot read is incomplete")
	}
	return read, nil
}

func (store fixedSnapshotStore) Range(
	ctx context.Context,
	request etcdstore.RangeRequest,
) (*etcdstore.RangeResult, error) {
	if store.revision <= 0 || request.Revision != 0 && request.Revision != store.revision {
		return nil, errs.New(errs.KindInternal, "backup snapshot range revision diverged")
	}
	request.Revision = store.revision
	read, err := store.store.Range(ctx, request)
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != store.revision {
		if read != nil {
			etcdstore.ClearRangeValues(read.Values)
		}
		return nil, errs.New(errs.KindInternal, "backup snapshot range is incomplete")
	}
	return read, nil
}
