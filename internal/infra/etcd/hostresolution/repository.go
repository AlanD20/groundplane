package hostresolution

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type projectionStore interface {
	Get(context.Context, string) (*etcdstore.GetResult, error)
}
type Repository struct{ store projectionStore }

func New(store etcdstore.Store) (*Repository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "host-resolution projection store is required")
	}
	return &Repository{store: store}, nil
}
func (repository *Repository) GetHostResolutionProjection(
	ctx context.Context,
) (etcdstore.Versioned[HostResolutionProjectionRecord], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[HostResolutionProjectionRecord]{}, false, err
	}
	result, err := repository.store.Get(ctx, StorageKey)
	if err != nil {
		return etcdstore.Versioned[HostResolutionProjectionRecord]{}, false, err
	}
	if result == nil || result.Entry == nil {
		readRevision := int64(0)
		if result != nil {
			readRevision = result.ReadRevision
		}
		return etcdstore.Versioned[HostResolutionProjectionRecord]{ReadRevision: readRevision}, false, nil
	}
	record, err := DecodeHostResolutionProjectionRecord(result.Entry.Value)
	if err != nil {
		return etcdstore.Versioned[HostResolutionProjectionRecord]{}, false, err
	}
	return etcdstore.Versioned[HostResolutionProjectionRecord]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}
