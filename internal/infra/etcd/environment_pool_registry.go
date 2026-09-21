package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	networkreservations "github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

func (repository *HierarchyRepository) GetEnvironmentPoolRegistry(
	ctx context.Context,
) (etcdstore.Versioned[networkreservations.EnvironmentPoolRegistry], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[networkreservations.EnvironmentPoolRegistry]{}, err
	}
	result, err := repository.store.Get(ctx, networkreservations.EnvironmentPoolRegistryKey)
	if err != nil {
		return etcdstore.Versioned[networkreservations.EnvironmentPoolRegistry]{}, err
	}
	if result.Entry == nil {
		return etcdstore.Versioned[networkreservations.EnvironmentPoolRegistry]{
			Record:       networkreservations.EnvironmentPoolRegistry{Reservations: map[string]string{}},
			ReadRevision: result.ReadRevision,
		}, nil
	}
	registry, err := recordcodec.Decode[networkreservations.EnvironmentPoolRegistry](result.Entry.Value, "environment_pool_registry")
	if err != nil || networkreservations.ValidateEnvironmentPoolRegistry(registry) != nil {
		return etcdstore.Versioned[networkreservations.EnvironmentPoolRegistry]{}, networkreservations.CorruptEnvironmentPoolRegistry()
	}
	return etcdstore.Versioned[networkreservations.EnvironmentPoolRegistry]{
		Record: registry, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, nil
}
