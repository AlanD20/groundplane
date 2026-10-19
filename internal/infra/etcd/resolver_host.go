package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/resolverwork"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) ResolverWorkRepository() (*resolverwork.Repository, error) {
	return resolverwork.NewRepository(repository.store)
}

func (repository *TaskRepository) SelectResolverHost(
	ctx context.Context,
) (keyvalue.Versioned[hostbindings.Platform], error) {
	host, err := hostbindings.ReadLocal(ctx, repository.store, 0)
	if err != nil {
		return keyvalue.Versioned[hostbindings.Platform]{}, err
	}
	platform, err := hostbindings.ReadPlatform(ctx, repository.store, host.Binding.HostID, host.ReadRevision)
	if err != nil {
		return keyvalue.Versioned[hostbindings.Platform]{}, err
	}
	if platform.Revision == 0 {
		return keyvalue.Versioned[hostbindings.Platform]{}, errs.New(
			errs.KindStateConflict,
			"CoreDNS is waiting for the enrolled Agent to report its host platform",
		)
	}
	return platform, nil
}
