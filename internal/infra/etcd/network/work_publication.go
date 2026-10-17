package network

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

func (repository *Repository) ResolveResourceHost(ctx context.Context) (workexecution.Placement, error) {
	return repository.publication.ResolveResourceHost(ctx)
}

func (repository *Repository) PublishNetworkWork(ctx context.Context, request etcd.NetworkWorkPublication) (etcd.IdempotencyTransactionResult, error) {
	return repository.publication.PublishNetworkWork(ctx, request)
}
