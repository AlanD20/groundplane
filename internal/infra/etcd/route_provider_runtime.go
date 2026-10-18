package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *RouteRepository) GetRouteProviderRuntimeAtRevision(
	ctx context.Context, componentID string, revision int64,
) (keyvalue.Versioned[componentplanning.Runtime], bool, error) {
	if repository == nil || revision <= 0 {
		return keyvalue.Versioned[componentplanning.Runtime]{}, false,
			errs.New(errs.KindValidationFailed, "Route router runtime requires a fixed snapshot")
	}
	return componentplanning.ReadRuntime(ctx, repository.store, componentID, revision)
}
