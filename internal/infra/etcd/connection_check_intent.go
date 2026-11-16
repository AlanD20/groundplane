package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
)

func connectionCheckScope(
	ctx context.Context,
	store hierarchyStore,
	environmentID string,
) (workexecution.Scope, error) {
	reader := hierarchy.NewReader(store)
	environment, err := reader.GetEnvironment(ctx, environmentID)
	if err != nil {
		return workexecution.Scope{}, err
	}
	project, err := reader.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return workexecution.Scope{}, err
	}
	return workexecution.Scope{
		TenantID:      project.Record.TenantID,
		ProjectID:     project.Record.ID,
		EnvironmentID: environmentID,
	}, nil
}

// ConnectionCheckScope reads only stable hierarchy identity. Runtime, access
// credentials, desired projections and observation plans remain deferred.
func (repository *AttachRepository) ConnectionCheckScope(
	ctx context.Context,
	environmentID string,
) (workexecution.Scope, error) {
	return connectionCheckScope(ctx, repository.store, environmentID)
}

func (repository *RouteRepository) ConnectionCheckScope(
	ctx context.Context,
	environmentID string,
) (workexecution.Scope, error) {
	return connectionCheckScope(ctx, repository.store, environmentID)
}
