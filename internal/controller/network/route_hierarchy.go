package network

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
)

func (service *routeMutationService) routeHierarchy(
	ctx context.Context,
	environmentID string,
	targetServiceID string,
) (
	etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	etcdstore.Versioned[servicerecord.ServiceRecord],
	error,
) {
	environment, err := service.repository.GetEnvironment(ctx, environmentID)
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, etcdstore.Versioned[hierarchyrecord.ProjectRecord]{},
			etcdstore.Versioned[servicerecord.ServiceRecord]{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, etcdstore.Versioned[hierarchyrecord.ProjectRecord]{},
			etcdstore.Versioned[servicerecord.ServiceRecord]{}, err
	}
	target, err := service.repository.GetService(ctx, targetServiceID)
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, etcdstore.Versioned[hierarchyrecord.ProjectRecord]{},
			etcdstore.Versioned[servicerecord.ServiceRecord]{}, err
	}
	return environment, project, target, nil
}
