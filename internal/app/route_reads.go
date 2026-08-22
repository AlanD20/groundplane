package app

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type routeReadRepository interface {
	GetEnvironment(context.Context, string) (etcd.Versioned[etcd.EnvironmentRecord], error)
	GetRoute(context.Context, string) (etcd.Versioned[etcd.RouteRecord], error)
	ListRoutes(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.RouteRecord], error)
}

type routeReadService struct {
	repository routeReadRepository
}

func newRouteReadService(repository routeReadRepository) (*routeReadService, error) {
	if repository == nil {
		return nil, errs.New(errs.KindInternal, "Route read repository is not configured")
	}
	return &routeReadService{repository: repository}, nil
}

func (service *routeReadService) ListRoutes(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.RouteRecord], error) {
	if ctx == nil {
		return etcd.Page[etcd.RouteRecord]{}, errs.New(errs.KindInternal, "Route list context is required")
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return etcd.Page[etcd.RouteRecord]{}, errs.New(
			errs.KindValidationFailed,
			"Route list requires a stable Environment id",
		)
	}
	if request.Limit < 0 {
		return etcd.Page[etcd.RouteRecord]{}, errs.New(
			errs.KindValidationFailed,
			"Route list limit must be a positive integer",
		)
	}
	if _, err := service.repository.GetEnvironment(ctx, environmentID); err != nil {
		return etcd.Page[etcd.RouteRecord]{}, err
	}
	return service.repository.ListRoutes(ctx, environmentID, request)
}

func (service *routeReadService) GetRoute(
	ctx context.Context,
	routeID string,
) (etcd.Versioned[etcd.RouteRecord], error) {
	if ctx == nil {
		return etcd.Versioned[etcd.RouteRecord]{}, errs.New(errs.KindInternal, "Route read context is required")
	}
	if ids.Validate(ids.KindRoute, routeID) != nil {
		return etcd.Versioned[etcd.RouteRecord]{}, errs.New(
			errs.KindValidationFailed,
			"Route read requires a stable Route id",
		)
	}
	return service.repository.GetRoute(ctx, routeID)
}

type durableRouteReadRepository struct {
	hierarchy *etcd.HierarchyRepository
	routes    *etcd.RouteRepository
}

func newDurableRouteReadRepository(
	hierarchy *etcd.HierarchyRepository,
	routes *etcd.RouteRepository,
) (*durableRouteReadRepository, error) {
	if hierarchy == nil || routes == nil {
		return nil, errs.New(errs.KindInternal, "Route read repositories are not configured")
	}
	return &durableRouteReadRepository{hierarchy: hierarchy, routes: routes}, nil
}

func (repository *durableRouteReadRepository) GetEnvironment(
	ctx context.Context,
	id string,
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	return repository.hierarchy.GetEnvironment(ctx, id)
}

func (repository *durableRouteReadRepository) GetRoute(
	ctx context.Context,
	id string,
) (etcd.Versioned[etcd.RouteRecord], error) {
	return repository.routes.GetRoute(ctx, id)
}

func (repository *durableRouteReadRepository) ListRoutes(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.RouteRecord], error) {
	return repository.routes.ListRoutes(ctx, environmentID, request)
}
