package network

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"

	"github.com/AlanD20/groundplane/internal/common/ids"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type routeReadRepository interface {
	GetEnvironment(context.Context, string) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error)
	GetRoute(context.Context, string) (etcdstore.Versioned[routerecord.Record], error)
	ListRoutes(context.Context, string, etcdstore.PageRequest) (etcdstore.Page[routerecord.Record], error)
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
	request etcdstore.PageRequest,
) (etcdstore.Page[routerecord.Record], error) {
	if ctx == nil {
		return etcdstore.Page[routerecord.Record]{}, errs.New(errs.KindInternal, "Route list context is required")
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return etcdstore.Page[routerecord.Record]{}, errs.New(
			errs.KindValidationFailed,
			"Route list requires a stable Environment id",
		)
	}
	if request.Limit < 0 {
		return etcdstore.Page[routerecord.Record]{}, errs.New(
			errs.KindValidationFailed,
			"Route list limit must be a positive integer",
		)
	}
	if _, err := service.repository.GetEnvironment(ctx, environmentID); err != nil {
		return etcdstore.Page[routerecord.Record]{}, err
	}
	return service.repository.ListRoutes(ctx, environmentID, request)
}

func (service *routeReadService) GetRoute(
	ctx context.Context,
	routeID string,
) (etcdstore.Versioned[routerecord.Record], error) {
	if ctx == nil {
		return etcdstore.Versioned[routerecord.Record]{}, errs.New(errs.KindInternal, "Route read context is required")
	}
	if ids.Validate(ids.KindRoute, routeID) != nil {
		return etcdstore.Versioned[routerecord.Record]{}, errs.New(
			errs.KindValidationFailed,
			"Route read requires a stable Route id",
		)
	}
	return service.repository.GetRoute(ctx, routeID)
}
