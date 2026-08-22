package app

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type serviceReadRepository interface {
	GetEnvironment(context.Context, string) (etcd.Versioned[etcd.EnvironmentRecord], error)
	ListServices(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.ServiceRecord], error)
}

type serviceReadService struct {
	repository serviceReadRepository
}

func newServiceReadService(repository serviceReadRepository) (*serviceReadService, error) {
	if repository == nil {
		return nil, errs.New(errs.KindInternal, "Service read repository is not configured")
	}
	return &serviceReadService{repository: repository}, nil
}

func (service *serviceReadService) ListServices(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.ServiceRecord], error) {
	if ctx == nil {
		return etcd.Page[etcd.ServiceRecord]{}, errs.New(errs.KindInternal, "Service list context is required")
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return etcd.Page[etcd.ServiceRecord]{}, errs.New(
			errs.KindValidationFailed,
			"Service list requires a stable Environment id",
		)
	}
	if request.Limit < 0 {
		return etcd.Page[etcd.ServiceRecord]{}, errs.New(
			errs.KindValidationFailed,
			"Service list limit must be a positive integer",
		)
	}
	if _, err := service.repository.GetEnvironment(ctx, environmentID); err != nil {
		return etcd.Page[etcd.ServiceRecord]{}, err
	}
	return service.repository.ListServices(ctx, environmentID, request)
}

type durableServiceReadRepository struct {
	hierarchy *etcd.HierarchyRepository
	services  *etcd.ServiceRepository
}

func newDurableServiceReadRepository(
	hierarchy *etcd.HierarchyRepository,
	services *etcd.ServiceRepository,
) (*durableServiceReadRepository, error) {
	if hierarchy == nil || services == nil {
		return nil, errs.New(errs.KindInternal, "Service read repositories are not configured")
	}
	return &durableServiceReadRepository{hierarchy: hierarchy, services: services}, nil
}

func (repository *durableServiceReadRepository) GetEnvironment(
	ctx context.Context,
	id string,
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	return repository.hierarchy.GetEnvironment(ctx, id)
}

func (repository *durableServiceReadRepository) ListServices(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.ServiceRecord], error) {
	return repository.services.ListServices(ctx, environmentID, request)
}
