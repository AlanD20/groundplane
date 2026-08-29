package app

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type serviceReadRepository interface {
	GetEnvironment(context.Context, string) (etcd.Versioned[etcd.EnvironmentRecord], error)
	GetEnvironmentComposeProjection(context.Context, string) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error)
	GetService(context.Context, string) (etcd.Versioned[etcd.ServiceRecord], error)
	ListServices(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.ServiceRecord], error)
}

type serviceReadService struct {
	repository serviceReadRepository
}

func (service *serviceReadService) GetService(
	ctx context.Context,
	serviceID string,
) (etcd.Versioned[etcd.ServiceRecord], error) {
	if ctx == nil {
		return etcd.Versioned[etcd.ServiceRecord]{}, errs.New(errs.KindInternal, "Service read context is required")
	}
	if ids.Validate(ids.KindService, serviceID) != nil {
		return etcd.Versioned[etcd.ServiceRecord]{}, errs.New(
			errs.KindValidationFailed,
			"Service read requires a stable Service id",
		)
	}
	return service.repository.GetService(ctx, serviceID)
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

func (service *serviceReadService) GetServiceNativeCompose(
	ctx context.Context,
	environmentID string,
	serviceName string,
) (string, error) {
	if ctx == nil {
		return "", errs.New(errs.KindInternal, "Service detail context is required")
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || strings.TrimSpace(serviceName) == "" {
		return "", errs.New(errs.KindValidationFailed, "Service detail identity is invalid")
	}
	environment, err := service.repository.GetEnvironment(ctx, environmentID)
	if err != nil {
		return "", err
	}
	projection, found, err := service.repository.GetEnvironmentComposeProjection(ctx, environmentID)
	if err != nil {
		return "", err
	}
	if !found {
		return "", errs.New(errs.KindInternal, "Service desired Compose projection is missing")
	}
	if projection.Record.EnvironmentID != environment.Record.ID {
		return "", errs.New(errs.KindInternal, "Service desired Compose projection owner is invalid")
	}
	artifact := &agentpb.ComposeArtifact{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(
		projection.Record.ComposeArtifact,
		artifact,
	); err != nil {
		return "", errs.New(errs.KindInternal, "Service desired Compose artifact is corrupt")
	}
	return controller.ProjectEnvironmentServiceNativeCompose(artifact, serviceName)
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

func (repository *durableServiceReadRepository) GetEnvironmentComposeProjection(
	ctx context.Context,
	environmentID string,
) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error) {
	return repository.hierarchy.GetEnvironmentComposeProjection(ctx, environmentID)
}

func (repository *durableServiceReadRepository) ListServices(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.ServiceRecord], error) {
	return repository.services.ListServices(ctx, environmentID, request)
}

func (repository *durableServiceReadRepository) GetService(
	ctx context.Context,
	id string,
) (etcd.Versioned[etcd.ServiceRecord], error) {
	return repository.services.GetService(ctx, id)
}
