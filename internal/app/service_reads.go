package app

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

type serviceReadRepository interface {
	GetEnvironment(context.Context, string) (etcd.Versioned[etcd.EnvironmentRecord], error)
	GetProject(context.Context, string) (etcd.Versioned[etcd.ProjectRecord], error)
	GetTenant(context.Context, string) (etcd.Versioned[etcd.TenantRecord], error)
	GetEnvironmentBlueprintHead(context.Context, string) (etcd.Versioned[etcd.EnvironmentBlueprintHead], bool, error)
	GetEnvironmentBlueprintRevision(context.Context, string, string) (etcd.Versioned[etcd.EnvironmentBlueprintRevision], bool, error)
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
	head, found, err := service.repository.GetEnvironmentBlueprintHead(ctx, environmentID)
	if err != nil || !found {
		return "", err
	}
	revision, found, err := service.repository.GetEnvironmentBlueprintRevision(
		ctx,
		environmentID,
		head.Record.RevisionID,
	)
	if err != nil {
		return "", err
	}
	if !found {
		return "", errs.New(errs.KindInternal, "Service desired revision is missing")
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return "", err
	}
	// Backing desired-state parsing is introduced with the backing facade. Its
	// clean-start records currently have no tenant-scoped Environment envelope.
	if project.Record.Kind == etcd.ProjectKindBacking {
		return "", nil
	}
	tenant, err := service.repository.GetTenant(ctx, project.Record.TenantID)
	if err != nil {
		return "", err
	}
	bundle := serviceBlueprintBundle(revision.Record)
	for index := range bundle.Files {
		defer clear(bundle.Files[index].Content)
	}
	parsed, err := blueprintparser.Parse(ctx, blueprintparser.EnvironmentScope{
		EnvironmentID: environmentID,
		Tenant:        tenant.Record.Slug,
		Project:       project.Record.Slug,
		Environment:   environment.Record.Name,
	}, bundle)
	if err != nil {
		return "", err
	}
	config, exists := parsed.Project.Services[serviceName]
	if !exists {
		config, exists = parsed.Project.DisabledServices[serviceName]
	}
	if !exists {
		return "", errs.New(errs.KindInternal, "Service is absent from its current desired revision")
	}
	document, err := (&composetypes.Project{
		Services: composetypes.Services{serviceName: config},
	}).MarshalYAML()
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	result := string(document)
	clear(document)
	return result, nil
}

func serviceBlueprintBundle(revision etcd.EnvironmentBlueprintRevision) core.BlueprintBundle {
	files := make([]core.BlueprintFile, len(revision.Files))
	for index, file := range revision.Files {
		files[index] = core.BlueprintFile{
			Path:    file.Path,
			Content: append([]byte(nil), file.Content...),
		}
	}
	interpolation := make(map[string]string, len(revision.Interpolation))
	for key, value := range revision.Interpolation {
		interpolation[key] = value
	}
	return core.BlueprintBundle{
		RootPath:       revision.RootPath,
		ComposeSources: append([]string(nil), revision.ComposeSources...),
		Interpolation:  interpolation,
		Files:          files,
	}
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

func (repository *durableServiceReadRepository) GetProject(
	ctx context.Context,
	id string,
) (etcd.Versioned[etcd.ProjectRecord], error) {
	return repository.hierarchy.GetProject(ctx, id)
}

func (repository *durableServiceReadRepository) GetTenant(
	ctx context.Context,
	id string,
) (etcd.Versioned[etcd.TenantRecord], error) {
	return repository.hierarchy.GetTenant(ctx, id)
}

func (repository *durableServiceReadRepository) GetEnvironmentBlueprintHead(
	ctx context.Context,
	environmentID string,
) (etcd.Versioned[etcd.EnvironmentBlueprintHead], bool, error) {
	return repository.hierarchy.GetEnvironmentBlueprintHead(ctx, environmentID)
}

func (repository *durableServiceReadRepository) GetEnvironmentBlueprintRevision(
	ctx context.Context,
	environmentID string,
	revisionID string,
) (etcd.Versioned[etcd.EnvironmentBlueprintRevision], bool, error) {
	return repository.hierarchy.GetEnvironmentBlueprintRevision(ctx, environmentID, revisionID)
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
