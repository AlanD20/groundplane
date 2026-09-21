package scripts

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type scriptReadRepository interface {
	GetEnvironment(context.Context, string) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error)
	GetService(context.Context, string) (etcdstore.Versioned[servicerecord.ServiceRecord], error)
	GetScript(context.Context, string) (etcdstore.Versioned[scriptrecord.Record], error)
	ListScripts(context.Context, string, etcdstore.PageRequest) (etcdstore.Page[scriptrecord.Record], error)
}

type scriptReadService struct {
	repository scriptReadRepository
}

func NewReadService(repository scriptReadRepository) (*scriptReadService, error) {
	if repository == nil {
		return nil, errs.New(errs.KindInternal, "Script read repository is not configured")
	}
	return &scriptReadService{repository: repository}, nil
}

func (service *scriptReadService) ListScripts(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[scriptrecord.Record], error) {
	if ctx == nil {
		return etcdstore.Page[scriptrecord.Record]{}, errs.New(errs.KindInternal, "Script list context is required")
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return etcdstore.Page[scriptrecord.Record]{}, errs.New(
			errs.KindValidationFailed,
			"Script list requires a stable Environment id",
		)
	}
	if request.Limit < 0 {
		return etcdstore.Page[scriptrecord.Record]{}, errs.New(
			errs.KindValidationFailed,
			"Script list limit must be a positive integer",
		)
	}
	if _, err := service.repository.GetEnvironment(ctx, environmentID); err != nil {
		return etcdstore.Page[scriptrecord.Record]{}, err
	}
	page, err := service.repository.ListScripts(ctx, environmentID, request)
	if err != nil {
		return etcdstore.Page[scriptrecord.Record]{}, err
	}
	for index := range page.Items {
		if err := service.projectServiceLabel(ctx, &page.Items[index].Record); err != nil {
			return etcdstore.Page[scriptrecord.Record]{}, err
		}
	}
	return page, nil
}

func (service *scriptReadService) GetScript(
	ctx context.Context,
	scriptID string,
) (etcdstore.Versioned[scriptrecord.Record], error) {
	if ctx == nil {
		return etcdstore.Versioned[scriptrecord.Record]{}, errs.New(
			errs.KindInternal,
			"Script read context is required",
		)
	}
	if ids.Validate(ids.KindScript, scriptID) != nil {
		return etcdstore.Versioned[scriptrecord.Record]{}, errs.New(
			errs.KindValidationFailed,
			"Script read requires a stable Script id",
		)
	}
	script, err := service.repository.GetScript(ctx, scriptID)
	if err != nil {
		return etcdstore.Versioned[scriptrecord.Record]{}, err
	}
	if err := service.projectServiceLabel(ctx, &script.Record); err != nil {
		return etcdstore.Versioned[scriptrecord.Record]{}, err
	}
	return script, nil
}

func (service *scriptReadService) projectServiceLabel(ctx context.Context, script *scriptrecord.Record) error {
	target, err := service.repository.GetService(ctx, script.ServiceID)
	if err != nil {
		return err
	}
	if target.Record.EnvironmentID != script.EnvironmentID {
		return errs.New(errs.KindInternal, "Script target Service belongs to another Environment")
	}
	script.Desired.ServiceName = target.Record.Desired.Name
	return nil
}

type durableScriptReadRepository struct {
	hierarchy *etcd.HierarchyRepository
	services  *etcd.ServiceRepository
	scripts   *etcd.ScriptRepository
}

func NewReadRepository(
	hierarchy *etcd.HierarchyRepository,
	services *etcd.ServiceRepository,
	scripts *etcd.ScriptRepository,
) (*durableScriptReadRepository, error) {
	if hierarchy == nil || services == nil || scripts == nil {
		return nil, errs.New(errs.KindInternal, "Script read repositories are not configured")
	}
	return &durableScriptReadRepository{hierarchy: hierarchy, services: services, scripts: scripts}, nil
}

func (repository *durableScriptReadRepository) GetEnvironment(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error) {
	return repository.hierarchy.GetEnvironment(ctx, id)
}

func (repository *durableScriptReadRepository) GetScript(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[scriptrecord.Record], error) {
	return repository.scripts.GetScript(ctx, id)
}

func (repository *durableScriptReadRepository) GetService(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[servicerecord.ServiceRecord], error) {
	return repository.services.GetService(ctx, id)
}

func (repository *durableScriptReadRepository) ListScripts(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[scriptrecord.Record], error) {
	return repository.scripts.ListScripts(ctx, environmentID, request)
}
