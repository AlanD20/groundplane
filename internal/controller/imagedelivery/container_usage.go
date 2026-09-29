package imagedelivery

import (
	"context"
	"errors"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) containerUses(
	ctx context.Context,
	uses []imagefetch.ContainerUse,
	owners map[string]*apiTypes.ImageContainerOwner,
) ([]apiTypes.ImageContainer, error) {
	result := make([]apiTypes.ImageContainer, 0, len(uses))
	for _, use := range uses {
		item := apiTypes.ImageContainer{ID: use.ID, Name: use.Name, State: use.State, Managed: use.Managed}
		if use.Managed && ids.Validate(ids.KindEnvironment, use.EnvironmentID) == nil {
			key := use.EnvironmentID + "/" + use.ServiceID
			owner, found := owners[key]
			if !found {
				var err error
				owner, err = service.containerOwner(ctx, use)
				if err != nil && !missingContainerOwner(err) {
					return nil, err
				}
				owners[key] = owner
			}
			item.Owner = owner
		}
		result = append(result, item)
	}
	return result, nil
}

func (service *Service) containerOwner(
	ctx context.Context,
	use imagefetch.ContainerUse,
) (*apiTypes.ImageContainerOwner, error) {
	reader := hierarchy.NewReader(service.store)
	environment, err := reader.GetEnvironment(ctx, use.EnvironmentID)
	if err != nil {
		return nil, err
	}
	project, err := reader.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return nil, err
	}
	owner := &apiTypes.ImageContainerOwner{
		EnvironmentID: environment.Record.ID, EnvironmentName: environment.Record.Name,
		ProjectID: project.Record.ID, ProjectSlug: project.Record.Slug,
		Backing: project.Record.Kind == hierarchy.ProjectKindBacking,
	}
	if !owner.Backing {
		tenant, err := reader.GetTenant(ctx, project.Record.TenantID)
		if err != nil {
			return nil, err
		}
		owner.TenantSlug = tenant.Record.Slug
	}
	if ids.Validate(ids.KindService, use.ServiceID) == nil {
		projection, found, err := blueprints.ReadCurrentProjection(
			ctx,
			service.store,
			use.EnvironmentID,
			environment.ReadRevision,
		)
		if err != nil {
			return nil, err
		}
		if found && !environmentqueries.IsComponentService(projection.Record.Components, use.ServiceID) {
			for _, workload := range projection.Record.DesiredServices {
				if workload.Desired.ID == use.ServiceID && workload.EnvironmentID == use.EnvironmentID {
					owner.ServiceID, owner.ServiceName = workload.Desired.ID, workload.Desired.Name
					break
				}
			}
		}
	}
	return owner, nil
}

func missingContainerOwner(err error) bool {
	return errors.Is(err, errs.New(errs.KindEnvironmentNotFound, "")) ||
		errors.Is(err, errs.New(errs.KindProjectNotFound, "")) ||
		errors.Is(err, errs.New(errs.KindTenantNotFound, "")) ||
		errors.Is(err, errs.New(errs.KindServiceNotFound, ""))
}
