package blueprint

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type authoredRouteSettlementRepository interface {
	PrepareBlueprintRouteSettlement(context.Context, workqueue.View, blueprintunits.Unit,
		projectionrecord.EnvironmentRouteProjection, time.Time) (workqueue.Change, error)
	PrepareBlueprintRouteOmission(context.Context, workqueue.View, blueprintunits.Unit, time.Time) (workqueue.Change, error)
}

func (service *Service) prepareRouteSettlement(
	ctx context.Context, work workqueue.View, unit blueprintunits.Unit, at time.Time,
) (workqueue.Change, error) {
	if service == nil || ctx == nil {
		return workqueue.Change{}, errs.New(errs.KindInternal, "Blueprint Route settler is not configured")
	}
	settler, ok := service.repository.(authoredRouteSettlementRepository)
	if !ok {
		return workqueue.Change{}, errs.New(errs.KindInternal, "Blueprint Route settlement repository is not configured")
	}
	if unit.Removal {
		return settler.PrepareBlueprintRouteOmission(ctx, work, unit, at)
	}
	source, err := service.loadAuthoredParentInput(ctx, work.Task.Task)
	if err != nil {
		return workqueue.Change{}, err
	}
	project, err := composerender.LoadNormalizedEnvironmentDesiredProject(
		ctx,
		source.desired,
		source.identities,
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	identities, err := authoredOwnedIdentitySnapshot(ctx, source.desired, source.identities)
	if err != nil {
		return workqueue.Change{}, err
	}
	services, err := taskplanning.ProjectServiceProjection(
		project,
		identities,
		source.desired.Input.ServiceExtensions,
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	routes, err := projectAuthoredRuntimeRoutes(source.desired, source.identities, services)
	if err != nil {
		return workqueue.Change{}, err
	}
	for _, route := range routes {
		if route.Desired.ID == unit.Target.ID {
			return settler.PrepareBlueprintRouteSettlement(ctx, work, unit, route, at)
		}
	}
	return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Route is absent from the sealed input")
}
