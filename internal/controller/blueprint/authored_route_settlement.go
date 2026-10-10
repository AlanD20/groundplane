package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type authoredRouteSettlementRepository interface {
	SettleBlueprintRoute(context.Context, etcd.TaskRecord, blueprintunits.Unit, projectionrecord.EnvironmentRouteProjection) error
	SettleBlueprintRouteOmission(context.Context, etcd.TaskRecord, blueprintunits.Unit) error
}

func (service *Service) SettleRouteOmission(ctx context.Context, parent etcd.TaskRecord, unit blueprintunits.Unit) error {
	if service == nil || ctx == nil {
		return errs.New(errs.KindInternal, "Blueprint Route settler is not configured")
	}
	settler, ok := service.repository.(authoredRouteSettlementRepository)
	if !ok {
		return errs.New(errs.KindInternal, "Blueprint Route settlement repository is not configured")
	}
	return settler.SettleBlueprintRouteOmission(ctx, parent, unit)
}

func (service *Service) SettleRoute(
	ctx context.Context, parent etcd.TaskRecord, unit blueprintunits.Unit,
) error {
	if service == nil || ctx == nil {
		return errs.New(errs.KindInternal, "Blueprint Route settler is not configured")
	}
	settler, ok := service.repository.(authoredRouteSettlementRepository)
	if !ok {
		return errs.New(errs.KindInternal, "Blueprint Route settlement repository is not configured")
	}
	source, err := service.loadAuthoredRuntimeProjectionSource(ctx, parent)
	if err != nil {
		return err
	}
	project, err := composerender.LoadNormalizedEnvironmentDesiredProject(ctx, source.desired.Record, source.identities.Record)
	if err != nil {
		return err
	}
	identities, err := authoredOwnedIdentitySnapshot(ctx, source.desired.Record, source.identities.Record)
	if err != nil {
		return err
	}
	services, err := taskplanning.ProjectServiceProjection(project, identities, source.desired.Record.Input.ServiceExtensions)
	if err != nil {
		return err
	}
	routes, err := projectAuthoredRuntimeRoutes(source.desired.Record, source.identities.Record, services)
	if err != nil {
		return err
	}
	for _, route := range routes {
		if route.Desired.ID == unit.Target.ID {
			return settler.SettleBlueprintRoute(ctx, parent, unit, route)
		}
	}
	return errs.New(errs.KindStateConflict, "Blueprint Route is absent from the sealed input")
}
