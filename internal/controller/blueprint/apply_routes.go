package blueprint

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) prepareApplyRoutes(ctx context.Context, environmentID string, parsed *blueprintparser.Result, desiredServices []core.Service, preserveRoutes bool, allocate func(ids.Kind) string) (controller.BlueprintRouteChanges, []etcd.EnvironmentBlueprintRouteChange, error) {
	currentRoutes, err := service.listBlueprintRoutes(ctx, environmentID)
	if err != nil {
		return controller.BlueprintRouteChanges{}, nil, err
	}
	if !preserveRoutes {
		parsed.Extensions.Routes, err = preserveEnvironmentBlueprintRoutes(
			parsed.Extensions.Routes, desiredServices, currentRoutes,
		)
		if err != nil {
			return controller.BlueprintRouteChanges{}, nil, err
		}
	}
	previousRoutes := make([]controller.RouteIdentity, len(currentRoutes))
	for index, route := range currentRoutes {
		previousRoutes[index] = controller.RouteIdentity{
			ID: route.Record.Desired.ID, Host: route.Record.Desired.Host, Path: route.Record.Desired.Path,
		}
	}
	reconciledRoutes := controller.BlueprintRouteChanges{}
	if preserveRoutes {
		reconciledRoutes.Current = make([]core.Route, len(currentRoutes))
		for index, route := range currentRoutes {
			reconciledRoutes.Current[index] = route.Record.Desired
		}
	} else {
		reconciledRoutes, err = controller.ReconcileBlueprintRoutes(
			parsed.Extensions.Routes,
			desiredServices,
			previousRoutes,
			allocate,
		)
		if err != nil {
			return controller.BlueprintRouteChanges{}, nil, err
		}
	}
	if len(reconciledRoutes.RemovedRouteIDs) != 0 {
		return controller.BlueprintRouteChanges{}, nil, errs.New(
			errs.KindResourceInUse,
			"Blueprint omits an existing Route; remove it explicitly before apply",
		)
	}
	routeChanges, err := prepareEnvironmentBlueprintRouteChanges(
		environmentID, reconciledRoutes.Current, currentRoutes,
	)
	if err != nil {
		return controller.BlueprintRouteChanges{}, nil, err
	}

	return reconciledRoutes, routeChanges, nil
}
