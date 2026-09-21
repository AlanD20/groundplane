package blueprint

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) prepareApplyRoutes(ctx context.Context, environmentID string, parsed *blueprintparser.Result, desiredServices []core.Service, preserveRoutes bool, allocate func(ids.Kind) string) (taskplanning.BlueprintRouteChanges, []blueprints.EnvironmentBlueprintRouteChange, error) {
	currentRoutes, err := service.listBlueprintRoutes(ctx, environmentID)
	if err != nil {
		return taskplanning.BlueprintRouteChanges{}, nil, err
	}
	if !preserveRoutes {
		parsed.Extensions.Routes, err = preserveEnvironmentBlueprintRoutes(
			parsed.Extensions.Routes, desiredServices, currentRoutes,
		)
		if err != nil {
			return taskplanning.BlueprintRouteChanges{}, nil, err
		}
	}
	previousRoutes := make([]taskplanning.RouteIdentity, len(currentRoutes))
	for index, route := range currentRoutes {
		previousRoutes[index] = taskplanning.RouteIdentity{
			ID: route.Record.Desired.ID, Host: route.Record.Desired.Host, Path: route.Record.Desired.Path,
		}
	}
	reconciledRoutes := taskplanning.BlueprintRouteChanges{}
	if preserveRoutes {
		reconciledRoutes.Current = make([]core.Route, len(currentRoutes))
		for index, route := range currentRoutes {
			reconciledRoutes.Current[index] = route.Record.Desired
		}
	} else {
		reconciledRoutes, err = taskplanning.ReconcileBlueprintRoutes(
			parsed.Extensions.Routes,
			desiredServices,
			previousRoutes,
			allocate,
		)
		if err != nil {
			return taskplanning.BlueprintRouteChanges{}, nil, err
		}
	}
	if len(reconciledRoutes.RemovedRouteIDs) != 0 {
		return taskplanning.BlueprintRouteChanges{}, nil, errs.New(
			errs.KindResourceInUse,
			"Blueprint omits an existing Route; remove it explicitly before apply",
		)
	}
	routeChanges, err := prepareEnvironmentBlueprintRouteChanges(
		environmentID, reconciledRoutes.Current, currentRoutes,
	)
	if err != nil {
		return taskplanning.BlueprintRouteChanges{}, nil, err
	}

	return reconciledRoutes, routeChanges, nil
}
