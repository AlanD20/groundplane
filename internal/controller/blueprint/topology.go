package blueprint

import (
	"github.com/AlanD20/groundplane/internal/core"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"

	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func prepareEnvironmentBlueprintZoneChanges(
	environmentID string,
	desired []core.Zone,
	current []etcdstore.Versioned[zonerecord.Record],
) ([]blueprints.EnvironmentBlueprintZoneChange, error) {
	currentByID := make(map[string]etcdstore.Versioned[zonerecord.Record], len(current))
	for _, zone := range current {
		if zone.Record.EnvironmentID != environmentID || zone.Record.Desired.ID == "" {
			return nil, errs.New(errs.KindInternal, "durable Blueprint Zone state is inconsistent")
		}
		if _, duplicate := currentByID[zone.Record.Desired.ID]; duplicate {
			return nil, errs.New(errs.KindInternal, "durable Blueprint Zone state repeats an id")
		}
		currentByID[zone.Record.Desired.ID] = zone
	}
	changes := make([]blueprints.EnvironmentBlueprintZoneChange, 0, len(desired))
	for _, next := range desired {
		if existing, found := currentByID[next.ID]; found {
			if existing.Record.Desired != next {
				return nil, errs.New(
					errs.KindValidationFailed,
					"Blueprint changed an immutable Zone; add a new Zone and move Services explicitly",
				)
			}
			currentCopy := existing
			changes = append(changes, blueprints.EnvironmentBlueprintZoneChange{
				Current: &currentCopy,
				Record:  existing.Record,
			})
			delete(currentByID, next.ID)
			continue
		}
		record, err := zonerecord.NewRecord(environmentID, next)
		if err != nil {
			return nil, err
		}
		changes = append(changes, blueprints.EnvironmentBlueprintZoneChange{Record: record})
	}
	if len(currentByID) != 0 {
		return nil, errs.New(
			errs.KindResourceInUse,
			"Blueprint omits an existing Zone; remove it explicitly after moving Services",
		)
	}
	return changes, nil
}

func environmentBlueprintTopologyProjection(
	zones []blueprints.EnvironmentBlueprintZoneChange,
	services []blueprints.EnvironmentBlueprintServiceChange,
	routes []blueprints.EnvironmentBlueprintRouteChange,
) ([]projectionrecord.EnvironmentZoneProjection, []servicerecord.EnvironmentServiceProjection, []projectionrecord.EnvironmentRouteProjection) {
	zoneProjection := make([]projectionrecord.EnvironmentZoneProjection, len(zones))
	for index, change := range zones {
		zoneProjection[index] = projectionrecord.EnvironmentZoneProjection{
			EnvironmentID: change.Record.EnvironmentID, Desired: change.Record.Desired,
		}
	}
	serviceProjection := make([]servicerecord.EnvironmentServiceProjection, len(services))
	for index, change := range services {
		serviceProjection[index] = servicerecord.EnvironmentServiceProjection{
			EnvironmentID:    change.Record.EnvironmentID,
			BackingNetworkID: change.Record.BackingNetworkID,
			Desired:          change.Record.Desired,
		}
	}
	routeProjection := make([]projectionrecord.EnvironmentRouteProjection, len(routes))
	for index, change := range routes {
		routeProjection[index] = projectionrecord.EnvironmentRouteProjection{
			EnvironmentID:     change.Record.EnvironmentID,
			Desired:           change.Record.Desired,
			DesiredGeneration: change.Record.DesiredGeneration,
		}
	}
	return zoneProjection, serviceProjection, routeProjection
}

func prepareEnvironmentBlueprintRouteChanges(
	environmentID string,
	desired []core.Route,
	current []etcdstore.Versioned[routerecord.Record],
) ([]blueprints.EnvironmentBlueprintRouteChange, error) {
	currentByID := make(map[string]etcdstore.Versioned[routerecord.Record], len(current))
	for _, route := range current {
		if route.Record.EnvironmentID != environmentID || route.Record.Desired.ID == "" {
			return nil, errs.New(errs.KindInternal, "durable Blueprint Route state is inconsistent")
		}
		if _, duplicate := currentByID[route.Record.Desired.ID]; duplicate {
			return nil, errs.New(errs.KindInternal, "durable Blueprint Route state repeats an id")
		}
		currentByID[route.Record.Desired.ID] = route
	}
	changes := make([]blueprints.EnvironmentBlueprintRouteChange, 0, len(desired))
	for _, next := range desired {
		if existing, found := currentByID[next.ID]; found {
			replacement, err := routerecord.ReplaceDesired(existing.Record, next)
			if err != nil {
				return nil, err
			}
			currentCopy := existing
			changes = append(changes, blueprints.EnvironmentBlueprintRouteChange{
				Current: &currentCopy,
				Record:  replacement,
			})
			delete(currentByID, next.ID)
			continue
		}
		record, err := routerecord.NewRecord(environmentID, next)
		if err != nil {
			return nil, err
		}
		changes = append(changes, blueprints.EnvironmentBlueprintRouteChange{Record: record})
	}
	if len(currentByID) != 0 {
		return nil, errs.New(
			errs.KindResourceInUse,
			"Blueprint omits an existing Route; remove it explicitly before apply",
		)
	}
	return changes, nil
}
