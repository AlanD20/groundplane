package blueprint

import (
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func prepareEnvironmentBlueprintZoneChanges(
	environmentID string,
	desired []core.Zone,
	current []etcd.Versioned[etcd.ZoneRecord],
) ([]etcd.EnvironmentBlueprintZoneChange, error) {
	currentByID := make(map[string]etcd.Versioned[etcd.ZoneRecord], len(current))
	for _, zone := range current {
		if zone.Record.EnvironmentID != environmentID || zone.Record.Desired.ID == "" {
			return nil, errs.New(errs.KindInternal, "durable Blueprint Zone state is inconsistent")
		}
		if _, duplicate := currentByID[zone.Record.Desired.ID]; duplicate {
			return nil, errs.New(errs.KindInternal, "durable Blueprint Zone state repeats an id")
		}
		currentByID[zone.Record.Desired.ID] = zone
	}
	changes := make([]etcd.EnvironmentBlueprintZoneChange, 0, len(desired))
	for _, next := range desired {
		if existing, found := currentByID[next.ID]; found {
			if existing.Record.Desired != next {
				return nil, errs.New(
					errs.KindValidationFailed,
					"Blueprint changed an immutable Zone; add a new Zone and move Services explicitly",
				)
			}
			currentCopy := existing
			changes = append(changes, etcd.EnvironmentBlueprintZoneChange{
				Current: &currentCopy,
				Record:  existing.Record,
			})
			delete(currentByID, next.ID)
			continue
		}
		record, err := etcd.NewZoneRecord(environmentID, next)
		if err != nil {
			return nil, err
		}
		changes = append(changes, etcd.EnvironmentBlueprintZoneChange{Record: record})
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
	zones []etcd.EnvironmentBlueprintZoneChange,
	services []etcd.EnvironmentBlueprintServiceChange,
	routes []etcd.EnvironmentBlueprintRouteChange,
) ([]etcd.EnvironmentZoneProjection, []etcd.EnvironmentServiceProjection, []etcd.EnvironmentRouteProjection) {
	zoneProjection := make([]etcd.EnvironmentZoneProjection, len(zones))
	for index, change := range zones {
		zoneProjection[index] = etcd.EnvironmentZoneProjection{
			EnvironmentID: change.Record.EnvironmentID, Desired: change.Record.Desired,
		}
	}
	serviceProjection := make([]etcd.EnvironmentServiceProjection, len(services))
	for index, change := range services {
		serviceProjection[index] = etcd.EnvironmentServiceProjection{
			EnvironmentID:    change.Record.EnvironmentID,
			BackingNetworkID: change.Record.BackingNetworkID,
			Desired:          change.Record.Desired,
		}
	}
	routeProjection := make([]etcd.EnvironmentRouteProjection, len(routes))
	for index, change := range routes {
		routeProjection[index] = etcd.EnvironmentRouteProjection{
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
	current []etcd.Versioned[etcd.RouteRecord],
) ([]etcd.EnvironmentBlueprintRouteChange, error) {
	currentByID := make(map[string]etcd.Versioned[etcd.RouteRecord], len(current))
	for _, route := range current {
		if route.Record.EnvironmentID != environmentID || route.Record.Desired.ID == "" {
			return nil, errs.New(errs.KindInternal, "durable Blueprint Route state is inconsistent")
		}
		if _, duplicate := currentByID[route.Record.Desired.ID]; duplicate {
			return nil, errs.New(errs.KindInternal, "durable Blueprint Route state repeats an id")
		}
		currentByID[route.Record.Desired.ID] = route
	}
	changes := make([]etcd.EnvironmentBlueprintRouteChange, 0, len(desired))
	for _, next := range desired {
		if existing, found := currentByID[next.ID]; found {
			replacement, err := etcd.ReplaceRouteDesired(existing.Record, next)
			if err != nil {
				return nil, err
			}
			currentCopy := existing
			changes = append(changes, etcd.EnvironmentBlueprintRouteChange{
				Current: &currentCopy,
				Record:  replacement,
			})
			delete(currentByID, next.ID)
			continue
		}
		record, err := etcd.NewRouteRecord(environmentID, next)
		if err != nil {
			return nil, err
		}
		changes = append(changes, etcd.EnvironmentBlueprintRouteChange{Record: record})
	}
	if len(currentByID) != 0 {
		return nil, errs.New(
			errs.KindResourceInUse,
			"Blueprint omits an existing Route; remove it explicitly before apply",
		)
	}
	return changes, nil
}
