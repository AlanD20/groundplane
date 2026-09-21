package etcd

import (
	"context"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	routepersistence "github.com/AlanD20/groundplane/internal/infra/etcd/routepersistence"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type preparedEnvironmentBlueprintRoute struct {
	change        blueprints.EnvironmentBlueprintRouteChange
	value         []byte
	ownerRevision int64
	matchRevision int64
}

func (repository *HierarchyRepository) prepareEnvironmentBlueprintRouteChanges(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	services []blueprints.EnvironmentBlueprintServiceChange,
	changes []blueprints.EnvironmentBlueprintRouteChange,
) ([]preparedEnvironmentBlueprintRoute, error) {
	return repository.prepareEnvironmentBlueprintRouteChangesAtRevision(
		ctx,
		environment,
		services,
		changes,
		0,
	)
}

func (repository *HierarchyRepository) prepareEnvironmentBlueprintRouteChangesAtRevision(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	services []blueprints.EnvironmentBlueprintServiceChange,
	changes []blueprints.EnvironmentBlueprintRouteChange,
	readRevision int64,
) ([]preparedEnvironmentBlueprintRoute, error) {
	targets := make(map[string]struct{}, len(services))
	for _, service := range services {
		targets[service.Record.Desired.ID] = struct{}{}
	}
	seenIDs := make(map[string]struct{}, len(changes))
	seenMatches := make(map[string]struct{}, len(changes))
	prepared := make([]preparedEnvironmentBlueprintRoute, 0, len(changes))
	for _, change := range changes {
		if err := routerecord.ValidateRecord(change.Record); err != nil {
			clearPreparedEnvironmentBlueprintRoutes(prepared)
			return nil, err
		}
		routeID := change.Record.Desired.ID
		matchKey := routerecord.MatchKey(
			change.Record.EnvironmentID,
			change.Record.Desired.Host,
			change.Record.Desired.Path,
		)
		_, targetExists := targets[change.Record.Desired.TargetServiceID]
		_, duplicateID := seenIDs[routeID]
		_, duplicateMatch := seenMatches[matchKey]
		if change.Record.EnvironmentID != environment.Record.ID || !targetExists || duplicateID ||
			duplicateMatch {
			clearPreparedEnvironmentBlueprintRoutes(prepared)
			return nil, errs.New(
				errs.KindValidationFailed,
				"Blueprint Route change does not match its Environment Service projection",
			)
		}
		seenIDs[routeID] = struct{}{}
		seenMatches[matchKey] = struct{}{}
		item := preparedEnvironmentBlueprintRoute{change: change}
		if change.Current != nil {
			if err := routepersistence.ValidateRouteVersion(*change.Current); err != nil {
				clearPreparedEnvironmentBlueprintRoutes(prepared)
				return nil, err
			}
			replacement, err := routerecord.ReplaceDesired(change.Current.Record, change.Record.Desired)
			if err != nil || replacement != change.Record {
				clearPreparedEnvironmentBlueprintRoutes(prepared)
				return nil, errs.New(
					errs.KindValidationFailed,
					"Blueprint Route replacement changed immutable identity, match, or target",
				)
			}
		}
		prepared = append(prepared, item)
	}
	return prepared, nil
}

func clearPreparedEnvironmentBlueprintRoutes(routes []preparedEnvironmentBlueprintRoute) {
	for index := range routes {
		clear(routes[index].value)
	}
}
