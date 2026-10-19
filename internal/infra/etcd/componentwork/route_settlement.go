package componentwork

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/routepersistence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// RouteSettlement binds a direct Route action to the Component operation that
// changes its router. It is immutable input, not a second execution journal.
type RouteSettlement struct {
	Record   routes.Record              `json:"record"`
	Remove   bool                       `json:"remove"`
	Provider routes.ProviderObservation `json:"provider"`
}

func (route RouteSettlement) matches(view workqueue.View, input *agentpb.ComponentWorkInput) error {
	task, item := view.Task.Task, view.Item.Item
	if routes.ValidateRecord(route.Record) != nil || routes.ValidateProviderObservation(&route.Provider) != nil ||
		task.TargetID != route.Record.Desired.ID || task.Scope.EnvironmentID != route.Record.EnvironmentID ||
		input == nil || input.Remove || input.ConfigurationAction == nil ||
		route.Provider.ComponentID != item.TargetID || route.Provider.ComponentID != input.ComponentId ||
		route.Provider.InputGeneration != input.RenderGeneration || route.Provider.InputRevision > view.ReadRevision ||
		hex.EncodeToString(input.ConfigurationAction.DefinitionDigest) != route.Provider.DefinitionDigest ||
		hex.EncodeToString(input.ConfigurationAction.CatalogDigest) != route.Provider.CatalogDigest ||
		(route.Remove && task.Action != workexecution.ActionRemove) ||
		(!route.Remove && task.Action != workexecution.ActionCreate && task.Action != workexecution.ActionUpdate) {
		return conflict("Route settlement differs from its captured router operation")
	}
	claimed := false
	for _, claim := range item.Claims {
		claimed = claimed || claim.HostID == "" && claim.ScopeID == route.Record.Desired.ID
	}
	if !claimed {
		return conflict("Route operation does not hold its resource claim")
	}
	return nil
}

// routeConditions protects the exact desired generation. Diagnostic or runtime
// observations never authorize settling a different Route definition.
func (repository *InputRepository) routeConditions(ctx context.Context, view workqueue.View,
	route *RouteSettlement,
) ([]keyvalue.Condition, error) {
	current, conditions, err := repository.readRouteMatch(ctx, view, route)
	if err != nil {
		return nil, err
	}
	if !current {
		return nil, conflict("Route definition changed after its router input was captured")
	}
	return conditions, nil
}

func (repository *InputRepository) readRouteMatch(ctx context.Context, view workqueue.View,
	route *RouteSettlement,
) (bool, []keyvalue.Condition, error) {
	if route == nil {
		return true, nil, nil
	}
	key := routes.RecordKey(route.Record.Desired.ID)
	read, err := repository.read(ctx, []string{key}, view.ReadRevision)
	if err != nil {
		return false, nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	conditions := []keyvalue.Condition{{Key: key, ModRevision: keyvalue.RevisionOf(value)}}
	if value == nil {
		return false, conditions, nil
	}
	current, err := routes.DecodeRecord(value.Value)
	if err != nil {
		return false, nil, err
	}
	if current.EnvironmentID != route.Record.EnvironmentID || current.Desired.ID != route.Record.Desired.ID {
		return false, nil, conflict("Route definition belongs to another resource")
	}
	return current == route.Record, conditions, nil
}

// settleRoute is called only alongside final Component host proof and queue
// completion. A failed or recovered operation must never retire the Route.
func (repository *InputRepository) settleRoute(ctx context.Context, view workqueue.View,
	route *RouteSettlement,
) (workqueue.Change, error) {
	current, conditions, err := repository.readRouteMatch(ctx, view, route)
	if err != nil || route == nil {
		return workqueue.Change{}, err
	}
	change := workqueue.Change{Conditions: conditions}
	// Accept proof of an already-authorized router effect without overwriting
	// a newer Route generation. The router runtime is settled independently.
	if !current {
		return change, nil
	}
	if route.Remove {
		guards, writes, err := routepersistence.PrepareRetirement(ctx, repository.store,
			route.Record.EnvironmentID, route.Record.Desired.ID, view.ReadRevision)
		if err != nil {
			return workqueue.Change{}, err
		}
		change.Conditions = append(change.Conditions, guards...)
		change.Mutations = writes
		return change, nil
	}
	return repository.observeRoute(ctx, view, route, change, routes.ObservedServed)
}

// failRoute records failure without retiring the definition or claiming that
// recovery restored service. Failure evidence and protected effects remain with
// the work item, independently of this operator-facing observation.
func (repository *InputRepository) failRoute(ctx context.Context, view workqueue.View,
	route *RouteSettlement,
) (workqueue.Change, error) {
	current, conditions, err := repository.readRouteMatch(ctx, view, route)
	if err != nil || route == nil {
		return workqueue.Change{}, err
	}
	change := workqueue.Change{Conditions: conditions}
	if !current {
		return change, nil
	}
	return repository.observeRoute(ctx, view, route, change, routes.ObservedDegraded)
}

func (repository *InputRepository) observeRoute(ctx context.Context, view workqueue.View,
	route *RouteSettlement, change workqueue.Change, status routes.ObservedStatus,
) (workqueue.Change, error) {
	key := routes.ObservationKey(route.Record.Desired.ID)
	read, err := repository.read(ctx, []string{key}, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] != nil {
		previous, err := routes.DecodeObservation(read.Values[0].Value)
		if err != nil || previous.EnvironmentID != route.Record.EnvironmentID ||
			previous.RouteID != route.Record.Desired.ID || previous.DesiredGeneration > route.Record.DesiredGeneration {
			return workqueue.Change{}, conflict("Route runtime observation belongs to another generation")
		}
	}
	observation, err := routes.NewObservationRecord(route.Record.EnvironmentID, route.Record.Desired.ID,
		route.Record.DesiredGeneration, routes.Observation{Status: status,
			DesiredGeneration: route.Record.DesiredGeneration, Provider: route.Provider})
	if err != nil {
		return workqueue.Change{}, err
	}
	encoded, err := routes.EncodeObservation(observation)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(
		change.Conditions,
		keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[0])},
	)
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: encoded})
	return change, nil
}
