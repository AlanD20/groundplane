package etcd

import (
	"context"
	"reflect"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ComponentTaskRouteCandidate pins one desired Route generation whose
// observation may change only when the owning Component Task succeeds.
type ComponentTaskRouteCandidate struct {
	Desired           core.Route `json:"desired"`
	DesiredGeneration uint64     `json:"desired_generation"`
}

// ComponentTaskRouteProjection is the generic terminal projection contributed
// by a Component that provides the HTTP router capability. A nil Provider means
// successful removal of that capability projects every retained Route unserved.
type ComponentTaskRouteProjection struct {
	Provider *RouteProviderPin             `json:"provider,omitempty"`
	Routes   []ComponentTaskRouteCandidate `json:"routes"`
}

// WithComponentTaskRouteProjection attaches immutable Route observation input
// to an otherwise complete Component Task preparation without publishing it.
func WithComponentTaskRouteProjection(
	preparation ComponentTaskPreparation,
	routes []RouteRecord,
	provider *RouteProviderPin,
) (ComponentTaskPreparation, error) {
	if componentTaskPreparationIsZero(preparation) {
		return ComponentTaskPreparation{}, errs.New(errs.KindInternal, "Component Route projection requires a Task")
	}
	projection := &ComponentTaskRouteProjection{
		Routes: make([]ComponentTaskRouteCandidate, len(routes)),
	}
	if provider != nil {
		cloned := cloneRouteProviderPin(*provider)
		projection.Provider = &cloned
	}
	for index, route := range routes {
		if validateRouteRecord(route) != nil ||
			route.EnvironmentID != preparation.Intent.EnvironmentID {
			return ComponentTaskPreparation{}, errs.New(errs.KindInternal, "Component Route projection input is invalid")
		}
		projection.Routes[index] = ComponentTaskRouteCandidate{
			Desired: route.Desired, DesiredGeneration: route.DesiredGeneration,
		}
	}
	sort.Slice(projection.Routes, func(left, right int) bool {
		return projection.Routes[left].Desired.ID < projection.Routes[right].Desired.ID
	})
	result := cloneComponentTaskPreparation(preparation)
	result.Intent.RouteProjection = projection
	if err := validateComponentTaskPreparation(result); err != nil {
		return ComponentTaskPreparation{}, err
	}
	return cloneComponentTaskPreparation(result), nil
}

func validateComponentTaskRouteProjection(intent ComponentTaskIntent) error {
	projection := intent.RouteProjection
	if projection == nil {
		return nil
	}
	if projection.Provider != nil {
		if err := validateRouteProviderPin(projection.Provider); err != nil {
			return err
		}
		matched := false
		for _, candidate := range intent.Candidates {
			matched = matched || candidate.Candidate.Desired.ID == projection.Provider.ComponentID &&
				candidate.Candidate.Desired.Enabled
		}
		if !matched {
			return errs.New(errs.KindValidationFailed, "Component Route provider is not a candidate")
		}
	}
	previousID := ""
	for _, route := range projection.Routes {
		if route.Desired.ID <= previousID || route.DesiredGeneration == 0 || route.Desired.Validate() != nil {
			return errs.New(errs.KindValidationFailed, "Component Route projection is invalid")
		}
		previousID = route.Desired.ID
	}
	return nil
}

func cloneComponentTaskRouteProjection(source *ComponentTaskRouteProjection) *ComponentTaskRouteProjection {
	if source == nil {
		return nil
	}
	clone := &ComponentTaskRouteProjection{
		Routes: append([]ComponentTaskRouteCandidate(nil), source.Routes...),
	}
	if source.Provider != nil {
		provider := cloneRouteProviderPin(*source.Provider)
		clone.Provider = &provider
	}
	return clone
}

type componentTaskRouteObservationChange struct {
	conditions []Condition
	mutations  []Mutation
	values     [][]byte
}

func (repository *TaskRepository) prepareComponentTaskRouteObservationAcknowledgement(
	ctx context.Context,
	intent ComponentTaskIntent,
	terminalStatus TaskStatus,
	revision int64,
) (componentTaskRouteObservationChange, error) {
	projection := intent.RouteProjection
	if projection == nil || len(projection.Routes) == 0 ||
		projection.Provider == nil && terminalStatus != TaskStatusCompleted {
		return componentTaskRouteObservationChange{}, nil
	}
	status := RouteObservedUnserved
	if projection.Provider != nil {
		status = RouteObservedDegraded
		if terminalStatus == TaskStatusCompleted {
			status = RouteObservedServed
		}
	}
	return repository.prepareComponentTaskRouteObservationStatus(ctx, intent, status, revision)
}

func (repository *TaskRepository) prepareComponentTaskRouteObservationRetry(
	ctx context.Context,
	intent ComponentTaskIntent,
	revision int64,
) (componentTaskRouteObservationChange, error) {
	if intent.RouteProjection == nil || intent.RouteProjection.Provider == nil ||
		len(intent.RouteProjection.Routes) == 0 {
		return componentTaskRouteObservationChange{}, nil
	}
	return repository.prepareComponentTaskRouteObservationStatus(
		ctx, intent, RouteObservedPending, revision,
	)
}

func (repository *TaskRepository) prepareComponentTaskRouteObservationStatus(
	ctx context.Context,
	intent ComponentTaskIntent,
	status RouteObservedStatus,
	revision int64,
) (componentTaskRouteObservationChange, error) {
	projection := intent.RouteProjection
	keys := make([]string, len(projection.Routes))
	for index, route := range projection.Routes {
		keys[index] = routeKey(route.Desired.ID)
	}
	state, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return componentTaskRouteObservationChange{}, err
	}
	if state == nil || len(state.Values) != len(keys) {
		return componentTaskRouteObservationChange{}, errs.New(errs.KindInternal, "Component Route terminal read is incomplete")
	}
	change := componentTaskRouteObservationChange{}
	for index, candidate := range projection.Routes {
		value := state.Values[index]
		if value == nil {
			return componentTaskRouteObservationChange{}, errs.New(errs.KindStateConflict, "Component Route was removed during reconciliation")
		}
		record, decodeErr := decodeRouteRecord(value.Value)
		if decodeErr != nil {
			return componentTaskRouteObservationChange{}, decodeErr
		}
		if record.EnvironmentID != intent.EnvironmentID || record.DesiredGeneration != candidate.DesiredGeneration ||
			!reflect.DeepEqual(record.Desired, candidate.Desired) {
			return componentTaskRouteObservationChange{}, errs.New(errs.KindStateConflict, "Component Route desired state changed during reconciliation")
		}
		var provider RouteProviderObservation
		if projection.Provider != nil {
			provider = RouteProviderObservation{
				ComponentID:      projection.Provider.ComponentID,
				DefinitionDigest: projection.Provider.DefinitionDigest,
				CatalogDigest:    projection.Provider.CatalogDigest,
				InputRevision:    projection.Provider.InputRevision,
				InputGeneration:  projection.Provider.InputGeneration,
			}
		}
		replacement, replaceErr := SetRouteObservation(record, RouteObservation{
			Status: status, DesiredGeneration: record.DesiredGeneration, Provider: provider,
		})
		if replaceErr != nil {
			return componentTaskRouteObservationChange{}, replaceErr
		}
		encoded, encodeErr := encodeRouteRecord(replacement)
		if encodeErr != nil {
			return componentTaskRouteObservationChange{}, encodeErr
		}
		change.conditions = append(change.conditions, Condition{Key: keys[index], ModRevision: value.ModRevision})
		change.values = append(change.values, encoded)
		change.mutations = append(change.mutations, Mutation{Type: MutationPut, Key: keys[index], Value: encoded})
	}
	return change, nil
}

func validateComponentTaskRouteIdentity(route ComponentTaskRouteCandidate) error {
	if ids.Validate(ids.KindRoute, route.Desired.ID) != nil {
		return errs.New(errs.KindValidationFailed, "Component Route identity is invalid")
	}
	return nil
}
