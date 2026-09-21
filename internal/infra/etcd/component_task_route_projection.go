package etcd

import (
	"context"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	recordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
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
	routes []routerecord.Record,
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
		if routerecord.ValidateRecord(route) != nil ||
			route.EnvironmentID != preparation.Intent.EnvironmentID {
			return ComponentTaskPreparation{}, errs.New(
				errs.KindInternal,
				"Component Route projection input is invalid",
			)
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
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	values     [][]byte
}

func (repository *TaskRepository) prepareComponentTaskRouteObservationAcknowledgement(
	ctx context.Context,
	intent ComponentTaskIntent,
	terminalStatus taskjournal.TaskStatus,
	revision int64,
) (componentTaskRouteObservationChange, error) {
	projection := intent.RouteProjection
	if projection == nil || len(projection.Routes) == 0 ||
		projection.Provider == nil && terminalStatus != taskjournal.TaskStatusCompleted {
		return componentTaskRouteObservationChange{}, nil
	}
	status := routerecord.ObservedUnserved
	if projection.Provider != nil {
		status = routerecord.ObservedDegraded
		if terminalStatus == taskjournal.TaskStatusCompleted {
			status = routerecord.ObservedServed
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
		ctx, intent, routerecord.ObservedPending, revision,
	)
}

func (repository *TaskRepository) prepareComponentTaskRouteObservationStatus(
	ctx context.Context,
	intent ComponentTaskIntent,
	status routerecord.ObservedStatus,
	revision int64,
) (componentTaskRouteObservationChange, error) {
	projection := intent.RouteProjection
	desiredProjection, found, err := currentEnvironmentProjectionAtRevision(
		ctx, repository.store, intent.EnvironmentID, revision,
	)
	if err != nil {
		return componentTaskRouteObservationChange{}, err
	}
	if !found {
		return componentTaskRouteObservationChange{}, errs.New(
			errs.KindStateConflict,
			"Component Route desired head is missing",
		)
	}
	change := componentTaskRouteObservationChange{}
	for _, candidate := range projection.Routes {
		var desired *projectionrecord.EnvironmentRouteProjection
		for index := range desiredProjection.Record.DesiredRoutes {
			if desiredProjection.Record.DesiredRoutes[index].Desired.ID == candidate.Desired.ID {
				desired = &desiredProjection.Record.DesiredRoutes[index]
				break
			}
		}
		if desired == nil || desired.EnvironmentID != intent.EnvironmentID ||
			desired.DesiredGeneration != candidate.DesiredGeneration ||
			!routeDesiredEqual(desired.Desired, candidate.Desired) {
			return componentTaskRouteObservationChange{}, errs.New(
				errs.KindStateConflict,
				"Component Route desired state changed during reconciliation",
			)
		}
		observation := routerecord.Observation{Status: status, DesiredGeneration: candidate.DesiredGeneration}
		if projection.Provider != nil {
			observation.Provider = routerecord.ProviderObservation{
				ComponentID:      projection.Provider.ComponentID,
				DefinitionDigest: projection.Provider.DefinitionDigest,
				CatalogDigest:    projection.Provider.CatalogDigest,
				InputRevision:    projection.Provider.InputRevision,
				InputGeneration:  projection.Provider.InputGeneration,
			}
		}
		observationRecord, recordErr := routerecord.NewObservationRecord(
			intent.EnvironmentID, candidate.Desired.ID, candidate.DesiredGeneration, observation,
		)
		if recordErr != nil {
			return componentTaskRouteObservationChange{}, recordErr
		}
		encoded, encodeErr := routerecord.EncodeObservation(observationRecord)
		if encodeErr != nil {
			return componentTaskRouteObservationChange{}, encodeErr
		}
		key := routerecord.ObservationKey(candidate.Desired.ID)
		read, readErr := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision})
		if readErr != nil {
			clear(encoded)
			return componentTaskRouteObservationChange{}, readErr
		}
		if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
			clear(encoded)
			return componentTaskRouteObservationChange{}, errs.New(
				errs.KindInternal,
				"Component Route observation read is incomplete",
			)
		}
		condition := etcdstore.Condition{Key: key}
		if read.Values[0] != nil {
			prior, decodeErr := routerecord.DecodeObservation(read.Values[0].Value)
			if decodeErr != nil || prior.EnvironmentID != intent.EnvironmentID ||
				prior.RouteID != candidate.Desired.ID {
				clear(encoded)
				return componentTaskRouteObservationChange{}, recordcodec.CorruptRecord()
			}
			condition.ModRevision = read.Values[0].ModRevision
		}
		change.conditions = append(change.conditions, condition)
		change.values = append(change.values, encoded)
		change.mutations = append(change.mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: key, Value: encoded})
	}
	return change, nil
}

func validateComponentTaskRouteIdentity(route ComponentTaskRouteCandidate) error {
	if ids.Validate(ids.KindRoute, route.Desired.ID) != nil {
		return errs.New(errs.KindValidationFailed, "Component Route identity is invalid")
	}
	return nil
}

func routeDesiredEqual(left, right core.Route) bool {
	return left.ID == right.ID && left.Host == right.Host && left.Path == right.Path &&
		left.TargetServiceID == right.TargetServiceID && left.TargetPort == right.TargetPort &&
		left.Exposure == right.Exposure
}
