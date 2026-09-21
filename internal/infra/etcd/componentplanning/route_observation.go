package componentplanning

import (
	"context"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	recordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type RouteObservationChange struct {
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	values     [][]byte
}

func (repository *Planner) PrepareRouteObservationAcknowledgement(
	ctx context.Context,
	intent environmentchanges.ComponentTaskIntent,
	terminalStatus taskjournal.TaskStatus,
	revision int64,
) (RouteObservationChange, error) {
	projection := intent.RouteProjection
	if projection == nil || len(projection.Routes) == 0 ||
		projection.Provider == nil && terminalStatus != taskjournal.TaskStatusCompleted {
		return RouteObservationChange{}, nil
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

func (repository *Planner) PrepareRouteObservationRetry(
	ctx context.Context,
	intent environmentchanges.ComponentTaskIntent,
	revision int64,
) (RouteObservationChange, error) {
	if intent.RouteProjection == nil || intent.RouteProjection.Provider == nil ||
		len(intent.RouteProjection.Routes) == 0 {
		return RouteObservationChange{}, nil
	}
	return repository.prepareComponentTaskRouteObservationStatus(
		ctx, intent, routerecord.ObservedPending, revision,
	)
}

func (repository *Planner) prepareComponentTaskRouteObservationStatus(
	ctx context.Context,
	intent environmentchanges.ComponentTaskIntent,
	status routerecord.ObservedStatus,
	revision int64,
) (RouteObservationChange, error) {
	projection := intent.RouteProjection
	desiredProjection, found, err := blueprints.ReadCurrentProjection(
		ctx, repository.store, intent.EnvironmentID, revision,
	)
	if err != nil {
		return RouteObservationChange{}, err
	}
	if !found {
		return RouteObservationChange{}, errs.New(
			errs.KindStateConflict,
			"Component Route desired head is missing",
		)
	}
	change := RouteObservationChange{}
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
			!RouteDesiredEqual(desired.Desired, candidate.Desired) {
			return RouteObservationChange{}, errs.New(
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
			return RouteObservationChange{}, recordErr
		}
		encoded, encodeErr := routerecord.EncodeObservation(observationRecord)
		if encodeErr != nil {
			return RouteObservationChange{}, encodeErr
		}
		key := routerecord.ObservationKey(candidate.Desired.ID)
		read, readErr := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision})
		if readErr != nil {
			clear(encoded)
			return RouteObservationChange{}, readErr
		}
		if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
			clear(encoded)
			return RouteObservationChange{}, errs.New(
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
				return RouteObservationChange{}, recordcodec.CorruptRecord()
			}
			condition.ModRevision = read.Values[0].ModRevision
		}
		change.conditions = append(change.conditions, condition)
		change.values = append(change.values, encoded)
		change.mutations = append(change.mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: key, Value: encoded})
	}
	return change, nil
}

func validateComponentTaskRouteIdentity(route environmentchanges.ComponentTaskRouteCandidate) error {
	if ids.Validate(ids.KindRoute, route.Desired.ID) != nil {
		return errs.New(errs.KindValidationFailed, "Component Route identity is invalid")
	}
	return nil
}

func RouteDesiredEqual(left, right core.Route) bool {
	return left.ID == right.ID && left.Host == right.Host && left.Path == right.Path &&
		left.TargetServiceID == right.TargetServiceID && left.TargetPort == right.TargetPort &&
		left.Exposure == right.Exposure
}
