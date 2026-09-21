package etcd

import (
	"context"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	recordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

func (repository *TaskRepository) prepareRouteTaskAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	terminalAt time.Time,
	revision int64,
) (routeTaskChange, error) {
	mutation, err := repository.prepareRouteMutationTaskAcknowledgement(ctx, task, terminalStatus, terminalAt, revision)
	if err != nil || mutation.applies {
		return mutation, err
	}
	intentRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{environmentchanges.RouteRemovalIntentKey(task.ID)}, Revision: revision,
	})
	if err != nil {
		return routeTaskChange{}, err
	}
	if intentRead == nil || len(intentRead.Values) != 1 {
		return routeTaskChange{}, errs.New(errs.KindInternal, "Route removal intent read is incomplete")
	}
	intentValue := intentRead.Values[0]
	if intentValue == nil {
		return routeTaskChange{}, nil
	}
	intent, err := environmentchanges.DecodeRouteRemovalIntent(intentValue.Value)
	if err != nil {
		return routeTaskChange{}, err
	}
	if err := validateRouteRemovalTaskOwner(task, intent); err != nil {
		return routeTaskChange{}, err
	}
	if intent.Status != taskjournal.TaskStatusPending {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route removal intent is not pending")
	}

	keys := []string{
		deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetRoute), intent.RouteID),
		componentTaskActiveEnvironmentKey(intent.EnvironmentID),
		blueprints.EnvironmentBlueprintHeadKey(intent.EnvironmentID),
	}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return routeTaskChange{}, err
	}
	if state == nil || len(state.Values) != len(keys) || state.Values[0] == nil || state.Values[1] == nil ||
		state.Values[2] == nil || string(state.Values[1].Value) != task.ID ||
		state.Values[2].ModRevision != intent.CurrentProjectionRevision {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route removal state is incomplete")
	}
	tombstone, err := deletionrecord.DecodeDeletionTombstone(state.Values[0].Value)
	if err != nil || tombstone.TargetKind != deletionrecord.DeletionTargetRoute || tombstone.TargetID != intent.RouteID ||
		tombstone.TargetRevision != intent.RouteRevision || tombstone.TaskID != task.ID ||
		tombstone.Phase != routeRemovalTombstonePhase(intent) {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route deletion tombstone changed")
	}
	selectedRevisionID, err := idempotencyrecord.DecodeTaskReference(state.Values[2].Value)
	if err != nil || intent.CurrentProjection == nil || selectedRevisionID != intent.CurrentProjection.RevisionID {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route removal selected projection changed")
	}
	projection, found, err := currentEnvironmentProjectionAtRevision(
		ctx, repository.store, intent.EnvironmentID, revision,
	)
	if err != nil || !found || projection.Revision != intent.CurrentProjectionRevision ||
		!environmentchanges.SameRouteRemovalProjection(projection.Record, *intent.CurrentProjection) {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route removal selected projection changed")
	}
	if _, err := routeAtProjection(ctx, repository.store, projection.Record, projection.Revision, revision, intent.RouteID); err != nil {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route removal selected Route changed")
	}

	terminalIntent, err := environmentchanges.TerminalRouteRemovalIntent(intent, terminalStatus, terminalAt)
	if err != nil {
		return routeTaskChange{}, err
	}
	intentBytes, err := environmentchanges.EncodeRouteRemovalIntent(terminalIntent)
	if err != nil {
		return routeTaskChange{}, err
	}
	change := routeTaskChange{
		applies: true,
		conditions: []etcdstore.Condition{
			{Key: environmentchanges.RouteRemovalIntentKey(task.ID), ModRevision: intentValue.ModRevision},
			{
				Key:         deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetRoute), intent.RouteID),
				ModRevision: state.Values[0].ModRevision,
			},
			{Key: componentTaskActiveEnvironmentKey(intent.EnvironmentID), ModRevision: state.Values[1].ModRevision},
			{Key: blueprints.EnvironmentBlueprintHeadKey(intent.EnvironmentID), ModRevision: state.Values[2].ModRevision},
		},
		mutations: []etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: environmentchanges.RouteRemovalIntentKey(task.ID), Value: intentBytes},
			{Type: etcdstore.MutationDelete, Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetRoute), intent.RouteID)},
		},
		values: [][]byte{intentBytes},
	}
	change.mutations = append(
		change.mutations,
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: componentTaskActiveEnvironmentKey(intent.EnvironmentID)},
	)
	if terminalStatus == taskjournal.TaskStatusCompleted {
		change.mutations[0] = etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: environmentchanges.RouteRemovalIntentKey(task.ID)}
		promotion, promotionErr := prepareRouteHeadPromotion(ctx, repository.store, intent, revision)
		if promotionErr != nil {
			clearRouteTaskChange(change)
			return routeTaskChange{}, promotionErr
		}
		change.conditions = append(change.conditions, promotion.conditions...)
		change.mutations = append(change.mutations, promotion.mutations...)
		change.values = append(change.values, promotion.values...)
		observation, readErr := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{routerecord.ObservationKey(intent.RouteID)}, Revision: revision,
		})
		if readErr != nil {
			clearRouteTaskChange(change)
			return routeTaskChange{}, readErr
		}
		if observation == nil || len(observation.Values) != 1 {
			clearRouteTaskChange(change)
			return routeTaskChange{}, errs.New(errs.KindInternal, "Route observation read is incomplete")
		}
		if observation.Values[0] != nil {
			if _, decodeErr := routerecord.DecodeObservation(observation.Values[0].Value); decodeErr != nil {
				clearRouteTaskChange(change)
				return routeTaskChange{}, decodeErr
			}
			change.conditions = append(
				change.conditions,
				etcdstore.Condition{Key: routerecord.ObservationKey(intent.RouteID), ModRevision: observation.Values[0].ModRevision},
			)
			change.mutations = append(
				change.mutations,
				etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: routerecord.ObservationKey(intent.RouteID)},
			)
			clear(observation.Values[0].Value)
		} else {
			change.conditions = append(change.conditions, etcdstore.Condition{Key: routerecord.ObservationKey(intent.RouteID)})
		}
	}
	return change, nil
}

func (repository *TaskRepository) prepareRouteMutationTaskAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	terminalAt time.Time,
	revision int64,
) (routeTaskChange, error) {
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{environmentchanges.RouteMutationIntentKey(task.ID)}, Revision: revision,
	})
	if err != nil {
		return routeTaskChange{}, err
	}
	if read == nil || len(read.Values) != 1 {
		return routeTaskChange{}, errs.New(errs.KindInternal, "Route mutation acknowledgement read is incomplete")
	}
	if read.Values[0] == nil {
		return routeTaskChange{}, nil
	}
	intent, err := environmentchanges.DecodeRouteMutationIntent(read.Values[0].Value)
	if err != nil {
		return routeTaskChange{}, err
	}
	if err := validateRouteMutationTaskOwner(task, intent); err != nil {
		return routeTaskChange{}, err
	}
	if intent.Status != taskjournal.TaskStatusPending {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route mutation intent is not pending")
	}
	stateKeys := []string{
		componentTaskActiveEnvironmentKey(intent.EnvironmentID),
		routerecord.ObservationKey(intent.RouteID),
	}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: stateKeys, Revision: revision})
	if err != nil {
		return routeTaskChange{}, err
	}
	if state == nil || len(state.Values) != len(stateKeys) || state.Values[0] == nil ||
		string(state.Values[0].Value) != task.ID {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route mutation ownership changed")
	}
	desiredProjection, found, err := currentEnvironmentProjectionAtRevision(
		ctx,
		repository.store,
		intent.EnvironmentID,
		revision,
	)
	if err != nil || !found {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route mutation desired state changed")
	}
	var desired *projectionrecord.EnvironmentRouteProjection
	for index := range desiredProjection.Record.DesiredRoutes {
		if desiredProjection.Record.DesiredRoutes[index].Desired.ID == intent.RouteID {
			desired = &desiredProjection.Record.DesiredRoutes[index]
			break
		}
	}
	if desired == nil || desired.DesiredGeneration != intent.Route.DesiredGeneration ||
		!routeDesiredEqual(desired.Desired, intent.Route.Desired) {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route mutation desired state changed")
	}
	status := routerecord.ObservedUnserved
	var provider routerecord.ProviderObservation
	if intent.Provider != nil {
		status = routerecord.ObservedDegraded
		if terminalStatus == taskjournal.TaskStatusCompleted {
			status = routerecord.ObservedServed
		}
		provider = routerecord.ProviderObservation{
			ComponentID:      intent.Provider.ComponentID,
			DefinitionDigest: intent.Provider.DefinitionDigest,
			CatalogDigest:    intent.Provider.CatalogDigest,
			InputRevision:    intent.Provider.InputRevision,
			InputGeneration:  intent.Provider.InputGeneration,
		}
	}
	observationRecord, err := routerecord.NewObservationRecord(
		intent.EnvironmentID, intent.RouteID, intent.Route.DesiredGeneration,
		routerecord.Observation{Status: status, DesiredGeneration: intent.Route.DesiredGeneration, Provider: provider},
	)
	if err != nil {
		return routeTaskChange{}, err
	}
	terminalIntent, err := environmentchanges.TerminalRouteMutationIntent(intent, terminalStatus, terminalAt)
	if err != nil {
		return routeTaskChange{}, err
	}
	routeValue, err := routerecord.EncodeObservation(observationRecord)
	if err != nil {
		return routeTaskChange{}, err
	}
	intentValue, err := environmentchanges.EncodeRouteMutationIntent(terminalIntent)
	if err != nil {
		clear(routeValue)
		return routeTaskChange{}, err
	}
	observationCondition := etcdstore.Condition{Key: routerecord.ObservationKey(intent.RouteID)}
	if state.Values[1] != nil {
		prior, decodeErr := routerecord.DecodeObservation(state.Values[1].Value)
		if decodeErr != nil || prior.EnvironmentID != intent.EnvironmentID || prior.RouteID != intent.RouteID {
			clear(routeValue)
			clear(intentValue)
			return routeTaskChange{}, recordcodec.CorruptRecord()
		}
		observationCondition.ModRevision = state.Values[1].ModRevision
	}
	change := routeTaskChange{
		applies: true,
		conditions: []etcdstore.Condition{
			{Key: environmentchanges.RouteMutationIntentKey(task.ID), ModRevision: read.Values[0].ModRevision},
			{Key: componentTaskActiveEnvironmentKey(intent.EnvironmentID), ModRevision: state.Values[0].ModRevision},
			observationCondition,
		},
		mutations: []etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: environmentchanges.RouteMutationIntentKey(task.ID), Value: intentValue},
			{Type: etcdstore.MutationPut, Key: routerecord.ObservationKey(intent.RouteID), Value: routeValue},
			{Type: etcdstore.MutationDelete, Key: componentTaskActiveEnvironmentKey(intent.EnvironmentID)},
		},
		values: [][]byte{routeValue, intentValue},
	}
	return change, nil
}
