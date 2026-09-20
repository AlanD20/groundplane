package etcd

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	recordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	"maps"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type routeTaskChange struct {
	applies    bool
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	values     [][]byte
}

func (repository *TaskRepository) prepareRouteTaskRetry(
	ctx context.Context,
	source TaskRecord,
	retry TaskRecord,
	revision int64,
) (routeTaskChange, error) {
	mutation, err := repository.prepareRouteMutationTaskRetry(ctx, source, retry, revision)
	if err != nil || mutation.applies {
		return mutation, err
	}
	intentRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{routeRemovalIntentKey(source.ID)}, Revision: revision,
	})
	if err != nil {
		return routeTaskChange{}, err
	}
	if intentRead == nil || len(intentRead.Values) != 1 {
		return routeTaskChange{}, errs.New(errs.KindInternal, "Route removal retry read is incomplete")
	}
	intentValue := intentRead.Values[0]
	if intentValue == nil {
		return routeTaskChange{}, nil
	}
	intent, err := decodeRouteRemovalIntent(intentValue.Value)
	if err != nil {
		return routeTaskChange{}, err
	}
	if err := validateRouteRemovalTaskOwner(source, intent); err != nil {
		return routeTaskChange{}, err
	}
	if source.FinishedAt == nil || intent.TerminalAt == nil || intent.Status != source.Status ||
		!intent.TerminalAt.Equal(*source.FinishedAt) || retry.RetryOf != source.ID ||
		retry.Executor != source.Executor || retry.Type != source.Type || retry.Target != source.Target ||
		retry.PlanID != source.PlanID || retry.PlanHash != source.PlanHash ||
		retry.RenderGeneration != source.RenderGeneration || !maps.Equal(retry.Params, source.Params) {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route removal retry changed its pinned Task")
	}
	retryIntent := cloneRouteRemovalIntent(intent)
	retryIntent.TaskID = retry.ID
	retryIntent.Status = TaskStatusPending
	retryIntent.CreatedAt = retry.CreatedAt
	retryIntent.TerminalAt = nil
	if err := validateRouteRemovalIntent(retryIntent); err != nil {
		return routeTaskChange{}, err
	}
	if err := validateRouteRemovalTaskOwner(retry, retryIntent); err != nil {
		return routeTaskChange{}, err
	}

	if intent.CurrentProjection == nil {
		return routeTaskChange{}, errs.New(
			errs.KindStateConflict,
			"Route desired projection is unavailable for removal retry",
		)
	}
	route, err := routeAtProjection(
		ctx,
		repository.store,
		*intent.CurrentProjection,
		intent.CurrentProjectionRevision,
		revision,
		intent.RouteID,
	)
	if err != nil {
		return routeTaskChange{}, err
	}

	parents, keys, err := repository.readRouteRetryDependencies(ctx, route, intent, revision)
	if err != nil {
		return routeTaskChange{}, err
	}
	change := routeTaskChange{
		applies: true,
		conditions: []etcdstore.Condition{
			{Key: routeRemovalIntentKey(source.ID), ModRevision: intentValue.ModRevision},
			{Key: routeRemovalIntentKey(retry.ID)},
			{Key: deletionTombstoneKey(string(DeletionTargetRoute), intent.RouteID)},
		},
	}
	for index, key := range keys {
		condition := etcdstore.Condition{Key: key}
		if parents.Values[index] != nil {
			condition.ModRevision = parents.Values[index].ModRevision
		}
		change.conditions = append(change.conditions, condition)
	}
	tombstone := DeletionTombstoneRecord{
		TargetKind: DeletionTargetRoute, TargetID: intent.RouteID, TargetRevision: intent.RouteRevision,
		TaskID: retry.ID, Phase: routeRemovalTombstonePhase(retryIntent),
		CreatedAt: retry.CreatedAt, UpdatedAt: retry.CreatedAt,
	}
	tombstoneValue, err := encodeDeletionTombstone(tombstone)
	if err != nil {
		return routeTaskChange{}, err
	}
	intentBytes, err := encodeRouteRemovalIntent(retryIntent)
	if err != nil {
		clear(tombstoneValue)
		return routeTaskChange{}, err
	}
	change.values = append(change.values, tombstoneValue, intentBytes)
	change.mutations = append(change.mutations,
		etcdstore.Mutation{
			Type: etcdstore.MutationPut, Key: deletionTombstoneKey(string(DeletionTargetRoute), intent.RouteID),
			Value: tombstoneValue,
		},
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: routeRemovalIntentKey(retry.ID), Value: intentBytes},
	)
	if intent.CurrentProjection != nil {
		change.mutations = append(change.mutations, etcdstore.Mutation{
			Type: etcdstore.MutationPut, Key: componentTaskActiveEnvironmentKey(intent.EnvironmentID), Value: []byte(retry.ID),
		})
	}
	return change, nil
}

func (repository *TaskRepository) prepareRouteMutationTaskRetry(
	ctx context.Context,
	source TaskRecord,
	retry TaskRecord,
	revision int64,
) (routeTaskChange, error) {
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{routeMutationIntentKey(source.ID)}, Revision: revision,
	})
	if err != nil {
		return routeTaskChange{}, err
	}
	if read == nil || len(read.Values) != 1 {
		return routeTaskChange{}, errs.New(errs.KindInternal, "Route mutation retry read is incomplete")
	}
	if read.Values[0] == nil {
		return routeTaskChange{}, nil
	}
	intent, err := decodeRouteMutationIntent(read.Values[0].Value)
	if err != nil {
		return routeTaskChange{}, err
	}
	if err := validateRouteMutationTaskOwner(source, intent); err != nil {
		return routeTaskChange{}, err
	}
	if source.FinishedAt == nil || intent.TerminalAt == nil || intent.Status != source.Status ||
		!intent.TerminalAt.Equal(*source.FinishedAt) || retry.RetryOf != source.ID ||
		retry.Executor != source.Executor || retry.Type != source.Type || retry.Target != source.Target ||
		retry.PlanID != source.PlanID || retry.PlanHash != source.PlanHash ||
		retry.RenderGeneration != source.RenderGeneration || !maps.Equal(retry.Params, source.Params) {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route mutation retry changed its pinned Task")
	}
	stateKeys := []string{componentTaskActiveEnvironmentKey(intent.EnvironmentID)}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: stateKeys, Revision: revision})
	if err != nil {
		return routeTaskChange{}, err
	}
	if state == nil || len(state.Values) != len(stateKeys) || state.Values[0] != nil {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route mutation retry state changed")
	}
	current, found, err := currentEnvironmentProjectionAtRevision(ctx, repository.store, intent.EnvironmentID, revision)
	if err != nil || !found || !routeMutationSelectedProjection(current.Record, source, intent) {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route mutation retry desired state changed")
	}
	retryIntent := cloneRouteMutationIntent(intent)
	retryIntent.TaskID, retryIntent.Status, retryIntent.CreatedAt, retryIntent.TerminalAt =
		retry.ID, TaskStatusPending, retry.CreatedAt, nil
	if err := validateRouteMutationIntent(retryIntent); err != nil {
		return routeTaskChange{}, err
	}
	encoded, err := encodeRouteMutationIntent(retryIntent)
	if err != nil {
		return routeTaskChange{}, err
	}
	conditions := []etcdstore.Condition{
		{Key: routeMutationIntentKey(source.ID), ModRevision: read.Values[0].ModRevision},
		{Key: routeMutationIntentKey(retry.ID)},
		{Key: componentTaskActiveEnvironmentKey(intent.EnvironmentID)},
	}
	return routeTaskChange{
		applies:    true,
		conditions: conditions,
		mutations: []etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: routeMutationIntentKey(retry.ID), Value: encoded},
			{Type: etcdstore.MutationPut, Key: componentTaskActiveEnvironmentKey(intent.EnvironmentID), Value: []byte(retry.ID)},
		},
		values: [][]byte{encoded},
	}, nil
}

func (repository *TaskRepository) readRouteRetryDependencies(
	ctx context.Context,
	route routerecord.Record,
	intent RouteRemovalIntent,
	revision int64,
) (*etcdstore.GetManyResult, []string, error) {
	service, err := findServiceAtRevision(ctx, repository.store, route.Desired.TargetServiceID, revision)
	if err != nil || service.Record.EnvironmentID != route.EnvironmentID {
		return nil, nil, errs.New(errs.KindResourceInUse, "Route retry target Service is unavailable")
	}
	baseKeys := []string{
		hierarchyrecord.EnvironmentKey(route.EnvironmentID),
		service.Record.desiredFenceKey,
		deletionTombstoneKey(string(DeletionTargetEnvironment), route.EnvironmentID),
		deletionTombstoneKey("service", route.Desired.TargetServiceID),
	}
	base, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: baseKeys, Revision: revision})
	if err != nil {
		return nil, nil, err
	}
	if base == nil || len(base.Values) != len(baseKeys) || base.Values[0] == nil || base.Values[1] == nil ||
		base.Values[2] != nil || base.Values[3] != nil {
		return nil, nil, errs.New(errs.KindResourceInUse, "Route retry hierarchy is unavailable")
	}
	environment, err := hierarchyrecord.DecodeEnvironment(base.Values[0].Value)
	if err != nil || environment.ID != route.EnvironmentID {
		return nil, nil, recordcodec.CorruptRecord()
	}
	if base.Values[1].ModRevision != service.Revision {
		return nil, nil, recordcodec.CorruptRecord()
	}
	extraKeys := []string{
		hierarchyrecord.ProjectKey(environment.ProjectID),
		deletionTombstoneKey(string(DeletionTargetProject), environment.ProjectID),
	}
	projectRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: extraKeys, Revision: revision})
	if err != nil {
		return nil, nil, err
	}
	if projectRead == nil || len(projectRead.Values) != len(extraKeys) ||
		projectRead.Values[0] == nil || projectRead.Values[1] != nil {
		return nil, nil, errs.New(errs.KindResourceInUse, "Route retry Project is unavailable")
	}
	project, err := hierarchyrecord.DecodeProject(projectRead.Values[0].Value)
	if err != nil || project.ID != environment.ProjectID {
		return nil, nil, recordcodec.CorruptRecord()
	}
	keys := append(baseKeys, extraKeys...)
	values := append(base.Values, projectRead.Values...)
	if project.TenantID != "" {
		hierarchyrecord.TenantKey := deletionTombstoneKey(string(DeletionTargetTenant), project.TenantID)
		tenantRead, readErr := repository.store.GetMany(
			ctx,
			etcdstore.GetManyRequest{Keys: []string{hierarchyrecord.TenantKey}, Revision: revision},
		)
		if readErr != nil {
			return nil, nil, readErr
		}
		if tenantRead == nil || len(tenantRead.Values) != 1 || tenantRead.Values[0] != nil {
			return nil, nil, errs.New(errs.KindResourceInUse, "Route retry Tenant is unavailable")
		}
		keys = append(keys, hierarchyrecord.TenantKey)
		values = append(values, tenantRead.Values[0])
	}
	if intent.CurrentProjection != nil {
		activeKey := componentTaskActiveEnvironmentKey(intent.EnvironmentID)
		activeRead, readErr := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{activeKey}, Revision: revision,
		})
		if readErr != nil {
			return nil, nil, readErr
		}
		if activeRead == nil || len(activeRead.Values) != 1 || activeRead.Values[0] != nil {
			return nil, nil, errs.New(errs.KindStateConflict, "Route retry reconciliation ownership changed")
		}
		keys = append(keys, activeKey)
		values = append(values, activeRead.Values[0])
	}
	return &etcdstore.GetManyResult{Values: values, ReadRevision: revision}, keys, nil
}

func (repository *TaskRepository) prepareRouteTaskAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	terminalAt time.Time,
	revision int64,
) (routeTaskChange, error) {
	mutation, err := repository.prepareRouteMutationTaskAcknowledgement(ctx, task, terminalStatus, terminalAt, revision)
	if err != nil || mutation.applies {
		return mutation, err
	}
	intentRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{routeRemovalIntentKey(task.ID)}, Revision: revision,
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
	intent, err := decodeRouteRemovalIntent(intentValue.Value)
	if err != nil {
		return routeTaskChange{}, err
	}
	if err := validateRouteRemovalTaskOwner(task, intent); err != nil {
		return routeTaskChange{}, err
	}
	if intent.Status != TaskStatusPending {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route removal intent is not pending")
	}

	keys := []string{
		deletionTombstoneKey(string(DeletionTargetRoute), intent.RouteID),
		componentTaskActiveEnvironmentKey(intent.EnvironmentID),
		environmentBlueprintHeadKey(intent.EnvironmentID),
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
	tombstone, err := decodeDeletionTombstone(state.Values[0].Value)
	if err != nil || tombstone.TargetKind != DeletionTargetRoute || tombstone.TargetID != intent.RouteID ||
		tombstone.TargetRevision != intent.RouteRevision || tombstone.TaskID != task.ID ||
		tombstone.Phase != routeRemovalTombstonePhase(intent) {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route deletion tombstone changed")
	}
	selectedRevisionID, err := decodeTaskReference(state.Values[2].Value)
	if err != nil || intent.CurrentProjection == nil || selectedRevisionID != intent.CurrentProjection.RevisionID {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route removal selected projection changed")
	}
	projection, found, err := currentEnvironmentProjectionAtRevision(
		ctx, repository.store, intent.EnvironmentID, revision,
	)
	if err != nil || !found || projection.Revision != intent.CurrentProjectionRevision ||
		!sameRouteRemovalProjection(projection.Record, *intent.CurrentProjection) {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route removal selected projection changed")
	}
	if _, err := routeAtProjection(ctx, repository.store, projection.Record, projection.Revision, revision, intent.RouteID); err != nil {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route removal selected Route changed")
	}

	terminalIntent, err := terminalRouteRemovalIntent(intent, terminalStatus, terminalAt)
	if err != nil {
		return routeTaskChange{}, err
	}
	intentBytes, err := encodeRouteRemovalIntent(terminalIntent)
	if err != nil {
		return routeTaskChange{}, err
	}
	change := routeTaskChange{
		applies: true,
		conditions: []etcdstore.Condition{
			{Key: routeRemovalIntentKey(task.ID), ModRevision: intentValue.ModRevision},
			{
				Key:         deletionTombstoneKey(string(DeletionTargetRoute), intent.RouteID),
				ModRevision: state.Values[0].ModRevision,
			},
			{Key: componentTaskActiveEnvironmentKey(intent.EnvironmentID), ModRevision: state.Values[1].ModRevision},
			{Key: environmentBlueprintHeadKey(intent.EnvironmentID), ModRevision: state.Values[2].ModRevision},
		},
		mutations: []etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: routeRemovalIntentKey(task.ID), Value: intentBytes},
			{Type: etcdstore.MutationDelete, Key: deletionTombstoneKey(string(DeletionTargetRoute), intent.RouteID)},
		},
		values: [][]byte{intentBytes},
	}
	change.mutations = append(
		change.mutations,
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: componentTaskActiveEnvironmentKey(intent.EnvironmentID)},
	)
	if terminalStatus == TaskStatusCompleted {
		change.mutations[0] = etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: routeRemovalIntentKey(task.ID)}
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
	terminalStatus TaskStatus,
	terminalAt time.Time,
	revision int64,
) (routeTaskChange, error) {
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{routeMutationIntentKey(task.ID)}, Revision: revision,
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
	intent, err := decodeRouteMutationIntent(read.Values[0].Value)
	if err != nil {
		return routeTaskChange{}, err
	}
	if err := validateRouteMutationTaskOwner(task, intent); err != nil {
		return routeTaskChange{}, err
	}
	if intent.Status != TaskStatusPending {
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
	var desired *EnvironmentRouteProjection
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
		if terminalStatus == TaskStatusCompleted {
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
	terminalIntent, err := terminalRouteMutationIntent(intent, terminalStatus, terminalAt)
	if err != nil {
		return routeTaskChange{}, err
	}
	routeValue, err := routerecord.EncodeObservation(observationRecord)
	if err != nil {
		return routeTaskChange{}, err
	}
	intentValue, err := encodeRouteMutationIntent(terminalIntent)
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
			{Key: routeMutationIntentKey(task.ID), ModRevision: read.Values[0].ModRevision},
			{Key: componentTaskActiveEnvironmentKey(intent.EnvironmentID), ModRevision: state.Values[0].ModRevision},
			observationCondition,
		},
		mutations: []etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: routeMutationIntentKey(task.ID), Value: intentValue},
			{Type: etcdstore.MutationPut, Key: routerecord.ObservationKey(intent.RouteID), Value: routeValue},
			{Type: etcdstore.MutationDelete, Key: componentTaskActiveEnvironmentKey(intent.EnvironmentID)},
		},
		values: [][]byte{routeValue, intentValue},
	}
	return change, nil
}

func (repository *TaskRepository) validateRouteTaskAcknowledgementReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	revision int64,
) error {
	matched, err := repository.validateRouteMutationTaskAcknowledgementReplay(ctx, task, terminalStatus, revision)
	if err != nil || matched {
		return err
	}
	intentRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{routeRemovalIntentKey(task.ID)}, Revision: revision,
	})
	if err != nil {
		return err
	}
	if intentRead == nil || len(intentRead.Values) != 1 {
		return errs.New(errs.KindInternal, "Route removal replay read is incomplete")
	}
	if intentRead.Values[0] == nil {
		if terminalStatus == TaskStatusCompleted {
			return validateCompletedRouteHeadReplay(ctx, repository.store, task, revision)
		}
		return nil
	}
	intent, err := decodeRouteRemovalIntent(intentRead.Values[0].Value)
	if err != nil {
		return err
	}
	if err := validateRouteRemovalTaskOwner(task, intent); err != nil {
		return err
	}
	if intent.Status != terminalStatus || intent.TerminalAt == nil || task.FinishedAt == nil ||
		!intent.TerminalAt.Equal(*task.FinishedAt) {
		return errs.New(errs.KindStateConflict, "Route removal intent does not match terminal Task")
	}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			deletionTombstoneKey(string(DeletionTargetRoute), intent.RouteID),
			componentTaskActiveEnvironmentKey(intent.EnvironmentID),
		},
		Revision: revision,
	})
	if err != nil {
		return err
	}
	if state == nil || len(state.Values) != 2 || state.Values[0] != nil || state.Values[1] != nil {
		return errs.New(errs.KindStateConflict, "Route removal terminal fence is inconsistent")
	}
	if intent.CandidateProjection == nil {
		return errs.New(errs.KindStateConflict, "Route removal terminal projection is missing")
	}
	if terminalStatus == TaskStatusCompleted {
		return errs.New(errs.KindStateConflict, "completed Route removal retained its active intent")
	}
	staging, err := prepareRouteHeadCandidate(ctx, repository.store, intent, revision)
	if err != nil {
		return err
	}
	clearRouteHeadPublication(staging)
	projection, found, projectionErr := currentEnvironmentProjectionAtRevision(
		ctx, repository.store, intent.EnvironmentID, revision,
	)
	if projectionErr != nil || !found || intent.CurrentProjection == nil ||
		!sameRouteRemovalProjection(projection.Record, *intent.CurrentProjection) {
		return errs.New(errs.KindStateConflict, "Route removal terminal projection changed")
	}
	return nil
}

func (repository *TaskRepository) validateRouteMutationTaskAcknowledgementReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	revision int64,
) (bool, error) {
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{routeMutationIntentKey(task.ID)}, Revision: revision,
	})
	if err != nil {
		return false, err
	}
	if read == nil || len(read.Values) != 1 {
		return false, errs.New(errs.KindInternal, "Route mutation replay read is incomplete")
	}
	if read.Values[0] == nil {
		return false, nil
	}
	intent, err := decodeRouteMutationIntent(read.Values[0].Value)
	if err != nil || validateRouteMutationTaskOwner(task, intent) != nil || intent.Status != terminalStatus ||
		intent.TerminalAt == nil || task.FinishedAt == nil || !intent.TerminalAt.Equal(*task.FinishedAt) {
		return true, errs.New(errs.KindStateConflict, "Route mutation replay evidence changed")
	}
	stateKeys := []string{componentTaskActiveEnvironmentKey(intent.EnvironmentID)}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: stateKeys, Revision: revision})
	if err != nil {
		return true, err
	}
	if state == nil || len(state.Values) != len(stateKeys) || state.Values[0] != nil {
		return true, errs.New(errs.KindStateConflict, "Route mutation replay state is incomplete")
	}
	projection, found, projectionErr := currentEnvironmentProjectionAtRevision(
		ctx,
		repository.store,
		intent.EnvironmentID,
		revision,
	)
	if projectionErr != nil || !found || !routeMutationSelectedProjection(projection.Record, task, intent) {
		return true, errs.New(errs.KindStateConflict, "Route mutation terminal projection changed")
	}
	return true, nil
}

func routeMutationSelectedProjection(
	projection EnvironmentComposeProjection,
	task TaskRecord,
	intent RouteMutationIntent,
) bool {
	if intent.CandidateProjection != nil {
		return sameRouteRemovalProjection(projection, *intent.CandidateProjection)
	}
	selectedRevisionID := task.ID
	if task.RetryOf != "" {
		selectedRevisionID = task.RetryOf
	}
	if intent.Provider != nil || projection.RevisionID != selectedRevisionID {
		return false
	}
	for _, route := range projection.DesiredRoutes {
		if route.Desired.ID == intent.RouteID {
			return route.DesiredGeneration == intent.Route.DesiredGeneration &&
				routeDesiredEqual(route.Desired, intent.Route.Desired)
		}
	}
	return false
}

func validateRouteMutationTaskOwner(task TaskRecord, intent RouteMutationIntent) error {
	executor := TaskExecutorController
	if intent.Provider != nil {
		executor = TaskExecutorAgent
	}
	if task.ID != intent.TaskID || task.OperationID != intent.OperationID || task.Executor != executor ||
		(task.Type != TaskCreate && task.Type != TaskUpdate) || task.Target != intent.RouteID ||
		!task.CreatedAt.Equal(intent.CreatedAt) || len(task.Params) < 2 ||
		task.Params[TaskResourceKindParam] != TaskResourceRoute ||
		task.Params[TaskRouteEnvironmentParam] != intent.EnvironmentID {
		return errs.New(errs.KindStateConflict, "Route mutation intent does not belong to its Task")
	}
	return nil
}

func sameRouteDesiredVersion(left routerecord.Record, right routerecord.Record) bool {
	return left.EnvironmentID == right.EnvironmentID && left.Desired == right.Desired &&
		left.DesiredGeneration == right.DesiredGeneration
}

func validateRouteRemovalTaskOwner(task TaskRecord, intent RouteRemovalIntent) error {
	expectedExecutor := TaskExecutorController
	validParams := len(task.Params) == 2 && task.Params[TaskResourceKindParam] == TaskResourceRoute &&
		task.Params[TaskRouteEnvironmentParam] == intent.EnvironmentID
	if intent.Provider != nil {
		expectedExecutor = TaskExecutorAgent
		validParams = intent.CandidateProjection != nil && len(task.Params) == 4 &&
			task.Params[TaskRouteEnvironmentParam] == intent.EnvironmentID &&
			task.Params[TaskMaterializationEnvironmentParam] == intent.EnvironmentID &&
			task.Params[EnvironmentDesiredRevisionParam] == intent.CandidateProjection.RevisionID
	}
	if task.ID != intent.TaskID || task.Executor != expectedExecutor || task.Type != TaskRemove ||
		task.Target != intent.RouteID || !task.CreatedAt.Equal(intent.CreatedAt) || !validParams {
		return errs.New(errs.KindStateConflict, "Route removal intent does not belong to its Task")
	}
	return nil
}

func routeRemovalTombstonePhase(intent RouteRemovalIntent) DeletionPhase {
	if intent.Provider != nil {
		return DeletionPhaseHostEffects
	}
	return DeletionPhaseFinalizing
}

func clearRouteTaskChange(change routeTaskChange) {
	for _, value := range change.values {
		clear(value)
	}
}
