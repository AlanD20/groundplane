package etcd

import (
	"context"
	"maps"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type routeTaskChange struct {
	applies    bool
	conditions []Condition
	mutations  []Mutation
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
	intentRead, err := repository.store.GetMany(ctx, GetManyRequest{
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

	primary, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			routeKey(intent.RouteID),
			deletionTombstoneKey(string(DeletionTargetRoute), intent.RouteID),
		},
		Revision: revision,
	})
	if err != nil {
		return routeTaskChange{}, err
	}
	if primary == nil || len(primary.Values) != 2 || primary.Values[0] == nil || primary.Values[1] != nil ||
		primary.Values[0].ModRevision != intent.RouteRevision {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route is not available for removal retry")
	}
	route, err := decodeRouteRecord(primary.Values[0].Value)
	if err != nil || route.Desired.ID != intent.RouteID || route.EnvironmentID != intent.EnvironmentID {
		return routeTaskChange{}, corruptRecord()
	}

	parents, keys, err := repository.readRouteRetryDependencies(ctx, route, intent, revision)
	if err != nil {
		return routeTaskChange{}, err
	}
	change := routeTaskChange{
		applies: true,
		conditions: []Condition{
			{Key: routeRemovalIntentKey(source.ID), ModRevision: intentValue.ModRevision},
			{Key: routeRemovalIntentKey(retry.ID)},
			{Key: routeKey(intent.RouteID), ModRevision: primary.Values[0].ModRevision},
			{Key: deletionTombstoneKey(string(DeletionTargetRoute), intent.RouteID)},
		},
	}
	for index, key := range keys {
		condition := Condition{Key: key}
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
		Mutation{
			Type: MutationPut, Key: deletionTombstoneKey(string(DeletionTargetRoute), intent.RouteID),
			Value: tombstoneValue,
		},
		Mutation{Type: MutationPut, Key: routeRemovalIntentKey(retry.ID), Value: intentBytes},
	)
	if intent.CurrentProjection != nil {
		change.mutations = append(change.mutations, Mutation{
			Type: MutationPut, Key: componentTaskActiveEnvironmentKey(intent.EnvironmentID), Value: []byte(retry.ID),
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
	read, err := repository.store.GetMany(ctx, GetManyRequest{
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
	stateKeys := []string{routeKey(intent.RouteID), componentTaskActiveEnvironmentKey(intent.EnvironmentID)}
	if intent.Provider != nil {
		stateKeys = append(stateKeys, environmentComposeProjectionKey(intent.EnvironmentID))
	}
	state, err := repository.store.GetMany(ctx, GetManyRequest{Keys: stateKeys, Revision: revision})
	if err != nil {
		return routeTaskChange{}, err
	}
	if state == nil || len(state.Values) != len(stateKeys) || state.Values[0] == nil || state.Values[1] != nil {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route mutation retry state changed")
	}
	record, err := decodeRouteRecord(state.Values[0].Value)
	if err != nil || !sameRouteDesiredVersion(record, intent.Route) {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route mutation retry desired state changed")
	}
	if intent.Provider != nil {
		if intent.CurrentProjection == nil || state.Values[2] == nil ||
			state.Values[2].ModRevision != intent.CurrentProjectionRevision {
			return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route mutation retry applied projection changed")
		}
		projection, decodeErr := decodeEnvironmentComposeProjection(state.Values[2].Value)
		if decodeErr != nil || !sameRouteRemovalProjection(projection, *intent.CurrentProjection) {
			return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route mutation retry applied projection changed")
		}
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
	conditions := []Condition{
		{Key: routeMutationIntentKey(source.ID), ModRevision: read.Values[0].ModRevision},
		{Key: routeMutationIntentKey(retry.ID)},
		{Key: routeKey(intent.RouteID), ModRevision: state.Values[0].ModRevision},
		{Key: componentTaskActiveEnvironmentKey(intent.EnvironmentID)},
	}
	if intent.Provider != nil {
		conditions = append(conditions, Condition{
			Key: environmentComposeProjectionKey(intent.EnvironmentID), ModRevision: state.Values[2].ModRevision,
		})
	}
	return routeTaskChange{
		applies:    true,
		conditions: conditions,
		mutations: []Mutation{
			{Type: MutationPut, Key: routeMutationIntentKey(retry.ID), Value: encoded},
			{Type: MutationPut, Key: componentTaskActiveEnvironmentKey(intent.EnvironmentID), Value: []byte(retry.ID)},
		},
		values: [][]byte{encoded},
	}, nil
}

func (repository *TaskRepository) readRouteRetryDependencies(
	ctx context.Context,
	route RouteRecord,
	intent RouteRemovalIntent,
	revision int64,
) (*GetManyResult, []string, error) {
	baseKeys := []string{
		routeOwnerKey(route.EnvironmentID, route.Desired.ID),
		routeMatchKey(route.EnvironmentID, route.Desired.Host, route.Desired.Path),
		environmentKey(route.EnvironmentID),
		serviceKey(route.Desired.TargetServiceID),
		deletionTombstoneKey(string(DeletionTargetEnvironment), route.EnvironmentID),
		deletionTombstoneKey("service", route.Desired.TargetServiceID),
	}
	base, err := repository.store.GetMany(ctx, GetManyRequest{Keys: baseKeys, Revision: revision})
	if err != nil {
		return nil, nil, err
	}
	if base == nil || len(base.Values) != len(baseKeys) || base.Values[0] == nil || base.Values[1] == nil ||
		base.Values[2] == nil || base.Values[3] == nil || base.Values[4] != nil || base.Values[5] != nil ||
		string(base.Values[0].Value) != route.Desired.ID || string(base.Values[1].Value) != route.Desired.ID {
		return nil, nil, errs.New(errs.KindResourceInUse, "Route retry hierarchy is unavailable")
	}
	environment, err := decodeEnvironment(base.Values[2].Value)
	if err != nil || environment.ID != route.EnvironmentID {
		return nil, nil, corruptRecord()
	}
	service, err := decodeServiceRecord(base.Values[3].Value)
	if err != nil || service.Desired.ID != route.Desired.TargetServiceID ||
		service.EnvironmentID != route.EnvironmentID {
		return nil, nil, corruptRecord()
	}
	extraKeys := []string{
		projectKey(environment.ProjectID),
		deletionTombstoneKey(string(DeletionTargetProject), environment.ProjectID),
	}
	projectRead, err := repository.store.GetMany(ctx, GetManyRequest{Keys: extraKeys, Revision: revision})
	if err != nil {
		return nil, nil, err
	}
	if projectRead == nil || len(projectRead.Values) != len(extraKeys) ||
		projectRead.Values[0] == nil || projectRead.Values[1] != nil {
		return nil, nil, errs.New(errs.KindResourceInUse, "Route retry Project is unavailable")
	}
	project, err := decodeProject(projectRead.Values[0].Value)
	if err != nil || project.ID != environment.ProjectID {
		return nil, nil, corruptRecord()
	}
	keys := append(baseKeys, extraKeys...)
	values := append(base.Values, projectRead.Values...)
	if project.TenantID != "" {
		tenantKey := deletionTombstoneKey(string(DeletionTargetTenant), project.TenantID)
		tenantRead, readErr := repository.store.GetMany(
			ctx,
			GetManyRequest{Keys: []string{tenantKey}, Revision: revision},
		)
		if readErr != nil {
			return nil, nil, readErr
		}
		if tenantRead == nil || len(tenantRead.Values) != 1 || tenantRead.Values[0] != nil {
			return nil, nil, errs.New(errs.KindResourceInUse, "Route retry Tenant is unavailable")
		}
		keys = append(keys, tenantKey)
		values = append(values, tenantRead.Values[0])
	}
	if intent.CurrentProjection != nil {
		projectionKey := environmentComposeProjectionKey(intent.EnvironmentID)
		activeKey := componentTaskActiveEnvironmentKey(intent.EnvironmentID)
		projectionRead, readErr := repository.store.GetMany(ctx, GetManyRequest{
			Keys: []string{projectionKey, activeKey}, Revision: revision,
		})
		if readErr != nil {
			return nil, nil, readErr
		}
		if projectionRead == nil || len(projectionRead.Values) != 2 || projectionRead.Values[0] == nil ||
			projectionRead.Values[1] != nil || projectionRead.Values[0].ModRevision != intent.CurrentProjectionRevision {
			return nil, nil, errs.New(errs.KindStateConflict, "Route retry applied projection changed")
		}
		projection, decodeErr := decodeEnvironmentComposeProjection(projectionRead.Values[0].Value)
		if decodeErr != nil || !sameRouteRemovalProjection(projection, *intent.CurrentProjection) {
			return nil, nil, errs.New(errs.KindStateConflict, "Route retry applied projection changed")
		}
		keys = append(keys, projectionKey, activeKey)
		values = append(values, projectionRead.Values...)
	}
	return &GetManyResult{Values: values, ReadRevision: revision}, keys, nil
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
	intentRead, err := repository.store.GetMany(ctx, GetManyRequest{
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
		routeKey(intent.RouteID),
		deletionTombstoneKey(string(DeletionTargetRoute), intent.RouteID),
	}
	state, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return routeTaskChange{}, err
	}
	if state == nil || len(state.Values) != 2 || state.Values[0] == nil || state.Values[1] == nil ||
		state.Values[0].ModRevision != intent.RouteRevision {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route removal state is incomplete")
	}
	route, err := decodeRouteRecord(state.Values[0].Value)
	if err != nil || route.Desired.ID != intent.RouteID || route.EnvironmentID != intent.EnvironmentID {
		return routeTaskChange{}, corruptRecord()
	}
	tombstone, err := decodeDeletionTombstone(state.Values[1].Value)
	if err != nil || tombstone.TargetKind != DeletionTargetRoute || tombstone.TargetID != intent.RouteID ||
		tombstone.TargetRevision != intent.RouteRevision || tombstone.TaskID != task.ID ||
		tombstone.Phase != routeRemovalTombstonePhase(intent) {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route deletion tombstone changed")
	}

	companionKeys := []string{
		routeOwnerKey(route.EnvironmentID, route.Desired.ID),
		routeMatchKey(route.EnvironmentID, route.Desired.Host, route.Desired.Path),
	}
	if intent.CurrentProjection != nil {
		companionKeys = append(companionKeys,
			environmentComposeProjectionKey(intent.EnvironmentID),
			componentTaskActiveEnvironmentKey(intent.EnvironmentID),
		)
	}
	companions, err := repository.store.GetMany(ctx, GetManyRequest{Keys: companionKeys, Revision: revision})
	if err != nil {
		return routeTaskChange{}, err
	}
	if companions == nil || len(companions.Values) != len(companionKeys) || companions.Values[0] == nil ||
		companions.Values[1] == nil || string(companions.Values[0].Value) != intent.RouteID ||
		string(companions.Values[1].Value) != intent.RouteID {
		return routeTaskChange{}, errs.New(errs.KindInternal, "Route deletion indexes are inconsistent")
	}
	if intent.CurrentProjection != nil {
		if companions.Values[2] == nil || companions.Values[3] == nil ||
			companions.Values[2].ModRevision != intent.CurrentProjectionRevision ||
			string(companions.Values[3].Value) != task.ID {
			return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route removal projection ownership changed")
		}
		projection, decodeErr := decodeEnvironmentComposeProjection(companions.Values[2].Value)
		if decodeErr != nil || !sameRouteRemovalProjection(projection, *intent.CurrentProjection) {
			return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route removal applied projection changed")
		}
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
		conditions: []Condition{
			{Key: routeRemovalIntentKey(task.ID), ModRevision: intentValue.ModRevision},
			{Key: routeKey(intent.RouteID), ModRevision: state.Values[0].ModRevision},
			{
				Key:         deletionTombstoneKey(string(DeletionTargetRoute), intent.RouteID),
				ModRevision: state.Values[1].ModRevision,
			},
			{Key: companionKeys[0], ModRevision: companions.Values[0].ModRevision},
			{Key: companionKeys[1], ModRevision: companions.Values[1].ModRevision},
		},
		mutations: []Mutation{
			{Type: MutationPut, Key: routeRemovalIntentKey(task.ID), Value: intentBytes},
			{Type: MutationDelete, Key: deletionTombstoneKey(string(DeletionTargetRoute), intent.RouteID)},
		},
		values: [][]byte{intentBytes},
	}
	if intent.CurrentProjection != nil {
		change.conditions = append(change.conditions,
			Condition{Key: companionKeys[2], ModRevision: companions.Values[2].ModRevision},
			Condition{Key: companionKeys[3], ModRevision: companions.Values[3].ModRevision},
		)
		change.mutations = append(change.mutations, Mutation{
			Type: MutationDelete, Key: componentTaskActiveEnvironmentKey(intent.EnvironmentID),
		})
	}
	if terminalStatus == TaskStatusCompleted {
		change.mutations = append(change.mutations,
			Mutation{Type: MutationDelete, Key: routeOwnerKey(route.EnvironmentID, route.Desired.ID)},
			Mutation{
				Type: MutationDelete,
				Key:  routeMatchKey(route.EnvironmentID, route.Desired.Host, route.Desired.Path),
			},
			Mutation{Type: MutationDelete, Key: routeKey(intent.RouteID)},
		)
		if intent.CandidateProjection != nil {
			projectionValue, encodeErr := encodeEnvironmentComposeProjection(*intent.CandidateProjection)
			if encodeErr != nil {
				clearRouteTaskChange(change)
				return routeTaskChange{}, encodeErr
			}
			change.values = append(change.values, projectionValue)
			change.mutations = append(change.mutations, Mutation{
				Type: MutationPut, Key: environmentComposeProjectionKey(intent.EnvironmentID), Value: projectionValue,
			})
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
	read, err := repository.store.GetMany(ctx, GetManyRequest{
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
	stateKeys := []string{routeKey(intent.RouteID), componentTaskActiveEnvironmentKey(intent.EnvironmentID)}
	if intent.Provider != nil {
		stateKeys = append(stateKeys, environmentComposeProjectionKey(intent.EnvironmentID))
	}
	state, err := repository.store.GetMany(ctx, GetManyRequest{Keys: stateKeys, Revision: revision})
	if err != nil {
		return routeTaskChange{}, err
	}
	if state == nil || len(state.Values) != len(stateKeys) || state.Values[0] == nil || state.Values[1] == nil ||
		string(state.Values[1].Value) != task.ID {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route mutation ownership changed")
	}
	record, err := decodeRouteRecord(state.Values[0].Value)
	if err != nil || !sameRouteDesiredVersion(record, intent.Route) {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route mutation desired state changed")
	}
	if intent.Provider != nil {
		if state.Values[2] == nil || state.Values[2].ModRevision != intent.CurrentProjectionRevision {
			return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route mutation applied projection changed")
		}
		projection, decodeErr := decodeEnvironmentComposeProjection(state.Values[2].Value)
		if decodeErr != nil || intent.CurrentProjection == nil ||
			!sameRouteRemovalProjection(projection, *intent.CurrentProjection) {
			return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route mutation applied projection changed")
		}
	}
	status := RouteObservedUnserved
	var provider RouteProviderObservation
	if intent.Provider != nil {
		status = RouteObservedDegraded
		if terminalStatus == TaskStatusCompleted {
			status = RouteObservedServed
		}
		provider = RouteProviderObservation{
			ComponentID:      intent.Provider.ComponentID,
			DefinitionDigest: intent.Provider.DefinitionDigest,
			CatalogDigest:    intent.Provider.CatalogDigest,
			InputRevision:    intent.Provider.InputRevision,
			InputGeneration:  intent.Provider.InputGeneration,
		}
	}
	record, err = SetRouteObservation(record, RouteObservation{
		Status: status, DesiredGeneration: record.DesiredGeneration, Provider: provider,
	})
	if err != nil {
		return routeTaskChange{}, err
	}
	terminalIntent, err := terminalRouteMutationIntent(intent, terminalStatus, terminalAt)
	if err != nil {
		return routeTaskChange{}, err
	}
	routeValue, err := encodeRouteRecord(record)
	if err != nil {
		return routeTaskChange{}, err
	}
	intentValue, err := encodeRouteMutationIntent(terminalIntent)
	if err != nil {
		clear(routeValue)
		return routeTaskChange{}, err
	}
	change := routeTaskChange{
		applies: true,
		conditions: []Condition{
			{Key: routeMutationIntentKey(task.ID), ModRevision: read.Values[0].ModRevision},
			{Key: routeKey(intent.RouteID), ModRevision: state.Values[0].ModRevision},
			{Key: componentTaskActiveEnvironmentKey(intent.EnvironmentID), ModRevision: state.Values[1].ModRevision},
		},
		mutations: []Mutation{
			{Type: MutationPut, Key: routeMutationIntentKey(task.ID), Value: intentValue},
			{Type: MutationPut, Key: routeKey(intent.RouteID), Value: routeValue},
			{Type: MutationDelete, Key: componentTaskActiveEnvironmentKey(intent.EnvironmentID)},
		},
		values: [][]byte{routeValue, intentValue},
	}
	if intent.Provider != nil {
		change.conditions = append(change.conditions, Condition{
			Key: environmentComposeProjectionKey(intent.EnvironmentID), ModRevision: state.Values[2].ModRevision,
		})
		if terminalStatus == TaskStatusCompleted {
			projectionValue, encodeErr := encodeEnvironmentComposeProjection(*intent.CandidateProjection)
			if encodeErr != nil {
				clearRouteTaskChange(change)
				return routeTaskChange{}, encodeErr
			}
			change.values = append(change.values, projectionValue)
			change.mutations = append(change.mutations, Mutation{
				Type: MutationPut, Key: environmentComposeProjectionKey(intent.EnvironmentID), Value: projectionValue,
			})
		}
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
	intentRead, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{routeRemovalIntentKey(task.ID)}, Revision: revision,
	})
	if err != nil {
		return err
	}
	if intentRead == nil || len(intentRead.Values) != 1 {
		return errs.New(errs.KindInternal, "Route removal replay read is incomplete")
	}
	if intentRead.Values[0] == nil {
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
	state, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			routeKey(intent.RouteID),
			deletionTombstoneKey(string(DeletionTargetRoute), intent.RouteID),
			componentTaskActiveEnvironmentKey(intent.EnvironmentID),
		},
		Revision: revision,
	})
	if err != nil {
		return err
	}
	if state == nil || len(state.Values) != 3 || state.Values[1] != nil {
		return errs.New(errs.KindStateConflict, "Route removal terminal fence is inconsistent")
	}
	if terminalStatus == TaskStatusCompleted && state.Values[0] != nil {
		return errs.New(errs.KindStateConflict, "completed Route removal retained its target")
	}
	if terminalStatus != TaskStatusCompleted && state.Values[0] == nil {
		return errs.New(errs.KindStateConflict, "failed Route removal lost its target")
	}
	if state.Values[2] != nil {
		activeTaskID := string(state.Values[2].Value)
		if activeTaskID == task.ID || ids.Validate(ids.KindTask, activeTaskID) != nil {
			return errs.New(errs.KindStateConflict, "terminal Route removal remains active")
		}
	}
	return nil
}

func (repository *TaskRepository) validateRouteMutationTaskAcknowledgementReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	revision int64,
) (bool, error) {
	read, err := repository.store.GetMany(ctx, GetManyRequest{
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
	stateKeys := []string{routeKey(intent.RouteID), componentTaskActiveEnvironmentKey(intent.EnvironmentID)}
	if intent.Provider != nil {
		stateKeys = append(stateKeys, environmentComposeProjectionKey(intent.EnvironmentID))
	}
	state, err := repository.store.GetMany(ctx, GetManyRequest{Keys: stateKeys, Revision: revision})
	if err != nil {
		return true, err
	}
	if state == nil || len(state.Values) != len(stateKeys) || state.Values[0] == nil {
		return true, errs.New(errs.KindStateConflict, "Route mutation replay state is incomplete")
	}
	record, err := decodeRouteRecord(state.Values[0].Value)
	if err != nil || !sameRouteDesiredVersion(record, intent.Route) || state.Values[1] != nil && string(state.Values[1].Value) == task.ID {
		return true, errs.New(errs.KindStateConflict, "Route mutation replay state changed")
	}
	if intent.Provider != nil {
		if state.Values[2] == nil {
			return true, errs.New(errs.KindStateConflict, "Route mutation terminal projection is missing")
		}
		projection, decodeErr := decodeEnvironmentComposeProjection(state.Values[2].Value)
		want := intent.CurrentProjection
		if terminalStatus == TaskStatusCompleted {
			want = intent.CandidateProjection
		}
		if decodeErr != nil || want == nil || !sameRouteRemovalProjection(projection, *want) {
			return true, errs.New(errs.KindStateConflict, "Route mutation terminal projection changed")
		}
	}
	return true, nil
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

func sameRouteDesiredVersion(left RouteRecord, right RouteRecord) bool {
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
