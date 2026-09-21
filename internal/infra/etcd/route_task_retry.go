package etcd

import (
	"context"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	environmentqueries "github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	recordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"maps"
)

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
		Keys: []string{environmentchanges.RouteRemovalIntentKey(source.ID)}, Revision: revision,
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
	intent, err := environmentchanges.DecodeRouteRemovalIntent(intentValue.Value)
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
	retryIntent := environmentchanges.CloneRouteRemovalIntent(intent)
	retryIntent.TaskID = retry.ID
	retryIntent.Status = taskjournal.TaskStatusPending
	retryIntent.CreatedAt = retry.CreatedAt
	retryIntent.TerminalAt = nil
	if err := environmentchanges.ValidateRouteRemovalIntent(retryIntent); err != nil {
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
	route, err := environmentqueries.RouteAtProjection(
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
			{Key: environmentchanges.RouteRemovalIntentKey(source.ID), ModRevision: intentValue.ModRevision},
			{Key: environmentchanges.RouteRemovalIntentKey(retry.ID)},
			{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetRoute), intent.RouteID)},
		},
	}
	for index, key := range keys {
		condition := etcdstore.Condition{Key: key}
		if parents.Values[index] != nil {
			condition.ModRevision = parents.Values[index].ModRevision
		}
		change.conditions = append(change.conditions, condition)
	}
	tombstone := deletionrecord.DeletionTombstoneRecord{
		TargetKind: deletionrecord.DeletionTargetRoute, TargetID: intent.RouteID, TargetRevision: intent.RouteRevision,
		TaskID: retry.ID, Phase: routeRemovalTombstonePhase(retryIntent),
		CreatedAt: retry.CreatedAt, UpdatedAt: retry.CreatedAt,
	}
	tombstoneValue, err := deletionrecord.EncodeDeletionTombstone(tombstone)
	if err != nil {
		return routeTaskChange{}, err
	}
	intentBytes, err := environmentchanges.EncodeRouteRemovalIntent(retryIntent)
	if err != nil {
		clear(tombstoneValue)
		return routeTaskChange{}, err
	}
	change.values = append(change.values, tombstoneValue, intentBytes)
	change.mutations = append(
		change.mutations,
		etcdstore.Mutation{
			Type: etcdstore.MutationPut, Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetRoute), intent.RouteID),
			Value: tombstoneValue,
		},
		etcdstore.Mutation{
			Type:  etcdstore.MutationPut,
			Key:   environmentchanges.RouteRemovalIntentKey(retry.ID),
			Value: intentBytes,
		},
	)
	if intent.CurrentProjection != nil {
		change.mutations = append(change.mutations, etcdstore.Mutation{
			Type: etcdstore.MutationPut, Key: environmentchanges.ComponentTaskActiveEnvironmentKey(intent.EnvironmentID), Value: []byte(retry.ID),
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
		Keys: []string{environmentchanges.RouteMutationIntentKey(source.ID)}, Revision: revision,
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
	intent, err := environmentchanges.DecodeRouteMutationIntent(read.Values[0].Value)
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
	stateKeys := []string{environmentchanges.ComponentTaskActiveEnvironmentKey(intent.EnvironmentID)}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: stateKeys, Revision: revision})
	if err != nil {
		return routeTaskChange{}, err
	}
	if state == nil || len(state.Values) != len(stateKeys) || state.Values[0] != nil {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route mutation retry state changed")
	}
	current, found, err := blueprints.ReadCurrentProjection(ctx, repository.store, intent.EnvironmentID, revision)
	if err != nil || !found || !routeMutationSelectedProjection(current.Record, source, intent) {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route mutation retry desired state changed")
	}
	retryIntent := environmentchanges.CloneRouteMutationIntent(intent)
	retryIntent.TaskID, retryIntent.Status, retryIntent.CreatedAt, retryIntent.TerminalAt =
		retry.ID, taskjournal.TaskStatusPending, retry.CreatedAt, nil
	if err := environmentchanges.ValidateRouteMutationIntent(retryIntent); err != nil {
		return routeTaskChange{}, err
	}
	encoded, err := environmentchanges.EncodeRouteMutationIntent(retryIntent)
	if err != nil {
		return routeTaskChange{}, err
	}
	conditions := []etcdstore.Condition{
		{Key: environmentchanges.RouteMutationIntentKey(source.ID), ModRevision: read.Values[0].ModRevision},
		{Key: environmentchanges.RouteMutationIntentKey(retry.ID)},
		{Key: environmentchanges.ComponentTaskActiveEnvironmentKey(intent.EnvironmentID)},
	}
	return routeTaskChange{
		applies:    true,
		conditions: conditions,
		mutations: []etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: environmentchanges.RouteMutationIntentKey(retry.ID), Value: encoded},
			{
				Type:  etcdstore.MutationPut,
				Key:   environmentchanges.ComponentTaskActiveEnvironmentKey(intent.EnvironmentID),
				Value: []byte(retry.ID),
			},
		},
		values: [][]byte{encoded},
	}, nil
}

func (repository *TaskRepository) readRouteRetryDependencies(
	ctx context.Context,
	route routerecord.Record,
	intent environmentchanges.RouteRemovalIntent,
	revision int64,
) (*etcdstore.GetManyResult, []string, error) {
	service, err := environmentqueries.FindServiceAtRevision(
		ctx,
		repository.store,
		route.Desired.TargetServiceID,
		revision,
	)
	if err != nil || service.Record.EnvironmentID != route.EnvironmentID {
		return nil, nil, errs.New(errs.KindResourceInUse, "Route retry target Service is unavailable")
	}
	baseKeys := []string{
		hierarchyrecord.EnvironmentKey(route.EnvironmentID),
		servicerecord.ServiceDesiredCondition(service).Key,
		deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetEnvironment), route.EnvironmentID),
		deletionrecord.TombstoneKey("service", route.Desired.TargetServiceID),
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
		deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetProject), environment.ProjectID),
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
		tenantDeletionKey := deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetTenant), project.TenantID)
		tenantRead, readErr := repository.store.GetMany(
			ctx,
			etcdstore.GetManyRequest{Keys: []string{tenantDeletionKey}, Revision: revision},
		)
		if readErr != nil {
			return nil, nil, readErr
		}
		if tenantRead == nil || len(tenantRead.Values) != 1 || tenantRead.Values[0] != nil {
			return nil, nil, errs.New(errs.KindResourceInUse, "Route retry Tenant is unavailable")
		}
		keys = append(keys, tenantDeletionKey)
		values = append(values, tenantRead.Values[0])
	}
	if intent.CurrentProjection != nil {
		activeKey := environmentchanges.ComponentTaskActiveEnvironmentKey(intent.EnvironmentID)
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
