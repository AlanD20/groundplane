package etcd

import (
	"context"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// BeginRouteDeletionWithTask atomically fences one Route, pins its exact
// applied-projection candidate, and publishes the Task that owns finalization.
// The Route and its indexes remain visible until successful acknowledgement.
func (repository *RouteRepository) BeginRouteDeletionWithTask(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	target etcdstore.Versioned[servicerecord.ServiceRecord],
	route etcdstore.Versioned[routerecord.Record],
	projection *etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
	tombstone deletionrecord.DeletionTombstoneRecord,
	intent RouteRemovalIntent,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (_ IdempotencyTransactionResult, publicationErr error) {
	if err := validateRouteHierarchy(ctx, environment, project, target, route.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateRouteVersion(route); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := deletionrecord.ValidateDeletionTombstone(tombstone); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateRouteRemovalIntent(intent); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateRouteDeletionProjection(projection, intent); err != nil {
		return IdempotencyTransactionResult{}, err
	}

	if err := validateRouteRemovalTaskOwner(task, intent); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if tombstone.TargetKind != deletionrecord.DeletionTargetRoute || tombstone.TargetID != route.Record.Desired.ID ||
		tombstone.TargetRevision != route.Revision || tombstone.TaskID != task.ID ||
		tombstone.Phase != routeRemovalTombstonePhase(intent) || !tombstone.CreatedAt.Equal(task.CreatedAt) ||
		!tombstone.UpdatedAt.Equal(tombstone.CreatedAt) || intent.TaskID != task.ID ||
		intent.EnvironmentID != environment.Record.ID || intent.RouteID != route.Record.Desired.ID ||
		intent.RouteRevision != route.Revision || intent.Status != taskjournal.TaskStatusPending ||
		!intent.CreatedAt.Equal(task.CreatedAt) || intent.TerminalAt != nil || task.Type != taskjournal.TaskRemove ||
		task.Target != route.Record.Desired.ID || task.Status != taskjournal.TaskStatusPending {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Route deletion Task, intent, and tombstone do not match",
		)
	}
	wantReplayTarget := idempotencyrecord.IdempotencyReplayTarget{Kind: idempotencyrecord.IdempotencyReplayTargetRoute, ID: route.Record.Desired.ID}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerTask || marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != environment.Record.ID || marker.ReplayTarget == nil ||
		*marker.ReplayTarget != wantReplayTarget || !marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Route deletion marker does not match its Task",
		)
	}

	if projection == nil || intent.CurrentProjection == nil || intent.CandidateProjection == nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "Route desired head is unavailable")
	}
	audit := EnvironmentDesiredMutationAudit{Route: &EnvironmentRouteMutationAudit{
		Action: EnvironmentRouteMutationRemove, BaseRevisionID: intent.CurrentProjection.RevisionID,
		RouteID: route.Record.Desired.ID,
	}}
	publication, err := prepareRouteHeadPublication(
		ctx, repository.store, task, marker, intent.CurrentProjection,
		*intent.CandidateProjection, audit, intent.CurrentProjectionRevision,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clearRouteHeadPublication(publication)

	task = cloneTaskRecord(task)
	if task.IdempotencyKey == "" {
		task.IdempotencyKey = marker.Locator.Key
	}
	task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	if err := validateTaskRecord(task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	configuration, err := prepareConfigurationTaskPublication(
		ctx, repository.store, task, projection.ReadRevision,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	task = configuration.task
	defer configuration.clear()
	defer func() { publicationErr = configuration.finish(ctx, repository.store, publicationErr) }()
	tombstoneValue, err := deletionrecord.EncodeDeletionTombstone(tombstone)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(tombstoneValue)
	intentValue, err := encodeRouteRemovalIntent(intent)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(intentValue)
	taskValue, err := encodeTaskRecord(task)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	reference, err := idempotencyrecord.EncodeTaskReference(task.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(reference)

	tombstoneKey := deletionTombstoneKey(string(deletionrecord.DeletionTargetRoute), route.Record.Desired.ID)
	conditions := []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(task.ID)},
		{Key: taskjournal.TaskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskjournal.TaskActiveOperationKey(task.OperationID)},
		{Key: taskjournal.TaskQueueKey(task.Executor, task.ID)},
		{Key: hierarchyrecord.EnvironmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{Key: hierarchyrecord.ProjectKey(project.Record.ID), ModRevision: project.Revision},
		{Key: routeRemovalIntentKey(task.ID)},
		{Key: tombstoneKey},
		{Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetEnvironment), environment.Record.ID)},
		{Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetProject), project.Record.ID)},
		{Key: deletionTombstoneKey("service", target.Record.Desired.ID)},
	}
	if project.Record.TenantID != "" {
		conditions = append(conditions, etcdstore.Condition{
			Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetTenant), project.Record.TenantID),
		})
	}
	conditions = append(conditions, etcdstore.Condition{Key: componentTaskActiveEnvironmentKey(environment.Record.ID)})
	conditions = append(conditions, publication.conditions...)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(task.ID), Value: taskValue},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskActiveOperationKey(task.OperationID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskQueueKey(task.Executor, task.ID), Value: reference},
		{Type: etcdstore.MutationPut, Key: tombstoneKey, Value: tombstoneValue},
		{Type: etcdstore.MutationPut, Key: routeRemovalIntentKey(task.ID), Value: intentValue},
	}
	mutations = append(
		mutations,
		etcdstore.Mutation{
			Type:  etcdstore.MutationPut,
			Key:   componentTaskActiveEnvironmentKey(environment.Record.ID),
			Value: []byte(task.ID),
		},
	)
	mutations = append(mutations, publication.mutations...)
	taskTenant, err := loadTaskInitiationTenant(ctx, repository.store, project)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	initiation, err := newEnvironmentTaskInitiation(taskTenant, project, environment, taskjournal.TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	baseConditions := append([]etcdstore.Condition(nil), conditions...)
	classify := func(revision int64, values []*etcdstore.KeyValue) error {
		if len(values) == len(baseConditions) {
			activeKey := componentTaskActiveEnvironmentKey(environment.Record.ID)
			for index, condition := range baseConditions {
				if condition.Key == activeKey && !conditionMatchesRead(condition, values[index]) {
					return errs.New(
						errs.KindResourceInUse,
						"Environment component reconciliation is in progress",
					)
				}
			}
		}
		return classifyRouteHeadConflict(baseConditions)(revision, values)
	}
	conditions, mutations, classify, err = configuration.bind(conditions, mutations, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newTaskIdempotencyMutationPlan(
		task,
		initiation,
		conditions,
		mutations,
		classify,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func validateRouteDeletionProjection(
	projection *etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
	intent RouteRemovalIntent,
) error {
	if intent.CurrentProjection == nil {
		if projection != nil {
			return errs.New(errs.KindValidationFailed, "Route deletion projection is unexpected")
		}
		return nil
	}
	if projection == nil || projection.Revision <= 0 || projection.ReadRevision < projection.Revision ||
		projection.Revision != intent.CurrentProjectionRevision ||
		!sameRouteRemovalProjection(projection.Record, *intent.CurrentProjection) {
		return errs.New(errs.KindStateConflict, "Route applied projection changed before deletion")
	}
	return nil
}

func classifyRouteDeletionStartConflict(
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	target etcdstore.Versioned[servicerecord.ServiceRecord],
	route etcdstore.Versioned[routerecord.Record],
	projection *etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
	indexes []*etcdstore.KeyValue,
	operationID string,
) idempotencyPlanClassifier {
	return func(_ int64, values []*etcdstore.KeyValue) error {
		expected := 15
		if project.Record.TenantID != "" {
			expected++
		}
		if projection != nil {
			expected += 2
		}
		if len(values) != expected {
			return errs.New(errs.KindInternal, "Route deletion compare evidence is incomplete")
		}
		if values[2] != nil {
			activeTaskID, err := idempotencyrecord.DecodeTaskReference(values[2].Value)
			if err != nil {
				return err
			}
			return errs.Newf(
				errs.KindStateConflict,
				"operation %s already has active task %s",
				operationID,
				activeTaskID,
			)
		}
		for _, index := range []int{0, 1, 3} {
			if values[index] != nil {
				return errs.New(errs.KindInternal, "Route deletion collided with durable Task state")
			}
		}
		if values[4] == nil {
			return errs.New(errs.KindRouteNotFound, "route was not found")
		}
		if values[4].ModRevision != route.Revision {
			return stateConflict("route", route.Record.Desired.ID)
		}
		for index := 5; index <= 6; index++ {
			if values[index] == nil || string(values[index].Value) != route.Record.Desired.ID {
				return errs.New(errs.KindInternal, "Route deletion index changed or is corrupt")
			}
			if values[index].ModRevision != indexes[index-5].ModRevision {
				return stateConflict("route", route.Record.Desired.ID)
			}
		}
		if values[7] != nil || values[8] != nil {
			return errs.New(errs.KindResourceInUse, "Route deletion is already in progress")
		}
		if values[9] == nil {
			return errs.New(errs.KindEnvironmentNotFound, "environment was not found")
		}
		if values[9].ModRevision != environment.Revision {
			return stateConflict("environment", environment.Record.ID)
		}
		if values[10] == nil {
			return errs.New(errs.KindProjectNotFound, "project was not found")
		}
		if values[10].ModRevision != project.Revision {
			return stateConflict("project", project.Record.ID)
		}
		if values[11] == nil {
			return errs.New(errs.KindServiceNotFound, "target Service was not found")
		}
		if values[11].ModRevision != target.Revision {
			return stateConflict("service", target.Record.Desired.ID)
		}
		position := 12
		hierarchyFenceCount := 3
		if project.Record.TenantID != "" {
			hierarchyFenceCount++
		}
		for index := position; index < position+hierarchyFenceCount; index++ {
			if values[index] != nil {
				return errs.New(errs.KindResourceInUse, "Route hierarchy deletion is in progress")
			}
		}
		position += hierarchyFenceCount
		if projection != nil {
			if values[position] == nil || values[position].ModRevision != projection.Revision {
				return stateConflict("Environment projection", environment.Record.ID)
			}
			position++
			if values[position] != nil {
				return errs.New(errs.KindResourceInUse, "Environment component reconciliation is in progress")
			}
		}
		return errs.New(errs.KindStateConflict, "Route deletion state changed")
	}
}
