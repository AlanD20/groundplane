package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// BeginRouteDeletionWithTask atomically fences one Route, pins its exact
// applied-projection candidate, and publishes the Task that owns finalization.
// The Route and its indexes remain visible until successful acknowledgement.
func (repository *RouteRepository) BeginRouteDeletionWithTask(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	target Versioned[ServiceRecord],
	route Versioned[RouteRecord],
	projection *Versioned[EnvironmentComposeProjection],
	tombstone DeletionTombstoneRecord,
	intent RouteRemovalIntent,
	task TaskRecord,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateRouteHierarchy(ctx, environment, project, target, route.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateRouteVersion(route); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateDeletionTombstone(tombstone); err != nil {
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
	if tombstone.TargetKind != DeletionTargetRoute || tombstone.TargetID != route.Record.Desired.ID ||
		tombstone.TargetRevision != route.Revision || tombstone.TaskID != task.ID ||
		tombstone.Phase != routeRemovalTombstonePhase(intent) || !tombstone.CreatedAt.Equal(task.CreatedAt) ||
		!tombstone.UpdatedAt.Equal(tombstone.CreatedAt) || intent.TaskID != task.ID ||
		intent.EnvironmentID != environment.Record.ID || intent.RouteID != route.Record.Desired.ID ||
		intent.RouteRevision != route.Revision || intent.Status != TaskStatusPending ||
		!intent.CreatedAt.Equal(task.CreatedAt) || intent.TerminalAt != nil || task.Type != TaskRemove ||
		task.Target != route.Record.Desired.ID || task.Status != TaskStatusPending {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Route deletion Task, intent, and tombstone do not match",
		)
	}
	wantReplayTarget := IdempotencyReplayTarget{Kind: IdempotencyReplayTargetRoute, ID: route.Record.Desired.ID}
	if marker.Kind != IdempotencyMarkerTask || marker.State != IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.ScopeKind != IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != environment.Record.ID || marker.ReplayTarget == nil ||
		*marker.ReplayTarget != wantReplayTarget || !marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Route deletion marker does not match its Task",
		)
	}

	indexes, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			routeOwnerKey(route.Record.EnvironmentID, route.Record.Desired.ID),
			routeMatchKey(route.Record.EnvironmentID, route.Record.Desired.Host, route.Record.Desired.Path),
		},
		Revision: route.ReadRevision,
	})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if indexes == nil || len(indexes.Values) != 2 || indexes.Values[0] == nil || indexes.Values[1] == nil ||
		string(indexes.Values[0].Value) != route.Record.Desired.ID ||
		string(indexes.Values[1].Value) != route.Record.Desired.ID {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "Route deletion indexes are corrupt")
	}

	task = cloneTaskRecord(task)
	if task.IdempotencyKey == "" {
		task.IdempotencyKey = marker.Locator.Key
	}
	task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	if err := validateTaskRecord(task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	tombstoneValue, err := encodeDeletionTombstone(tombstone)
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
	reference, err := encodeTaskReference(task.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(reference)

	tombstoneKey := deletionTombstoneKey(string(DeletionTargetRoute), route.Record.Desired.ID)
	conditions := []Condition{
		{Key: taskKey(task.ID)},
		{Key: taskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskActiveOperationKey(task.OperationID)},
		{Key: taskQueueKey(task.Executor, task.ID)},
		{Key: routeKey(route.Record.Desired.ID), ModRevision: route.Revision},
		{
			Key:         routeOwnerKey(route.Record.EnvironmentID, route.Record.Desired.ID),
			ModRevision: indexes.Values[0].ModRevision,
		},
		{
			Key: routeMatchKey(
				route.Record.EnvironmentID,
				route.Record.Desired.Host,
				route.Record.Desired.Path,
			),
			ModRevision: indexes.Values[1].ModRevision,
		},
		{Key: routeRemovalIntentKey(task.ID)},
		{Key: tombstoneKey},
		{Key: environmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{Key: projectKey(project.Record.ID), ModRevision: project.Revision},
		{Key: serviceKey(target.Record.Desired.ID), ModRevision: target.Revision},
		{Key: deletionTombstoneKey(string(DeletionTargetEnvironment), environment.Record.ID)},
		{Key: deletionTombstoneKey(string(DeletionTargetProject), project.Record.ID)},
		{Key: deletionTombstoneKey("service", target.Record.Desired.ID)},
	}
	if project.Record.TenantID != "" {
		conditions = append(conditions, Condition{
			Key: deletionTombstoneKey(string(DeletionTargetTenant), project.Record.TenantID),
		})
	}
	if projection != nil {
		conditions = append(conditions,
			Condition{Key: environmentComposeProjectionKey(environment.Record.ID), ModRevision: projection.Revision},
			Condition{Key: componentTaskActiveEnvironmentKey(environment.Record.ID)},
		)
	}
	mutations := []Mutation{
		{Type: MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{Type: MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		{Type: MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: reference},
		{Type: MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: reference},
		{Type: MutationPut, Key: tombstoneKey, Value: tombstoneValue},
		{Type: MutationPut, Key: routeRemovalIntentKey(task.ID), Value: intentValue},
	}
	if projection != nil {
		mutations = append(mutations, Mutation{
			Type: MutationPut, Key: componentTaskActiveEnvironmentKey(environment.Record.ID), Value: []byte(task.ID),
		})
	}
	plan, err := newTaskIdempotencyMutationPlan(
		conditions,
		mutations,
		classifyRouteDeletionStartConflict(
			environment, project, target, route, projection, indexes.Values, task.OperationID,
		),
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
	projection *Versioned[EnvironmentComposeProjection],
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
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	target Versioned[ServiceRecord],
	route Versioned[RouteRecord],
	projection *Versioned[EnvironmentComposeProjection],
	indexes []*KeyValue,
	operationID string,
) idempotencyPlanClassifier {
	return func(_ int64, values []*KeyValue) error {
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
			activeTaskID, err := decodeTaskReference(values[2].Value)
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
