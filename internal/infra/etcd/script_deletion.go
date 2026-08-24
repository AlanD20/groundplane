package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// BeginScriptDeletionWithTask atomically fences one Script and publishes the
// Controller Task that owns finalization. The Script remains visible until
// successful acknowledgement.
func (repository *ScriptRepository) BeginScriptDeletionWithTask(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	target Versioned[ServiceRecord],
	current Versioned[ScriptRecord],
	tombstone DeletionTombstoneRecord,
	task TaskRecord,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateScriptHierarchy(ctx, environment, project, target, current.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateScriptVersion(current); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateDeletionTombstone(tombstone); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	scriptID := current.Record.Desired.ID
	if tombstone.TargetKind != DeletionTargetScript || tombstone.TargetID != scriptID ||
		tombstone.TargetRevision != current.Revision || tombstone.TaskID != task.ID ||
		tombstone.Phase != DeletionPhaseFinalizing || !tombstone.CreatedAt.Equal(task.CreatedAt) ||
		!tombstone.UpdatedAt.Equal(tombstone.CreatedAt) || task.Executor != TaskExecutorController ||
		task.Type != TaskRemove || task.Target != scriptID || task.Status != TaskStatusPending ||
		len(task.Params) != 1 || task.Params[TaskResourceKindParam] != TaskResourceScript {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed, "Script deletion Task and tombstone do not match",
		)
	}
	wantReplayTarget := IdempotencyReplayTarget{Kind: IdempotencyReplayTargetScript, ID: scriptID}
	if marker.Kind != IdempotencyMarkerTask || marker.State != IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.ScopeKind != IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != environment.Record.ID || marker.ReplayTarget == nil ||
		*marker.ReplayTarget != wantReplayTarget || !marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed, "Script deletion marker does not match its Task",
		)
	}

	indexes, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			scriptOwnerKey(current.Record.EnvironmentID, scriptID),
			scriptNameKey(current.Record.EnvironmentID, current.Record.Desired.Name),
		},
		Revision: current.ReadRevision,
	})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if indexes == nil || len(indexes.Values) != 2 || indexes.Values[0] == nil || indexes.Values[1] == nil ||
		string(indexes.Values[0].Value) != scriptID || string(indexes.Values[1].Value) != scriptID {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "Script deletion indexes are corrupt")
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

	conditions := []Condition{
		{Key: taskKey(task.ID)},
		{Key: taskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskActiveOperationKey(task.OperationID)},
		{Key: taskQueueKey(task.Executor, task.ID)},
		{Key: scriptKey(scriptID), ModRevision: current.Revision},
		{Key: scriptOwnerKey(current.Record.EnvironmentID, scriptID), ModRevision: indexes.Values[0].ModRevision},
		{
			Key:         scriptNameKey(current.Record.EnvironmentID, current.Record.Desired.Name),
			ModRevision: indexes.Values[1].ModRevision,
		},
		{Key: deletionTombstoneKey(string(DeletionTargetScript), scriptID)},
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
	mutations := []Mutation{
		{Type: MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{Type: MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		{Type: MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: reference},
		{Type: MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: reference},
		{
			Type: MutationPut, Key: deletionTombstoneKey(string(DeletionTargetScript), scriptID),
			Value: tombstoneValue,
		},
	}
	taskTenant, err := loadTaskInitiationTenant(ctx, repository.store, project)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	initiation, err := newEnvironmentTaskInitiation(taskTenant, project, environment, TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newTaskIdempotencyMutationPlan(
		task, initiation, conditions, mutations,
		classifyScriptDeletionStartConflict(environment, project, target, current, task.OperationID),
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

func classifyScriptDeletionStartConflict(
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	target Versioned[ServiceRecord],
	current Versioned[ScriptRecord],
	operationID string,
) idempotencyPlanClassifier {
	return func(_ int64, values []*KeyValue) error {
		expected := 14
		if project.Record.TenantID != "" {
			expected++
		}
		if len(values) != expected {
			return errs.New(errs.KindInternal, "Script deletion compare evidence is incomplete")
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
				return errs.New(errs.KindInternal, "Script deletion collided with durable Task state")
			}
		}
		if values[4] == nil {
			return errs.New(errs.KindScriptNotFound, "Script was not found")
		}
		if values[4].ModRevision != current.Revision {
			return stateConflict("script", current.Record.Desired.ID)
		}
		for _, index := range []int{5, 6} {
			if values[index] == nil || string(values[index].Value) != current.Record.Desired.ID {
				return errs.New(errs.KindInternal, "Script deletion index changed or is corrupt")
			}
		}
		if values[7] != nil {
			return errs.New(errs.KindResourceInUse, "Script deletion is already in progress")
		}
		if values[8] == nil {
			return errs.New(errs.KindEnvironmentNotFound, "Environment was not found")
		}
		if values[8].ModRevision != environment.Revision {
			return stateConflict("environment", environment.Record.ID)
		}
		if values[9] == nil {
			return errs.New(errs.KindProjectNotFound, "Project was not found")
		}
		if values[9].ModRevision != project.Revision {
			return stateConflict("project", project.Record.ID)
		}
		if values[10] == nil {
			return errs.New(errs.KindServiceNotFound, "target Service was not found")
		}
		if values[10].ModRevision != target.Revision {
			return stateConflict("service", target.Record.Desired.ID)
		}
		for index := 11; index < len(values); index++ {
			if values[index] != nil {
				return errs.New(errs.KindResourceInUse, "Script hierarchy deletion is in progress")
			}
		}
		return errs.New(errs.KindStateConflict, "Script deletion state changed")
	}
}
