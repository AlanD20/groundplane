package etcd

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// BeginScriptDeletionWithTask atomically fences one Script and publishes the
// Controller Task that owns finalization. The Script remains visible until
// successful acknowledgement.
func (repository *ScriptRepository) BeginScriptDeletionWithTask(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	target etcdstore.Versioned[ServiceRecord],
	current etcdstore.Versioned[scriptrecord.Record],
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
	if current.Record.ActiveReferences != 0 {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindResourceInUse, "active Script executions fence deletion",
		)
	}
	if err := validateDeletionTombstone(tombstone); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	scriptID := current.Record.Desired.ID
	active, err := readActiveScriptSet(ctx, repository.store, current.Record.EnvironmentID, current.ReadRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if active.Record.GenerationID != current.Record.ScriptSetGeneration {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "Script-set generation changed")
	}
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

	indexes, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			scriptrecord.ScriptSetOwnerKey(current.Record.EnvironmentID, active.Record.GenerationID, scriptID),
			scriptrecord.ScriptSetSlugKey(current.Record.EnvironmentID, active.Record.GenerationID, current.Record.Desired.Slug),
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
	activeValue, err := scriptrecord.EncodeScriptSetGeneration(active.Record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(activeValue)

	conditions := []etcdstore.Condition{
		{Key: taskKey(task.ID)},
		{Key: taskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskActiveOperationKey(task.OperationID)},
		{Key: taskQueueKey(task.Executor, task.ID)},
		{
			Key:         scriptrecord.ScriptSetScriptKey(current.Record.EnvironmentID, active.Record.GenerationID, scriptID),
			ModRevision: current.Revision,
		},
		{
			Key:         scriptrecord.ScriptSetOwnerKey(current.Record.EnvironmentID, active.Record.GenerationID, scriptID),
			ModRevision: indexes.Values[0].ModRevision,
		},
		{
			Key: scriptrecord.ScriptSetSlugKey(
				current.Record.EnvironmentID,
				active.Record.GenerationID,
				current.Record.Desired.Slug,
			),
			ModRevision: indexes.Values[1].ModRevision,
		},
		{Key: deletionTombstoneKey(string(DeletionTargetScript), scriptID)},
		{Key: hierarchyrecord.EnvironmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{Key: hierarchyrecord.ProjectKey(project.Record.ID), ModRevision: project.Revision},
		serviceDesiredCondition(target),
		{Key: deletionTombstoneKey(string(DeletionTargetEnvironment), environment.Record.ID)},
		{Key: deletionTombstoneKey(string(DeletionTargetProject), project.Record.ID)},
		{Key: deletionTombstoneKey("service", target.Record.Desired.ID)},
		{Key: scriptrecord.ScriptSetActiveKey(current.Record.EnvironmentID), ModRevision: active.Revision},
	}
	if project.Record.TenantID != "" {
		conditions = append(conditions, etcdstore.Condition{
			Key: deletionTombstoneKey(string(DeletionTargetTenant), project.Record.TenantID),
		})
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{Type: etcdstore.MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: reference},
		{
			Type: etcdstore.MutationPut, Key: deletionTombstoneKey(string(DeletionTargetScript), scriptID),
			Value: tombstoneValue,
		},
		{Type: etcdstore.MutationPut, Key: scriptrecord.ScriptSetActiveKey(current.Record.EnvironmentID), Value: activeValue},
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
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	target etcdstore.Versioned[ServiceRecord],
	current etcdstore.Versioned[scriptrecord.Record],
	operationID string,
) idempotencyPlanClassifier {
	return func(_ int64, values []*etcdstore.KeyValue) error {
		expected := 15
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
			changed, err := scriptrecord.DecodeRecord(values[4].Value)
			if err != nil {
				return err
			}
			if changed.ActiveReferences != 0 {
				return errs.New(errs.KindResourceInUse, "active Script executions fence deletion")
			}
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
		for index := 11; index <= 13; index++ {
			if values[index] != nil {
				return errs.New(errs.KindResourceInUse, "Script hierarchy deletion is in progress")
			}
		}
		if values[14] == nil {
			return errs.New(errs.KindInternal, "Environment active Script-set generation is missing")
		}
		if project.Record.TenantID != "" && values[15] != nil {
			return errs.New(errs.KindResourceInUse, "Script hierarchy deletion is in progress")
		}
		return errs.New(errs.KindStateConflict, "Script deletion state changed")
	}
}
