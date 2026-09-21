package etcd

import (
	"context"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	recordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// BeginScriptDeletionWithTask atomically fences one Script and publishes the
// Controller Task that owns finalization. The Script remains visible until
// successful acknowledgement.
func (repository *ScriptRepository) BeginScriptDeletionWithTask(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	target etcdstore.Versioned[servicerecord.ServiceRecord],
	current etcdstore.Versioned[scriptrecord.Record],
	tombstone deletionrecord.DeletionTombstoneRecord,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
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
	if err := deletionrecord.ValidateDeletionTombstone(tombstone); err != nil {
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
	if tombstone.TargetKind != deletionrecord.DeletionTargetScript || tombstone.TargetID != scriptID ||
		tombstone.TargetRevision != current.Revision || tombstone.TaskID != task.ID ||
		tombstone.Phase != deletionrecord.DeletionPhaseFinalizing || !tombstone.CreatedAt.Equal(task.CreatedAt) ||
		!tombstone.UpdatedAt.Equal(tombstone.CreatedAt) || task.Executor != taskjournal.TaskExecutorController ||
		task.Type != taskjournal.TaskRemove || task.Target != scriptID || task.Status != taskjournal.TaskStatusPending ||
		len(task.Params) != 1 || task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceScript {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed, "Script deletion Task and tombstone do not match",
		)
	}
	wantReplayTarget := idempotencyrecord.IdempotencyReplayTarget{Kind: idempotencyrecord.IdempotencyReplayTargetScript, ID: scriptID}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerTask || marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment ||
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
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	tombstoneValue, err := deletionrecord.EncodeDeletionTombstone(tombstone)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(tombstoneValue)
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
	activeValue, err := scriptrecord.EncodeScriptSetGeneration(active.Record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(activeValue)

	conditions := []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(task.ID)},
		{Key: taskjournal.TaskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskjournal.TaskActiveOperationKey(task.OperationID)},
		{Key: taskjournal.TaskQueueKey(task.Executor, task.ID)},
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
		{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetScript), scriptID)},
		{Key: hierarchyrecord.EnvironmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{Key: hierarchyrecord.ProjectKey(project.Record.ID), ModRevision: project.Revision},
		servicerecord.ServiceDesiredCondition(target),
		{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetEnvironment), environment.Record.ID)},
		{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetProject), project.Record.ID)},
		{Key: deletionrecord.TombstoneKey("service", target.Record.Desired.ID)},
		{Key: scriptrecord.ScriptSetActiveKey(current.Record.EnvironmentID), ModRevision: active.Revision},
	}
	if project.Record.TenantID != "" {
		conditions = append(conditions, etcdstore.Condition{
			Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetTenant), project.Record.TenantID),
		})
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(task.ID), Value: taskValue},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskActiveOperationKey(task.OperationID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskQueueKey(task.Executor, task.ID), Value: reference},
		{
			Type: etcdstore.MutationPut, Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetScript), scriptID),
			Value: tombstoneValue,
		},
		{Type: etcdstore.MutationPut, Key: scriptrecord.ScriptSetActiveKey(current.Record.EnvironmentID), Value: activeValue},
	}
	taskTenant, err := loadTaskInitiationTenant(ctx, repository.store, project)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	initiation, err := newEnvironmentTaskInitiation(taskTenant, project, environment, taskjournal.TaskActorOperator)
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
	idempotency, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func classifyScriptDeletionStartConflict(
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	target etcdstore.Versioned[servicerecord.ServiceRecord],
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
			return recordcodec.StateConflict("script", current.Record.Desired.ID)
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
			return recordcodec.StateConflict("environment", environment.Record.ID)
		}
		if values[9] == nil {
			return errs.New(errs.KindProjectNotFound, "Project was not found")
		}
		if values[9].ModRevision != project.Revision {
			return recordcodec.StateConflict("project", project.Record.ID)
		}
		if values[10] == nil {
			return errs.New(errs.KindServiceNotFound, "target Service was not found")
		}
		if values[10].ModRevision != target.Revision {
			return recordcodec.StateConflict("service", target.Record.Desired.ID)
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
