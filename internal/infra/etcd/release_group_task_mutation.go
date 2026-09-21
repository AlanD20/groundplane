package etcd

import (
	"context"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	groupstore "github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) PublishReleaseGroupDirectMutation(
	ctx context.Context, prepared groupstore.ReleaseGroupPreparedMutation, marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if prepared.TaskType() != taskjournal.TaskCreate && prepared.TaskType() != taskjournal.TaskUpdate {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"release group direct mutation type is invalid",
		)
	}
	if ids.Validate(ids.KindEnvironment, prepared.EnvironmentID()) != nil ||
		ids.Validate(ids.KindReleaseGroup, prepared.GroupID()) != nil {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"release group direct mutation identity is invalid",
		)
	}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerDirect ||
		marker.State != idempotencyrecord.IdempotencyMarkerCompleted ||
		marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != prepared.EnvironmentID() {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"release group direct marker is invalid",
		)
	}
	if err := groupstore.ValidateReleaseGroupPreparedFragment(prepared); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := NewIdempotencyMutationPlan(
		prepared.Conditions(),
		prepared.Mutations(),
		func(_ int64, _ []*etcdstore.KeyValue) error {
			return errs.New(errs.KindStateConflict, "release group mutation evidence changed")
		},
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

func (repository *TaskRepository) PublishReleaseGroupMutation(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	prepared groupstore.ReleaseGroupPreparedMutation,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateReleaseGroupPreparedMutation(prepared, task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if environment.Record.ID != prepared.EnvironmentID() || project.Record.ID != environment.Record.ProjectID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindScopeUnauthorized,
			"release group mutation hierarchy is inconsistent",
		)
	}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerTask ||
		marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		marker.TaskID != task.ID ||
		marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != environment.Record.ID ||
		!marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"release group mutation marker does not match its task",
		)
	}
	task = cloneTaskRecord(task)
	if task.IdempotencyKey == "" {
		task.IdempotencyKey = marker.Locator.Key
	}
	task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	if err := ValidateTaskRecord(task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	taskValue, err := EncodeTaskRecord(task)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	reference, err := idempotencyrecord.EncodeTaskReference(task.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(reference)
	conditions := prepared.Conditions()
	conditions = append(conditions,
		etcdstore.Condition{Key: taskjournal.TaskStorageKey(task.ID)},
		etcdstore.Condition{Key: taskjournal.TaskOperationIndexKey(task.OperationID, task.ID)},
		etcdstore.Condition{Key: taskjournal.TaskActiveOperationKey(task.OperationID)},
		etcdstore.Condition{Key: taskjournal.TaskQueueKey(task.Executor, task.ID)},
	)
	mutations := prepared.Mutations()
	mutations = append(
		mutations,
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(task.ID), Value: taskValue},
		etcdstore.Mutation{
			Type:  etcdstore.MutationPut,
			Key:   taskjournal.TaskOperationIndexKey(task.OperationID, task.ID),
			Value: reference,
		},
		etcdstore.Mutation{
			Type:  etcdstore.MutationPut,
			Key:   taskjournal.TaskActiveOperationKey(task.OperationID),
			Value: reference,
		},
		etcdstore.Mutation{
			Type:  etcdstore.MutationPut,
			Key:   taskjournal.TaskQueueKey(task.Executor, task.ID),
			Value: reference,
		},
	)
	if prepared.TaskType() == taskjournal.TaskRemove {
		tombstone := deletionrecord.DeletionTombstoneRecord{
			TargetKind: deletionrecord.DeletionTargetReleaseGroup, TargetID: prepared.GroupID(),
			TargetRevision: prepared.GroupRevision(), TaskID: task.ID,
			Phase: deletionrecord.DeletionPhaseFinalizing, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt,
		}
		value, err := deletionrecord.EncodeDeletionTombstone(tombstone)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		defer clear(value)
		mutations = append(mutations, etcdstore.Mutation{
			Type:  etcdstore.MutationPut,
			Key:   deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetReleaseGroup), prepared.GroupID()),
			Value: value,
		})
	}
	tenant, err := loadTaskInitiationTenant(ctx, repository.store, project)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	initiation, err := newEnvironmentTaskInitiation(tenant, project, environment, taskjournal.TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newTaskIdempotencyMutationPlan(
		task, initiation, conditions, mutations,
		func(_ int64, _ []*etcdstore.KeyValue) error {
			return errs.New(errs.KindStateConflict, "release group mutation evidence changed")
		},
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

func validateReleaseGroupPreparedMutation(prepared groupstore.ReleaseGroupPreparedMutation, task TaskRecord) error {
	if ids.Validate(ids.KindEnvironment, prepared.EnvironmentID()) != nil ||
		ids.Validate(ids.KindReleaseGroup, prepared.GroupID()) != nil || prepared.TaskType() != task.Type ||
		task.Executor != taskjournal.TaskExecutorController || task.Target != prepared.GroupID() ||
		task.Status != taskjournal.TaskStatusPending || task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceReleaseGroup ||
		len(task.Params) != 1 ||
		(prepared.TaskType() != taskjournal.TaskCreate && prepared.TaskType() != taskjournal.TaskUpdate && prepared.TaskType() != taskjournal.TaskRemove) {
		return errs.New(errs.KindValidationFailed, "release group prepared mutation is invalid")
	}
	if prepared.TaskType() == taskjournal.TaskRemove && prepared.GroupRevision() <= 0 {
		return errs.New(errs.KindValidationFailed, "release group removal revision is invalid")
	}
	if err := groupstore.ValidateReleaseGroupPreparedFragment(prepared); err != nil {
		return err
	}
	return prepared.ValidateRemovalPrimaryMutations()
}
