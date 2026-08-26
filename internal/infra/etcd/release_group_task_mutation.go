package etcd

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReleaseGroupPreparedMutation is the closed handoff from the Release Group
// record adapter to the Task/idempotency repository. Only Release Group keys
// and the owning Environment epoch may be mutated through this seam.
type ReleaseGroupPreparedMutation struct {
	EnvironmentID string
	GroupID       string
	GroupRevision int64
	Type          TaskType
	Conditions    []Condition
	Mutations     []Mutation
}

func (repository *TaskRepository) PublishReleaseGroupDirectMutation(
	ctx context.Context, prepared ReleaseGroupPreparedMutation, marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if prepared.Type != TaskCreate && prepared.Type != TaskUpdate {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "release group direct mutation type is invalid")
	}
	if ids.Validate(ids.KindEnvironment, prepared.EnvironmentID) != nil || ids.Validate(ids.KindReleaseGroup, prepared.GroupID) != nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "release group direct mutation identity is invalid")
	}
	if marker.Kind != IdempotencyMarkerDirect || marker.State != IdempotencyMarkerCompleted ||
		marker.Locator.ScopeKind != IdempotencyScopeEnvironment || marker.Locator.ScopeID != prepared.EnvironmentID {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "release group direct marker is invalid")
	}
	for _, condition := range prepared.Conditions {
		if !validReleaseGroupMutationKey(prepared, condition.Key) {
			return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "release group mutation compare escaped its keyspace")
		}
	}
	for _, mutation := range prepared.Mutations {
		if !validReleaseGroupMutationKey(prepared, mutation.Key) {
			return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "release group mutation escaped its keyspace")
		}
	}
	plan, err := newIdempotencyMutationPlan(prepared.Conditions, prepared.Mutations, func(_ int64, _ []*KeyValue) error {
		return errs.New(errs.KindStateConflict, "release group mutation evidence changed")
	})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func (repository *TaskRepository) PublishReleaseGroupMutation(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	prepared ReleaseGroupPreparedMutation,
	task TaskRecord,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateReleaseGroupPreparedMutation(prepared, task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if environment.Record.ID != prepared.EnvironmentID || project.Record.ID != environment.Record.ProjectID {
		return IdempotencyTransactionResult{}, errs.New(errs.KindScopeUnauthorized, "release group mutation hierarchy is inconsistent")
	}
	if marker.Kind != IdempotencyMarkerTask || marker.State != IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.ScopeKind != IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != environment.Record.ID || !marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "release group mutation marker does not match its task")
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
	conditions := append([]Condition(nil), prepared.Conditions...)
	conditions = append(conditions,
		Condition{Key: taskKey(task.ID)},
		Condition{Key: taskOperationIndexKey(task.OperationID, task.ID)},
		Condition{Key: taskActiveOperationKey(task.OperationID)},
		Condition{Key: taskQueueKey(task.Executor, task.ID)},
	)
	mutations := make([]Mutation, len(prepared.Mutations))
	for index, mutation := range prepared.Mutations {
		mutations[index] = mutation
		mutations[index].Value = append([]byte(nil), mutation.Value...)
	}
	mutations = append(mutations,
		Mutation{Type: MutationPut, Key: taskKey(task.ID), Value: taskValue},
		Mutation{Type: MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		Mutation{Type: MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: reference},
		Mutation{Type: MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: reference},
	)
	if prepared.Type == TaskRemove {
		tombstone := DeletionTombstoneRecord{
			TargetKind: DeletionTargetReleaseGroup, TargetID: prepared.GroupID,
			TargetRevision: prepared.GroupRevision, TaskID: task.ID,
			Phase: DeletionPhaseFinalizing, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt,
		}
		value, err := encodeDeletionTombstone(tombstone)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		defer clear(value)
		mutations = append(mutations, Mutation{
			Type: MutationPut, Key: deletionTombstoneKey(string(DeletionTargetReleaseGroup), prepared.GroupID), Value: value,
		})
	}
	tenant, err := loadTaskInitiationTenant(ctx, repository.store, project)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	initiation, err := newEnvironmentTaskInitiation(tenant, project, environment, TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newTaskIdempotencyMutationPlan(task, initiation, conditions, mutations,
		func(_ int64, _ []*KeyValue) error {
			return errs.New(errs.KindStateConflict, "release group mutation evidence changed")
		})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func validateReleaseGroupPreparedMutation(prepared ReleaseGroupPreparedMutation, task TaskRecord) error {
	if ids.Validate(ids.KindEnvironment, prepared.EnvironmentID) != nil ||
		ids.Validate(ids.KindReleaseGroup, prepared.GroupID) != nil || prepared.Type != task.Type ||
		task.Executor != TaskExecutorController || task.Target != prepared.GroupID ||
		task.Status != TaskStatusPending || task.Params[TaskResourceKindParam] != TaskResourceReleaseGroup ||
		len(task.Params) != 1 || (prepared.Type != TaskCreate && prepared.Type != TaskUpdate && prepared.Type != TaskRemove) {
		return errs.New(errs.KindValidationFailed, "release group prepared mutation is invalid")
	}
	if prepared.Type == TaskRemove && prepared.GroupRevision <= 0 {
		return errs.New(errs.KindValidationFailed, "release group removal revision is invalid")
	}
	for _, condition := range prepared.Conditions {
		if !validReleaseGroupMutationKey(prepared, condition.Key) {
			return errs.New(errs.KindInternal, "release group mutation compare escaped its keyspace")
		}
	}
	for _, mutation := range prepared.Mutations {
		if !validReleaseGroupMutationKey(prepared, mutation.Key) ||
			(prepared.Type == TaskRemove && strings.HasPrefix(mutation.Key, "/v1/records/release-groups/")) {
			return errs.New(errs.KindInternal, "release group mutation escaped its keyspace")
		}
	}
	return nil
}

func validReleaseGroupMutationKey(prepared ReleaseGroupPreparedMutation, key string) bool {
	if key == environmentMutationEpochKey(prepared.EnvironmentID) ||
		key == environmentKey(prepared.EnvironmentID) ||
		key == environmentOperationLockKey(prepared.EnvironmentID) ||
		key == deletionTombstoneKey(string(DeletionTargetEnvironment), prepared.EnvironmentID) ||
		key == deletionTombstoneKey(string(DeletionTargetReleaseGroup), prepared.GroupID) {
		return true
	}
	allowedPrefixes := []string{
		"/v1/records/release-groups/", "/v1/indexes/release-groups/", "/v1/records/projects/",
		"/v1/records/tenants/", "/v1/records/services/", "/v1/indexes/services/",
		"/v1/indexes/projects/", "/v1/indexes/environments/", "/v1/records/environment-compose-projections/",
		"/v1/runtime/deletions/project/", "/v1/runtime/deletions/tenant/", "/v1/runtime/deletions/service/",
	}
	for _, prefix := range allowedPrefixes {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}
