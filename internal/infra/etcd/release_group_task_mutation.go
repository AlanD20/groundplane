package etcd

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReleaseGroupPreparedMutation is an opaque, immutable transaction fragment.
// Only the etcd Release Group adapter can prepare a non-zero value.
type ReleaseGroupPreparedMutation struct {
	environmentID string
	groupID       string
	groupRevision int64
	taskType      TaskType
	conditions    []etcdstore.Condition
	mutations     []etcdstore.Mutation
}

// ReleaseGroupBlueprintPreparedMutation is an opaque, immutable Release Group
// projection fragment for Environment desired-revision publication.
type ReleaseGroupBlueprintPreparedMutation struct {
	environmentID string
	conditions    []etcdstore.Condition
	mutations     []etcdstore.Mutation
}

func (prepared ReleaseGroupBlueprintPreparedMutation) isZero() bool {
	return prepared.environmentID == "" && len(prepared.conditions) == 0 && len(prepared.mutations) == 0
}

func newReleaseGroupPreparedMutation(
	environmentID string,
	groupID string,
	groupRevision int64,
	taskType TaskType,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) ReleaseGroupPreparedMutation {
	return ReleaseGroupPreparedMutation{
		environmentID: environmentID,
		groupID:       groupID,
		groupRevision: groupRevision,
		taskType:      taskType,
		conditions:    cloneReleaseGroupConditions(conditions),
		mutations:     cloneReleaseGroupMutations(mutations),
	}
}

func newReleaseGroupBlueprintPreparedMutation(
	environmentID string,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) ReleaseGroupBlueprintPreparedMutation {
	return ReleaseGroupBlueprintPreparedMutation{
		environmentID: environmentID,
		conditions:    cloneReleaseGroupConditions(conditions),
		mutations:     cloneReleaseGroupMutations(mutations),
	}
}

func (repository *TaskRepository) PublishReleaseGroupDirectMutation(
	ctx context.Context, prepared ReleaseGroupPreparedMutation, marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if prepared.taskType != TaskCreate && prepared.taskType != TaskUpdate {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"release group direct mutation type is invalid",
		)
	}
	if ids.Validate(ids.KindEnvironment, prepared.environmentID) != nil ||
		ids.Validate(ids.KindReleaseGroup, prepared.groupID) != nil {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"release group direct mutation identity is invalid",
		)
	}
	if marker.Kind != IdempotencyMarkerDirect || marker.State != IdempotencyMarkerCompleted ||
		marker.Locator.ScopeKind != IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != prepared.environmentID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"release group direct marker is invalid",
		)
	}
	if err := validateReleaseGroupPreparedFragment(prepared); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newIdempotencyMutationPlan(
		cloneReleaseGroupConditions(prepared.conditions),
		cloneReleaseGroupMutations(prepared.mutations),
		func(_ int64, _ []*etcdstore.KeyValue) error {
			return errs.New(errs.KindStateConflict, "release group mutation evidence changed")
		},
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

func (repository *TaskRepository) PublishReleaseGroupMutation(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
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
	if environment.Record.ID != prepared.environmentID || project.Record.ID != environment.Record.ProjectID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindScopeUnauthorized,
			"release group mutation hierarchy is inconsistent",
		)
	}
	if marker.Kind != IdempotencyMarkerTask || marker.State != IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.ScopeKind != IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != environment.Record.ID || !marker.CreatedAt.Equal(task.CreatedAt) ||
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
	conditions := cloneReleaseGroupConditions(prepared.conditions)
	conditions = append(conditions,
		etcdstore.Condition{Key: taskKey(task.ID)},
		etcdstore.Condition{Key: taskOperationIndexKey(task.OperationID, task.ID)},
		etcdstore.Condition{Key: taskActiveOperationKey(task.OperationID)},
		etcdstore.Condition{Key: taskQueueKey(task.Executor, task.ID)},
	)
	mutations := cloneReleaseGroupMutations(prepared.mutations)
	mutations = append(mutations,
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: taskKey(task.ID), Value: taskValue},
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: reference},
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: reference},
	)
	if prepared.taskType == TaskRemove {
		tombstone := DeletionTombstoneRecord{
			TargetKind: DeletionTargetReleaseGroup, TargetID: prepared.groupID,
			TargetRevision: prepared.groupRevision, TaskID: task.ID,
			Phase: DeletionPhaseFinalizing, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt,
		}
		value, err := encodeDeletionTombstone(tombstone)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		defer clear(value)
		mutations = append(mutations, etcdstore.Mutation{
			Type:  etcdstore.MutationPut,
			Key:   deletionTombstoneKey(string(DeletionTargetReleaseGroup), prepared.groupID),
			Value: value,
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
	plan, err := newTaskIdempotencyMutationPlan(
		task, initiation, conditions, mutations,
		func(_ int64, _ []*etcdstore.KeyValue) error {
			return errs.New(errs.KindStateConflict, "release group mutation evidence changed")
		},
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

func validateReleaseGroupPreparedMutation(prepared ReleaseGroupPreparedMutation, task TaskRecord) error {
	if ids.Validate(ids.KindEnvironment, prepared.environmentID) != nil ||
		ids.Validate(ids.KindReleaseGroup, prepared.groupID) != nil || prepared.taskType != task.Type ||
		task.Executor != TaskExecutorController || task.Target != prepared.groupID ||
		task.Status != TaskStatusPending || task.Params[TaskResourceKindParam] != TaskResourceReleaseGroup ||
		len(task.Params) != 1 ||
		(prepared.taskType != TaskCreate && prepared.taskType != TaskUpdate && prepared.taskType != TaskRemove) {
		return errs.New(errs.KindValidationFailed, "release group prepared mutation is invalid")
	}
	if prepared.taskType == TaskRemove && prepared.groupRevision <= 0 {
		return errs.New(errs.KindValidationFailed, "release group removal revision is invalid")
	}
	if err := validateReleaseGroupPreparedFragment(prepared); err != nil {
		return err
	}
	for _, mutation := range prepared.mutations {
		if prepared.taskType == TaskRemove && strings.HasPrefix(mutation.Key, releaseGroupRecordPrefix) {
			return errs.New(errs.KindInternal, "release group removal preparation contains a primary mutation")
		}
	}
	return nil
}

func validateReleaseGroupPreparedFragment(prepared ReleaseGroupPreparedMutation) error {
	if len(prepared.mutations) == 0 {
		return errs.New(errs.KindInternal, "release group mutation handoff is empty")
	}
	for _, condition := range prepared.conditions {
		if condition.Prefix {
			return errs.New(errs.KindInternal, "release group mutation contains open-ended compare authority")
		}
	}
	for _, mutation := range prepared.mutations {
		if mutation.Prefix {
			return errs.New(errs.KindInternal, "release group mutation contains open-ended write authority")
		}
	}
	return nil
}

func validateReleaseGroupBlueprintPreparedMutation(
	prepared ReleaseGroupBlueprintPreparedMutation,
	environmentID string,
) error {
	if prepared.isZero() {
		return nil
	}
	if prepared.environmentID != environmentID {
		return errs.New(errs.KindInternal, "Blueprint Release Group mutation handoff scope is invalid")
	}
	for _, condition := range prepared.conditions {
		if condition.Prefix {
			return errs.New(errs.KindInternal, "Blueprint Release Group mutation contains open-ended compare authority")
		}
	}
	for _, mutation := range prepared.mutations {
		if mutation.Prefix {
			return errs.New(errs.KindInternal, "Blueprint Release Group mutation contains open-ended write authority")
		}
	}
	return nil
}

func cloneReleaseGroupConditions(conditions []etcdstore.Condition) []etcdstore.Condition {
	return append([]etcdstore.Condition(nil), conditions...)
}

func cloneReleaseGroupMutations(mutations []etcdstore.Mutation) []etcdstore.Mutation {
	cloned := make([]etcdstore.Mutation, len(mutations))
	for index := range mutations {
		cloned[index] = mutations[index]
		cloned[index].Value = append([]byte(nil), mutations[index].Value...)
	}
	return cloned
}
