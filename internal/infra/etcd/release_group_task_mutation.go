package etcd

import (
	"context"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	groupstore "github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) PublishReleaseGroupDirectMutation(
	ctx context.Context, prepared groupstore.ReleaseGroupPreparedMutation, marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := groupstore.ValidatePreparedDirect(prepared, marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	authoring, err := groupstore.PrepareDesiredAuthoring(ctx, repository.store, prepared)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	desired, err := prepareDirectDesiredProjectionPublication(
		ctx, repository.store, prepared.EnvironmentID(), marker,
		authoring.Apply,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clearDesiredMutationFragment(desired)
	conditions := prepared.Conditions()
	mutations := prepared.Mutations()
	conditions, mutations, classify, err := desired.bindDirectDesired(
		conditions,
		mutations,
		func(_ int64, _ []*etcdstore.KeyValue) error {
			return errs.New(errs.KindStateConflict, "release group mutation evidence changed")
		},
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := NewIdempotencyMutationPlan(
		conditions,
		mutations,
		classify,
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

// PublishReleaseGroupRemoval commits desired authoring and native execution together.
func (repository *TaskRepository) PublishReleaseGroupRemoval(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	prepared groupstore.ReleaseGroupPreparedMutation,
	input groupstore.RemovalInput, marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := groupstore.ValidatePreparedRemoval(prepared, input); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if environment.Record.ID != input.Scope.EnvironmentID || project.Record.ID != environment.Record.ProjectID ||
		project.Record.ID != input.Scope.ProjectID || project.Record.TenantID != input.Scope.TenantID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindScopeUnauthorized,
			"Release Group removal hierarchy changed",
		)
	}
	task, item, err := input.Work()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	work, err := workqueue.PrepareSingleTask(task, item, marker)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer etcdstore.ClearMutationValues(work.Mutations)
	value, err := groupstore.EncodeRemovalInput(input)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(value)
	tombstoneValue, err := deletionrecord.EncodeDeletionTombstone(deletionrecord.DeletionTombstoneRecord{
		TargetKind: deletionrecord.DeletionTargetReleaseGroup, TargetID: input.GroupID,
		TargetRevision: input.GroupRevision, TaskID: input.TaskID,
		Phase: deletionrecord.DeletionPhaseFinalizing, CreatedAt: input.CreatedAt, UpdatedAt: input.CreatedAt})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(tombstoneValue)
	authoring, err := groupstore.PrepareDesiredAuthoring(ctx, repository.store, prepared)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	desired, err := prepareDirectDesiredProjectionPublication(
		ctx,
		repository.store,
		input.Scope.EnvironmentID,
		marker,
		authoring.Apply,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clearDesiredMutationFragment(desired)
	conditions := append(work.Conditions, prepared.Conditions()...)
	mutations := append(work.Mutations, prepared.Mutations()...)
	defer etcdstore.ClearMutationValues(mutations)
	conditions = append(conditions, etcdstore.Condition{Key: groupstore.RemovalInputKey(input.WorkItemID)})
	mutations = append(
		mutations,
		etcdstore.Mutation{
			Type:  etcdstore.MutationPut,
			Key:   groupstore.RemovalInputKey(input.WorkItemID),
			Value: value,
		},
		etcdstore.Mutation{Type: etcdstore.MutationPut,
			Key: deletionrecord.TombstoneKey(
				string(deletionrecord.DeletionTargetReleaseGroup),
				input.GroupID,
			), Value: tombstoneValue},
	)
	tenant, err := loadTaskInitiationTenant(ctx, repository.store, project)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	initiation, err := newEnvironmentTaskInitiation(tenant, project, environment, taskjournal.TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	conditions, mutations, classify, err := desired.bindDirectDesired(conditions, mutations,
		func(_ int64, _ []*etcdstore.KeyValue) error {
			return errs.New(errs.KindStateConflict, "Release Group removal publication changed")
		})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	conditions, classify, err = prepareTaskInitiationFences(initiation, conditions, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newIdempotencyMutationPlanForMarker(
		idempotencyrecord.IdempotencyMarkerTask,
		conditions,
		mutations,
		classify,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return (&IdempotencyRepository{store: repository.store}).Apply(ctx, marker, plan)
}
