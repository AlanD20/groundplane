package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/desiredauthoring"
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

// PrepareReleaseGroupRemovalAuthority captures desired authoring and native removal for promotion of the accepted Task.
func (repository *TaskRepository) PrepareReleaseGroupRemovalAuthority(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	prepared groupstore.ReleaseGroupPreparedMutation,
	input groupstore.RemovalInput,
	task workexecution.Task,
	acceptedRevision int64,
	locator idempotencyrecord.IdempotencyLocator,
) (workqueue.Change, workexecution.Item, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return workqueue.Change{}, workexecution.Item{}, err
	}
	if task.Validate() != nil || task.Actor != workexecution.OperatorActor || task.AcceptedIntent == nil ||
		!task.UpdatedAt.Equal(input.CreatedAt) {
		return workqueue.Change{}, workexecution.Item{}, errs.New(
			errs.KindValidationFailed,
			"Release Group removal requires its captured accepted Task",
		)
	}
	if err := groupstore.ValidatePreparedRemoval(prepared, input); err != nil {
		return workqueue.Change{}, workexecution.Item{}, err
	}
	if environment.Record.ID != input.Scope.EnvironmentID || project.Record.ID != environment.Record.ProjectID ||
		project.Record.ID != input.Scope.ProjectID || project.Record.TenantID != input.Scope.TenantID {
		return workqueue.Change{}, workexecution.Item{}, errs.New(
			errs.KindScopeUnauthorized,
			"Release Group removal hierarchy changed",
		)
	}
	_, item, err := input.Work()
	if err != nil {
		return workqueue.Change{}, workexecution.Item{}, err
	}
	if item.Deadline.After(task.Deadline) {
		item.Deadline = task.Deadline
	}
	if err := input.Match(task, item); err != nil {
		return workqueue.Change{}, workexecution.Item{}, err
	}
	evidence, err := (&IdempotencyRepository{store: repository.store}).Read(ctx, locator)
	if err != nil {
		return workqueue.Change{}, workexecution.Item{}, err
	}
	if evidence == nil || evidence.marker.TaskID != task.ID ||
		evidence.marker.Kind != idempotencyrecord.IdempotencyMarkerTask ||
		evidence.marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		!evidence.marker.CreatedAt.Equal(task.CreatedAt) {
		return workqueue.Change{}, workexecution.Item{}, errs.New(
			errs.KindStateConflict,
			"Release Group accepted request is unavailable",
		)
	}
	marker := evidence.marker
	defer clear(marker.Intent.Ciphertext)
	markerKey, err := idempotencyrecord.IdempotencyMarkerKey(locator)
	if err != nil {
		return workqueue.Change{}, workexecution.Item{}, err
	}
	value, err := groupstore.EncodeRemovalInput(input)
	if err != nil {
		return workqueue.Change{}, workexecution.Item{}, err
	}
	defer clear(value)
	tombstoneValue, err := deletionrecord.EncodeDeletionTombstone(deletionrecord.DeletionTombstoneRecord{
		TargetKind: deletionrecord.DeletionTargetReleaseGroup, TargetID: input.GroupID,
		TargetRevision: input.GroupRevision, TaskID: input.TaskID,
		Phase: deletionrecord.DeletionPhaseFinalizing, CreatedAt: input.CreatedAt, UpdatedAt: input.CreatedAt})
	if err != nil {
		return workqueue.Change{}, workexecution.Item{}, err
	}
	defer clear(tombstoneValue)
	authoring, err := groupstore.PrepareDesiredAuthoring(ctx, repository.store, prepared)
	if err != nil {
		return workqueue.Change{}, workexecution.Item{}, err
	}
	publication, err := desiredauthoring.PrepareAccepted(
		ctx,
		repository.store,
		task,
		acceptedRevision,
		marker,
		input.CreatedAt,
		authoring.Apply,
	)
	if err != nil {
		return workqueue.Change{}, workexecution.Item{}, err
	}
	defer etcdstore.ClearMutationValues(publication.Mutations)
	desired := desiredMutationFragment{conditions: publication.Conditions, mutations: publication.Mutations}
	conditions := prepared.Conditions()
	conditions = append(conditions, etcdstore.Condition{Key: markerKey, ModRevision: evidence.modRevision})
	mutations := prepared.Mutations()
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
		return workqueue.Change{}, workexecution.Item{}, err
	}
	initiation, err := newEnvironmentTaskInitiation(tenant, project, environment, taskjournal.TaskActorOperator)
	if err != nil {
		return workqueue.Change{}, workexecution.Item{}, err
	}
	conditions, mutations, classify, err := desired.bindDirectDesired(conditions, mutations,
		func(_ int64, _ []*etcdstore.KeyValue) error {
			return errs.New(errs.KindStateConflict, "Release Group removal publication changed")
		})
	if err != nil {
		return workqueue.Change{}, workexecution.Item{}, err
	}
	conditions, classify, err = prepareTaskInitiationFences(initiation, conditions, classify)
	if err != nil {
		return workqueue.Change{}, workexecution.Item{}, err
	}
	_ = classify

	return workqueue.Change{Conditions: conditions, Mutations: etcdstore.CloneMutations(mutations)}, item, nil
}
