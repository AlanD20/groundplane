package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PublishRunnerRemoval fences the captured allocation and publishes one native
// work item. Neither acceptance nor a failed cleanup releases host reservations.
func (repository *RunnerRepository) PublishRunnerRemoval(ctx context.Context,
	source RunnerRemovalSource, task workexecution.Task, marker idempotency.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	input := source.input
	desired := input.Desired
	ref, err := input.Reference()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if len(source.conditions) == 0 || input.Removal == nil || task.Validate() != nil ||
		task.ID != input.TaskID || task.Action != workexecution.ActionRemove || task.Actor != workexecution.OperatorActor ||
		task.TargetID != desired.ID || task.ExpectedItems != 1 || task.Intent != ref ||
		task.Scope != (workexecution.Scope{TenantID: desired.TenantID, ProjectID: desired.ProjectID(), EnvironmentID: desired.EnvironmentID()}) ||
		!task.CreatedAt.Equal(input.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "Runner removal Task differs from its captured runtime")
	}
	target := idempotency.IdempotencyReplayTarget{Kind: idempotency.IdempotencyReplayTargetRunner, ID: desired.ID}
	if idempotency.ValidateIdempotencyMarker(marker) != nil || validateRunnerMarkerScope(desired, marker) != nil ||
		marker.Kind != idempotency.IdempotencyMarkerTask || marker.State != idempotency.IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.Method != "DELETE" || marker.Locator.Route != "/runners/{id}" ||
		!runnerReplayTargetsEqual(marker.ReplayTarget, &target) || !idempotency.ValidTaskResponse(marker.Response, task.ID) ||
		!marker.CreatedAt.Equal(task.CreatedAt) || !marker.UpdatedAt.Equal(task.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "Runner removal replay marker is invalid")
	}
	item := workexecution.Item{ID: input.WorkItemID, TaskID: task.ID, Operation: workexecution.Runner,
		TargetID: desired.ID, Placement: workexecution.Placement{Executor: workexecution.Controller},
		Claims: []workexecution.Claim{{ScopeID: desired.ID}}, Input: &task.Intent,
		Phase: workexecution.Ready, Cleanup: workexecution.CleanupNone,
		CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt, Deadline: task.Deadline}
	work, err := workqueue.PrepareSingleTask(task, item, marker)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(work.Mutations)
	captureConditions, captureMutations, err := runnerwork.PreparePublication(input)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(captureMutations)
	tombstone := deletions.DeletionTombstoneRecord{TargetKind: deletions.DeletionTargetRunner, TargetID: desired.ID,
		TargetRevision: input.Removal.DesiredRevision, TaskID: task.ID, Phase: deletions.DeletionPhaseFinalizing,
		CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt}
	tombstoneValue, err := runners.EncodeRunnerDeletionTombstone(tombstone)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	conditions := append(work.Conditions, source.conditions...)
	conditions = append(conditions, captureConditions...)
	mutations := append(work.Mutations, captureMutations...)
	mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut,
		Key: deletions.TombstoneKey(string(deletions.DeletionTargetRunner), desired.ID), Value: tombstoneValue})
	publicationConditions := conditions
	classify := func(_ int64, values []*keyvalue.KeyValue) error {
		if len(values) != len(publicationConditions) {
			return errs.New(errs.KindInternal, "Runner removal publication evidence is incomplete")
		}
		return errs.New(errs.KindStateConflict, "Runner runtime, allocation or another operation changed before removal; refresh and submit again")
	}
	initiation, err := newRunnerTaskInitiation(desired, source.parents, taskjournal.TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	conditions, classify, err = prepareTaskInitiationFences(initiation, conditions, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	publication, err := newIdempotencyMutationPlanForMarker(idempotency.IdempotencyMarkerTask, conditions, mutations, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	receipts, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return receipts.Apply(ctx, marker, publication)
}
