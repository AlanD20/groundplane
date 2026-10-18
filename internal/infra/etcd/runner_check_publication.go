package etcd

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *RunnerRepository) CreateRunnerCheck(
	ctx context.Context,
	source RunnerCheckSource,
	record runnerchecks.Record,
	task workexecution.Task,
	marker idempotency.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if source.readRevision <= 0 || record.Validate() != nil || record.Phase != runnerchecks.Ready ||
		task.Validate() != nil || task.Actor != workexecution.OperatorActor || task.Action != workexecution.ActionCheck ||
		task.ExpectedItems != 1 || task.PublicationComplete || task.Progress.Total() != 0 ||
		record.Owner.TaskID != task.ID || record.Owner.PlanID != task.Intent.ID || record.Source.RunnerID != task.TargetID ||
		task.Scope != (workexecution.Scope{TenantID: source.runner.Desired.TenantID,
			ProjectID: source.runner.Desired.ProjectID(), EnvironmentID: source.runner.Desired.EnvironmentID()}) ||
		!record.CreatedAt.Equal(task.CreatedAt) ||
		record.Plan.Allocation != source.runner.Allocation {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Runner diagnostic publication differs from its captured input",
		)
	}
	rebuilt, err := runnerchecks.NewRecord(
		record.Owner,
		source.Configuration(),
		record.Plan,
		record.Request,
		record.CreatedAt,
	)
	if err != nil || rebuilt.InputSHA256 != record.InputSHA256 {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Runner diagnostic source differs from its repository capture",
		)
	}
	digest, err := runnerchecks.PlanHash(record)
	target := idempotency.IdempotencyReplayTarget{Kind: idempotency.IdempotencyReplayTargetRunner, ID: task.TargetID}
	if err != nil || digest != task.Intent.SHA256 ||
		idempotency.ValidateIdempotencyMarker(marker) != nil || validateRunnerMarkerScope(source.runner.Desired, marker) != nil ||
		marker.Kind != idempotency.IdempotencyMarkerTask || marker.State != idempotency.IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.Method != "POST" || marker.Locator.Route != "/runners/{id}/checks" ||
		!runnerReplayTargetsEqual(marker.ReplayTarget, &target) || !idempotency.ValidTaskResponse(marker.Response, task.ID) ||
		!marker.CreatedAt.Equal(task.CreatedAt) || !marker.UpdatedAt.Equal(task.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "Runner check captured input or replay marker differs from its Task")
	}
	item := workexecution.Item{ID: record.Owner.WorkItemID, TaskID: task.ID, Operation: workexecution.RunnerCheck,
		TargetID: task.TargetID, Placement: workexecution.Placement{Executor: workexecution.Controller},
		Claims: []workexecution.Claim{{ScopeID: task.TargetID}}, Input: &task.Intent,
		Phase: workexecution.Ready, Cleanup: workexecution.CleanupNone,
		CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt, Deadline: task.Deadline}
	work, err := workqueue.PrepareSingleTask(task, item, marker)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(work.Mutations)
	conditions, mutations := work.Conditions, work.Mutations
	checkConditions, checkMutations, err := runnerchecks.PreparePublication(record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(checkMutations)
	conditions = append(conditions, slices.Clone(source.conditions)...)
	conditions = append(conditions, checkConditions...)
	mutations = append(mutations, checkMutations...)
	publicationConditions := conditions
	classify := func(_ int64, values []*keyvalue.KeyValue) error {
		if len(values) != len(publicationConditions) {
			return errs.New(errs.KindInternal, "Runner diagnostic publication evidence is incomplete")
		}
		for index, condition := range publicationConditions {
			if keyvalue.RevisionOf(values[index]) != condition.ModRevision {
				return errs.New(
					errs.KindStateConflict,
					"Runner runtime, ownership or another check changed before acceptance; refresh and submit a new check",
				)
			}
		}
		return errs.New(errs.KindStateConflict, "Runner diagnostic publication changed before acceptance")
	}
	initiation, err := newRunnerTaskInitiation(source.runner.Desired, source.parents, taskjournal.TaskActorOperator)
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
	evidence, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return evidence.Apply(ctx, marker, publication)
}
