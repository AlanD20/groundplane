package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareRunnerRemovalCapture fences the captured allocation and publishes one native
// work item. Neither acceptance nor a failed cleanup releases host reservations.
func (repository *RunnerRepository) PrepareRunnerRemovalCapture(ctx context.Context,
	source RunnerRemovalSource, task workexecution.Task, item workexecution.Item,
) (workqueue.Change, error) {
	input := source.input
	desired := input.Desired
	ref, err := input.Reference()
	if err != nil {
		return workqueue.Change{}, err
	}
	if len(source.conditions) == 0 || input.Removal == nil || task.Validate() != nil ||
		task.ID != input.TaskID || task.Action != workexecution.ActionRemove || task.Actor != workexecution.OperatorActor ||
		task.TargetID != desired.ID || task.ExpectedItems != 1 || task.Intent != ref ||
		task.Scope != (workexecution.Scope{TenantID: desired.TenantID, ProjectID: desired.ProjectID(), EnvironmentID: desired.EnvironmentID()}) ||
		input.CreatedAt.Before(task.CreatedAt) || input.CreatedAt.After(task.UpdatedAt) {
		return workqueue.Change{}, errs.New(
			errs.KindValidationFailed,
			"Runner removal Task differs from its captured runtime",
		)
	}
	if task.AcceptedIntent == nil || item.Validate() != nil || item.TaskID != task.ID ||
		item.ID != input.WorkItemID || item.Operation != workexecution.Runner || item.TargetID != desired.ID ||
		item.Placement != (workexecution.Placement{Executor: workexecution.Controller}) ||
		item.Input == nil || *item.Input != task.Intent || !item.CreatedAt.Equal(input.CreatedAt) || item.Deadline.After(task.Deadline) {
		return workqueue.Change{}, errs.New(
			errs.KindValidationFailed,
			"Runner removal work differs from its accepted Task",
		)
	}
	captureConditions, captureMutations, err := runnerwork.PreparePublication(input)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearMutationValues(captureMutations)
	tombstone := deletions.DeletionTombstoneRecord{TargetKind: deletions.DeletionTargetRunner, TargetID: desired.ID,
		TargetRevision: input.Removal.DesiredRevision, TaskID: task.ID, Phase: deletions.DeletionPhaseFinalizing,
		CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt}
	tombstoneValue, err := runners.EncodeRunnerDeletionTombstone(tombstone)
	if err != nil {
		return workqueue.Change{}, err
	}
	conditions := append([]keyvalue.Condition(nil), source.conditions...)
	conditions = append(conditions, captureConditions...)
	mutations := append([]keyvalue.Mutation(nil), captureMutations...)
	mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut,
		Key: deletions.TombstoneKey(string(deletions.DeletionTargetRunner), desired.ID), Value: tombstoneValue})
	initiation, err := newRunnerTaskInitiation(desired, source.parents, taskjournal.TaskActorOperator)
	if err != nil {
		return workqueue.Change{}, err
	}
	conditions = append(conditions, initiation.fences...)
	return ownedRunnerCapture(workqueue.Change{Conditions: conditions, Mutations: mutations}), nil
}
