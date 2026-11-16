package etcd

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerupdates"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *RunnerRepository) PrepareRunnerSoftwareUpdate(
	ctx context.Context,
	source RunnerRuntimeSource,
	record runnerupdates.Record,
	task workexecution.Task,
	item workexecution.Item,
) (workqueue.Change, error) {
	if source.readRevision <= 0 || record.Validate() != nil || record.Phase != runnerupdates.Ready ||
		task.Validate() != nil || task.Actor != workexecution.OperatorActor && task.Actor != workexecution.SystemActor || task.Action != workexecution.ActionUpdate ||
		task.ExpectedItems != 1 || task.PublicationComplete || task.Progress.Total() != 0 ||
		record.Owner.TaskID != task.ID || record.Owner.PlanID != task.Intent.ID || record.Source.RunnerID != task.TargetID ||
		task.Scope != (workexecution.Scope{TenantID: source.runner.Desired.TenantID,
			ProjectID: source.runner.Desired.ProjectID(), EnvironmentID: source.runner.Desired.EnvironmentID()}) ||
		record.CreatedAt.Before(task.CreatedAt) || record.CreatedAt.After(task.UpdatedAt) ||
		record.Plan.Allocation != source.runner.Allocation {
		return workqueue.Change{}, errs.New(
			errs.KindValidationFailed,
			"Runner software update publication differs from its captured input",
		)
	}
	if (task.Actor == workexecution.SystemActor) != source.runner.Desired.AutoUpdate {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Runner automatic-update setting changed before publication",
		)
	}
	rebuilt, err := runnerupdates.NewRecord(
		record.Owner,
		source.Configuration(),
		record.Plan,
		record.Request,
		record.CreatedAt,
	)
	if err != nil || rebuilt.InputSHA256 != record.InputSHA256 {
		return workqueue.Change{}, errs.New(
			errs.KindValidationFailed,
			"Runner software update source differs from its repository capture",
		)
	}
	digest, err := runnerupdates.PlanHash(record)
	if err != nil || digest != task.Intent.SHA256 || task.AcceptedIntent == nil ||
		item.Validate() != nil || item.TaskID != task.ID || item.ID != record.Owner.WorkItemID ||
		item.Operation != workexecution.RunnerSoftware || item.TargetID != task.TargetID ||
		item.Placement != (workexecution.Placement{Executor: workexecution.Controller}) ||
		item.Input == nil || *item.Input != task.Intent ||
		!item.CreatedAt.Equal(record.CreatedAt) || item.Deadline.After(task.Deadline) {
		return workqueue.Change{}, errs.New(
			errs.KindValidationFailed,
			"Runner captured work differs from its accepted Task",
		)
	}
	var conditions []keyvalue.Condition
	var mutations []keyvalue.Mutation
	checkConditions, checkMutations, err := runnerupdates.PreparePublication(record)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearMutationValues(checkMutations)
	conditions = append(conditions, slices.Clone(source.conditions)...)
	conditions = append(conditions, checkConditions...)
	mutations = append(mutations, checkMutations...)
	journalActor := taskjournal.TaskActorOperator
	if task.Actor == workexecution.SystemActor {
		journalActor = taskjournal.TaskActorSystem
	}
	initiation, err := newRunnerTaskInitiation(source.runner.Desired, source.parents, journalActor)
	if err != nil {
		return workqueue.Change{}, err
	}
	conditions = append(conditions, initiation.fences...)
	return ownedRunnerCapture(workqueue.Change{Conditions: conditions, Mutations: mutations}), nil
}
