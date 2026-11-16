package taskinspection

import (
	"context"
	"fmt"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

type Preparation struct {
	Reason     string
	Outcome    workexecution.Outcome
	Summary    string
	NextAction string
	FinishedAt time.Time
}

func (service *Service) preparation(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	revision int64,
) (*Preparation, error) {
	if task.Task.AcceptedIntent != nil && task.Task.Intent == (workexecution.Input{}) {
		if task.Task.FinishedAt == nil {
			return nil, nil
		}
		result := &Preparation{Outcome: task.Task.Outcome, FinishedAt: *task.Task.FinishedAt,
			Reason: "preparation.stopped", Summary: "This Task stopped before execution inputs were prepared. No workload operation started.",
			NextAction: "Submit a new action if it is still needed."}
		failure, err := service.queue.ReadAcceptedPreparationFailure(ctx, task.Task.ID, revision)
		if err != nil {
			return nil, err
		}
		if failure != nil {
			result.Reason, result.Summary = failure.Code, failure.Detail
			result.NextAction = "Correct the reported input or prerequisite, then submit a new action."
		}
		if len(task.Task.After) != 0 {
			dependencies, _, err := service.queue.ReadTaskDependencies(ctx, task.Task.ID, revision)
			if err != nil {
				return nil, err
			}
			if dependencies.Revision > 0 {
				decision, err := dependencies.Record.Decision()
				if err != nil {
					return nil, err
				}
				if decision.Failed != nil {
					result.Reason = "dependency.failed"
					result.Summary = fmt.Sprintf(
						"Run after Task %s reported %s. This Task did not start.",
						decision.Failed.TaskID,
						decision.Failed.Outcome,
					)
					result.NextAction = "Inspect the predecessor Task, resolve its failure, then submit a new action."
				}
			}
		}
		return result, nil
	}
	rejection, err := service.hierarchy.ReadPlanningRejection(ctx, task, revision)
	if err != nil || rejection == nil {
		return nil, err
	}
	result := &Preparation{Reason: string(rejection.Reason), Outcome: rejection.Outcome, FinishedAt: rejection.At}
	switch rejection.Reason {
	case hierarchydeletion.PlanningDeadline:
		result.Summary = "Removal preparation exceeded its deadline. No removal work started; the resource is unlocked."
		result.NextAction = "Check Controller availability and storage health, then submit a new removal."
	case hierarchydeletion.PlanningStopped:
		result.Summary = "Removal was stopped during preparation. No removal work started; the resource is unlocked."
		result.NextAction = "Submit a new removal if it is still needed."
	case hierarchydeletion.PlanningInvalidInput:
		result.Summary = "The captured removal inputs could not form a valid plan. No removal work started; the resource is unlocked."
		result.NextAction = "Review the resource configuration and Controller diagnostics, then submit a new removal."
	case hierarchydeletion.PlanningDependencyConflict:
		result.Summary = "A dependency prevents this removal. No removal work started; the resource is unlocked."
		result.NextAction = "Resolve the resource's dependencies, then submit a new removal."
	}
	return result, nil
}
