package taskinspection

import (
	"context"
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
