package hierarchydeletion

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ControllerWork handles metadata-only actions in a captured removal graph.
// Host cleanup is a separate execution owner, not a Controller no-op attempt.
type ControllerWork struct {
	records *etcd.HierarchyDeletionRepository
}

func NewControllerWork(records *etcd.HierarchyDeletionRepository) (*ControllerWork, error) {
	if records == nil {
		return nil, errs.New(errs.KindInternal, "hierarchy Controller work requires captured storage")
	}
	return &ControllerWork{records: records}, nil
}

func (owner *ControllerWork) Admit(ctx context.Context, request workqueue.Assignment, at time.Time) (workdispatch.Decision, error) {
	if request.Item.Item.Placement.Executor != workexecution.Controller || !request.DecisionOnly {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "metadata removal requires Controller decision admission")
	}
	renewal, err := owner.records.PrepareHierarchyDeadline(ctx, request, at)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	if len(renewal.Mutations) != 0 {
		return workdispatch.Decision{Disposition: workdispatch.Advance, Change: renewal}, nil
	}
	if !at.Before(request.Item.Item.Deadline) {
		change, err := owner.records.PrepareHierarchyUnstartedStop(ctx, request.Task, request.Item, workexecution.Failed, at)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
	}
	progress, err := owner.records.PrepareHierarchyControllerProgress(ctx, request, at)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	if len(progress.Mutations) != 0 {
		return workdispatch.Decision{Disposition: workdispatch.Advance, Change: progress}, nil
	}
	change, err := owner.records.PrepareHierarchyControllerSettlement(ctx, request, at)
	return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
}

func (owner *ControllerWork) Stop(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot, at time.Time) (workqueue.Change, error) {
	if task.Task.Stop == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "removal work has no stop request")
	}
	return owner.records.PrepareHierarchyUnstartedStop(ctx, task, item, task.Task.Stop.Outcome, at)
}
