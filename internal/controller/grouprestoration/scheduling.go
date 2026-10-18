package grouprestoration

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) Admit(ctx context.Context, request workqueue.Assignment, at time.Time) (workdispatch.Decision, error) {
	item, task := request.Item.Item, request.Task.Task
	if request.DecisionOnly || item.Operation != workexecution.ReleaseRestoration || item.Input == nil ||
		item.Placement.Executor != workexecution.Agent || request.Executor.Record.Identity.HostID != item.Placement.HostID {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Group restoration cannot obtain this assignment")
	}
	if request.Direction == workexecution.Recovery && !at.Before(item.Deadline) {
		change, err := workqueue.PrepareCleanupRetryExpiry(request.Task, request.Item, at)
		return workdispatch.Decision{Disposition: workdispatch.NeedsAttention, Change: change}, err
	}
	if request.Direction == workexecution.Forward && (task.Stop != nil || item.ForwardRevoked ||
		!at.Before(task.Deadline) || !at.Before(item.Deadline)) {
		change, err := releasework.PrepareQueuedStop(request.Task, request.Item, at)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
	}
	conditions, err := service.ledger.GroupRestorationConditions(ctx, workqueue.View{Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision})
	if err != nil {
		return workdispatch.Decision{}, err
	}
	return workdispatch.Decision{Disposition: workdispatch.Dispatch, Deadline: item.Deadline,
		Change: workqueue.Change{Conditions: conditions}}, nil
}

func (service *Service) AuthorizeAgentStart(ctx context.Context, view workqueue.ResumeView) (workqueue.Change, error) {
	if _, err := service.LoadRecoveryInput(ctx, view); err != nil {
		return workqueue.Change{}, err
	}
	conditions, err := service.ledger.GroupRestorationConditions(ctx, view.Work)
	return workqueue.Change{Conditions: conditions}, err
}

func (service *Service) Stop(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot, at time.Time) (workqueue.Change, error) {
	if err := ctx.Err(); err != nil {
		return workqueue.Change{}, err
	}
	if task.Task.Stop == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Group restoration has no stop request")
	}
	return releasework.PrepareQueuedStop(task, item, at)
}
