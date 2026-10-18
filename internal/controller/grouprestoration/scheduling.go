package grouprestoration

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) Admit(ctx context.Context, request workqueue.Assignment, at time.Time) (workdispatch.Decision, error) {
	item, task := request.Item.Item, request.Task.Task
	if request.DecisionOnly || item.Operation != workexecution.ReleaseRestoration || item.Input == nil ||
		request.Direction != workexecution.Forward && request.Direction != workexecution.Recovery ||
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
	view := workqueue.View{Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision}
	conditions, err := service.ledger.GroupRestorationConditions(ctx, view)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	if request.Direction == workexecution.Recovery {
		previous, err := service.groups.LoadRestorationFailure(ctx, view, item.AttemptNumber)
		if err != nil {
			return workdispatch.Decision{}, err
		}
		conditions = append(conditions, previous.Conditions...)
	}
	return workdispatch.Decision{Disposition: workdispatch.Dispatch, Deadline: item.Deadline,
		Change: workqueue.Change{Conditions: conditions}}, nil
}

func (service *Service) AuthorizeAgentStart(ctx context.Context, view workqueue.ResumeView) (workqueue.Change, error) {
	if _, err := service.LoadRecoveryInput(ctx, view); err != nil {
		return workqueue.Change{}, err
	}
	conditions, err := service.activeConditions(ctx, view)
	return workqueue.Change{Conditions: conditions}, err
}

// activeConditions joins the exact interrupted attempt to each new execution
// grant. The successful Group member and the later failure remain unchanged.
func (service *Service) activeConditions(ctx context.Context, view workqueue.ResumeView) ([]keyvalue.Condition, error) {
	conditions, err := service.ledger.GroupRestorationConditions(ctx, view.Work)
	if err != nil || view.Attempt.Attempt.Direction != workexecution.Recovery {
		return conditions, err
	}
	if view.Attempt.Attempt.Number < 2 {
		return nil, errs.New(errs.KindStateConflict, "Group restoration Retry has no preceding attempt")
	}
	previous, err := service.groups.LoadRestorationFailure(ctx, view.Work, view.Attempt.Attempt.Number-1)
	if err != nil {
		return nil, err
	}
	return append(conditions, previous.Conditions...), nil
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
