package blueprintrelease

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
	if err := ctx.Err(); err != nil {
		return workdispatch.Decision{}, err
	}
	if request.Item.Item.Operation != workexecution.Release {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Release admission received another operation")
	}
	if request.Direction == workexecution.Recovery {
		if !at.Before(request.Item.Item.Deadline) {
			change, err := workqueue.PrepareCleanupRetryExpiry(request.Task, request.Item, at)
			return workdispatch.Decision{Disposition: workdispatch.NeedsAttention, Change: change}, err
		}
		return service.PrepareRecoveryAssignment(ctx, request, at)
	}
	if request.Direction != workexecution.Forward {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Release admission direction is invalid")
	}
	if request.Task.Task.Stop != nil || request.Item.Item.ForwardRevoked ||
		!at.Before(request.Item.Item.Deadline) || !at.Before(request.Task.Task.Deadline) {
		change, err := releasework.PrepareQueuedStop(request.Task, request.Item, at)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
	}
	if request.Task.Task.Action == workexecution.ActionDeploy || request.Task.Task.Action == workexecution.ActionRollback {
		view := workqueue.View{Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision}
		captured, _, err := service.ledger.ReadDirectReleasePreparation(ctx, view)
		if err != nil {
			return workdispatch.Decision{}, err
		}
		current, guards, err := service.ledger.MatchDirectReleaseSource(ctx, view, captured)
		if err != nil {
			return workdispatch.Decision{}, err
		}
		if !current {
			change, err := releasework.PrepareUnstartedSupersession(view, guards, at)
			return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
		}
	}
	return service.PrepareForwardAssignment(ctx, request, at)
}

func (service *Service) Stop(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot, at time.Time) (workqueue.Change, error) {
	if err := ctx.Err(); err != nil {
		return workqueue.Change{}, err
	}
	if task.Task.Stop == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Release work has no stop request")
	}
	return releasework.PrepareQueuedStop(task, item, at)
}
