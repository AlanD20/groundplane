package blueprintrelease

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) PrepareRecoveryAssignment(
	ctx context.Context,
	request workqueue.Assignment,
	at time.Time,
) (workdispatch.Decision, error) {
	item := request.Item.Item
	if service == nil || service.ledger == nil || request.DecisionOnly || request.Direction != workexecution.Recovery ||
		item.Phase != workexecution.Ready || item.ActiveAttemptID != "" || item.Placement.Executor != workexecution.Agent ||
		request.Executor.Record.Identity.HostID != item.Placement.HostID || at.IsZero() || !at.Before(item.Deadline) {
		return workdispatch.Decision{}, errs.New(
			errs.KindStateConflict,
			"Release recovery cannot obtain this assignment",
		)
	}
	conditions, err := service.ledger.BlueprintRecoveryConditions(ctx, workqueue.View{
		Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision,
	})
	if err != nil {
		return workdispatch.Decision{}, err
	}
	return workdispatch.Decision{Disposition: workdispatch.Dispatch, Deadline: item.Deadline,
		Change: workqueue.Change{Conditions: conditions}}, nil
}

func (service *Service) AuthorizeAgentStart(ctx context.Context, view workqueue.ResumeView) (workqueue.Change, error) {
	if view.Attempt.Attempt.Direction == workexecution.Forward {
		return service.PrepareForwardStart(ctx, view)
	}
	if _, err := service.LoadRecoveryInput(ctx, view); err != nil {
		return workqueue.Change{}, err
	}
	conditions, err := service.ledger.BlueprintRecoveryConditions(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: conditions}, nil
}
