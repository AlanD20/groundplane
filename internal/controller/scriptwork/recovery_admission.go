package scriptwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) PrepareWorkRetry(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot,
	previous workqueue.AttemptSnapshot, revision int64, at time.Time,
) (workqueue.Change, error) {
	if item.Item.Operation != workexecution.Script || item.Item.Phase != workexecution.Attention ||
		item.Item.ActiveAttemptID != "" ||
		(item.Item.Cleanup != workexecution.CleanupRequired && item.Item.Cleanup != workexecution.CleanupStopped) ||
		previous.Attempt.Result == nil ||
		previous.Attempt.FinishedAt == nil {
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "Script has no stopped cleanup to retry")
	}
	sources, err := service.repository.LoadScriptRecoverySources(
		ctx,
		workqueue.View{Task: task, Item: item, ReadRevision: revision},
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	budget := task.Task.Deadline.Sub(task.Task.CreatedAt)
	if budget <= 0 || budget > 24*time.Hour {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Script cleanup has no valid captured execution budget",
		)
	}
	change, err := workqueue.PrepareRetry(task, item, previous, workexecution.RetryDecision{
		Direction: workexecution.Recovery, Result: *previous.Attempt.Result, NotBefore: at, Deadline: at.Add(budget)}, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, sources.Conditions...)
	return change, nil
}

func (service *Service) admitRecovery(
	ctx context.Context,
	request workqueue.Assignment,
	at time.Time,
) (workdispatch.Decision, error) {
	item := request.Item.Item
	if !at.Before(item.Deadline) {
		change, err := workqueue.PrepareCleanupRetryExpiry(request.Task, request.Item, at)
		return workdispatch.Decision{Disposition: workdispatch.NeedsAttention, Change: change}, err
	}
	if request.DecisionOnly || item.Operation != workexecution.Script ||
		item.Placement.Executor != workexecution.Agent ||
		request.Executor.Record.Identity.HostID != item.Placement.HostID ||
		item.Phase != workexecution.Ready ||
		item.ActiveAttemptID != "" {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Script cleanup cannot obtain this assignment")
	}
	sources, err := service.repository.LoadScriptRecoverySources(
		ctx,
		workqueue.View{Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision},
	)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	return workdispatch.Decision{Disposition: workdispatch.Dispatch, Deadline: item.Deadline,
		Change: workqueue.Change{Conditions: sources.Conditions}}, nil
}
