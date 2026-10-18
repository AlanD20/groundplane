package attachwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (owner *Reports) admitRecovery(ctx context.Context, request workqueue.Assignment, at time.Time) (workdispatch.Decision, error) {
	item := request.Item.Item
	if request.DecisionOnly || item.Operation != workexecution.Attach || item.Placement.Executor != workexecution.Agent ||
		item.Phase != workexecution.Ready || item.ActiveAttemptID != "" || at.IsZero() || at.Before(item.UpdatedAt) ||
		item.Cleanup != workexecution.CleanupRequired && item.Cleanup != workexecution.CleanupStopped {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Attach inspection cannot obtain this assignment")
	}
	if !at.Before(item.Deadline) {
		change, err := workqueue.PrepareCleanupRetryExpiry(request.Task, request.Item, at)
		return workdispatch.Decision{Disposition: workdispatch.NeedsAttention, Change: change}, err
	}
	if request.Executor.Record.Identity.HostID != item.Placement.HostID {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Attach inspection belongs to another host")
	}
	_, attempt, err := workexecution.Assign(item, request.Executor.Record.Identity, request.AttemptID, workexecution.Recovery, at, item.Deadline)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	view := workqueue.View{Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision}
	_, guards, err := owner.inputs.Recovery(ctx, view, attempt)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	return workdispatch.Decision{Disposition: workdispatch.Dispatch, Deadline: item.Deadline, Change: workqueue.Change{Conditions: guards}}, nil
}
