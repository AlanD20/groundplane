package serviceremoval

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (owner *AgentWork) Admit(ctx context.Context, request workqueue.Assignment, at time.Time) (workdispatch.Decision, error) {
	if request.Direction == workexecution.Recovery {
		return owner.admitRecovery(ctx, request, at)
	}
	task, item := request.Task.Task, request.Item.Item
	if request.DecisionOnly || request.Direction != workexecution.Forward || item.Operation != workexecution.ServiceRemoval ||
		item.Placement.Executor != workexecution.Agent || item.Phase != workexecution.Ready || item.ActiveAttemptID != "" ||
		at.IsZero() || at.Before(item.UpdatedAt) || request.Executor.Record.Identity.HostID != item.Placement.HostID {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Service removal cannot obtain this host assignment")
	}
	view := workqueue.View{Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision}
	if task.Stop != nil || item.ForwardRevoked || !at.Before(item.Deadline) || !at.Before(task.Deadline) {
		if item.AttemptNumber == 0 {
			change, err := owner.inputs.PrepareUnstartedStop(ctx, view, at)
			return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
		}
		captured, _, err := owner.inputs.Load(ctx, view)
		if err != nil {
			return workdispatch.Decision{}, err
		}
		delivery, err := BuildHostInput(captured)
		if err != nil {
			return workdispatch.Decision{}, err
		}
		_, previous, err := owner.queue.ReadAttempt(ctx, task.ID, item.ID, item.AttemptNumber, request.ReadRevision)
		if err != nil {
			return workdispatch.Decision{}, err
		}
		change, err := owner.inputs.PrepareQueuedRetryStop(ctx, view, previous, delivery, at)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
	}
	captured, change, err := owner.inputs.PrepareAssignment(ctx, view)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	if captured.Runtime == nil || captured.HostID != request.Executor.Record.Identity.HostID {
		if captured.Runtime == nil && captured.HostID == request.Executor.Record.Identity.HostID {
			settlement, err := owner.records.PrepareServiceRemovalDecision(ctx, view, at)
			return workdispatch.Decision{Disposition: workdispatch.Settle, Change: settlement}, err
		}
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Service removal belongs to a different host")
	}
	deadline := item.Deadline
	if task.Deadline.Before(deadline) {
		deadline = task.Deadline
	}
	return workdispatch.Decision{Disposition: workdispatch.Dispatch, Deadline: deadline, Change: change}, nil
}
