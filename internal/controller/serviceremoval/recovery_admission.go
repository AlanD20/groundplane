package serviceremoval

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/serviceremovalwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (owner *AgentWork) admitRecovery(ctx context.Context, request workqueue.Assignment, at time.Time) (workdispatch.Decision, error) {
	task, item := request.Task.Task, request.Item.Item
	if request.DecisionOnly || request.Direction != workexecution.Recovery || item.Operation != workexecution.ServiceRemoval ||
		item.Placement.Executor != workexecution.Agent || item.Phase != workexecution.Ready || item.ActiveAttemptID != "" ||
		!task.PublicationComplete || task.FinishedAt != nil || task.CollectionStartedAt != nil ||
		at.IsZero() || at.Before(item.UpdatedAt) ||
		(item.Cleanup != workexecution.CleanupRequired && item.Cleanup != workexecution.CleanupStopped) ||
		request.Executor.Record.Identity.HostID != item.Placement.HostID {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Service removal recovery cannot obtain this host assignment")
	}
	if !at.Before(item.Deadline) {
		change, err := workqueue.PrepareCleanupRetryExpiry(request.Task, request.Item, at)
		return workdispatch.Decision{Disposition: workdispatch.NeedsAttention, Change: change}, err
	}
	view := workqueue.View{Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision}
	captured, guard, err := owner.inputs.Load(ctx, view)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	if captured.HostID != request.Executor.Record.Identity.HostID {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Service removal recovery belongs to another host")
	}
	delivery, err := BuildHostInput(captured)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	saved, err := owner.inputs.LoadRecovery(ctx, view, delivery)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	// Forward head/revocation guards do not apply to restoration. The original
	// deletion fence and acknowledged runtime still protect the captured resource.
	conditions, err := owner.inputs.RuntimeGuards(ctx, captured, view.ReadRevision)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	conditions = append(conditions, guard,
		keyvalue.Condition{Key: serviceremovalwork.RecoveryKey(item.ID), ModRevision: saved.Revision},
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: request.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(item.ID), ModRevision: request.Item.Revision})
	return workdispatch.Decision{Disposition: workdispatch.Dispatch, Deadline: item.Deadline,
		Change: workqueue.Change{Conditions: conditions}}, nil
}
