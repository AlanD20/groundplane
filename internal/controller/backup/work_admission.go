package backup

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (owner *AgentWork) Admit(ctx context.Context, request workqueue.Assignment, at time.Time) (workdispatch.Decision, error) {
	if request.Direction == workexecution.Recovery {
		return owner.admitRecovery(ctx, request, at)
	}
	task, item := request.Task.Task, request.Item.Item
	if request.DecisionOnly || request.Direction != workexecution.Forward ||
		(item.Operation != workexecution.BackupPrune && item.Operation != workexecution.Backup && item.Operation != workexecution.Restore) ||
		item.Placement.Executor != workexecution.Agent || item.Phase != workexecution.Ready || item.ActiveAttemptID != "" ||
		at.IsZero() || at.Before(item.UpdatedAt) || request.Executor.Record.Identity.HostID != item.Placement.HostID {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Backup work is not ready for this host assignment")
	}
	view := workqueue.View{Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision}
	if task.Stop != nil || item.ForwardRevoked || !at.Before(item.Deadline) || !at.Before(task.Deadline) {
		change, err := owner.inputs.PrepareUnstartedStop(ctx, view, at)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
	}
	_, change, err := owner.inputs.PrepareAdmission(ctx, view)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	deadline := item.Deadline
	if task.Deadline.Before(deadline) {
		deadline = task.Deadline
	}
	return workdispatch.Decision{Disposition: workdispatch.Dispatch, Deadline: deadline, Change: change}, nil
}

func (owner *AgentWork) admitRecovery(ctx context.Context, request workqueue.Assignment, at time.Time) (workdispatch.Decision, error) {
	item := request.Item.Item
	if request.DecisionOnly || (item.Operation != workexecution.BackupPrune && item.Operation != workexecution.Backup && item.Operation != workexecution.Restore) || item.Placement.Executor != workexecution.Agent ||
		item.Phase != workexecution.Ready || item.ActiveAttemptID != "" || at.IsZero() || at.Before(item.UpdatedAt) ||
		(item.Cleanup != workexecution.CleanupRequired && item.Cleanup != workexecution.CleanupStopped) ||
		request.Executor.Record.Identity.HostID != item.Placement.HostID {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Backup recovery is not ready for this host assignment")
	}
	if !at.Before(item.Deadline) {
		change, err := workqueue.PrepareCleanupRetryExpiry(request.Task, request.Item, at)
		return workdispatch.Decision{Disposition: workdispatch.NeedsAttention, Change: change}, err
	}
	view := workqueue.View{Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision}
	_, change, err := owner.inputs.PrepareAdmission(ctx, view)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	source, err := owner.inputs.LoadRecovery(ctx, view)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: backupwork.RecoveryKey(item.ID), ModRevision: source.Revision})
	return workdispatch.Decision{Disposition: workdispatch.Dispatch, Deadline: item.Deadline, Change: change}, nil
}

func (owner *AgentWork) Stop(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot, at time.Time) (workqueue.Change, error) {
	if task.Task.Stop == nil || (item.Item.Operation != workexecution.BackupPrune && item.Item.Operation != workexecution.Backup && item.Item.Operation != workexecution.Restore) || item.Item.TaskID != task.Task.ID {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Backup work has no matching stop request")
	}
	if item.Item.ActiveAttemptID != "" || item.Item.Cleanup != workexecution.CleanupNone {
		return workqueue.PrepareRevocation(task, item, task.Task.Stop.Outcome, at)
	}
	view, err := owner.queue.ReadWork(ctx, task.Task.ID, item.Item.ID, 0)
	if err != nil {
		return workqueue.Change{}, err
	}
	if view.Task.Revision != task.Revision || view.Item.Revision != item.Revision {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Backup ownership changed during Stop")
	}
	return owner.inputs.PrepareUnstartedStop(ctx, view, at)
}
