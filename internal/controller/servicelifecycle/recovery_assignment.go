package servicelifecycle

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/servicelifecyclework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (owner *AgentWork) admitRecovery(ctx context.Context, request workqueue.Assignment, at time.Time) (workdispatch.Decision, error) {
	item := request.Item.Item
	if request.DecisionOnly || item.Operation != workexecution.ServiceLifecycle || item.Placement.Executor != workexecution.Agent ||
		item.Phase != workexecution.Ready || item.ActiveAttemptID != "" || at.IsZero() || at.Before(item.UpdatedAt) ||
		(item.Cleanup != workexecution.CleanupRequired && item.Cleanup != workexecution.CleanupStopped) ||
		request.Executor.Record.Identity.HostID != item.Placement.HostID {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Service lifecycle recovery cannot obtain this host assignment")
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
	input, err := BuildWorkInput(captured)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	saved, err := owner.inputs.LoadRecovery(ctx, view, input)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	return workdispatch.Decision{Disposition: workdispatch.Dispatch, Deadline: item.Deadline,
		Change: workqueue.Change{Conditions: []keyvalue.Condition{guard,
			{Key: servicelifecyclework.RecoveryKey(item.ID), ModRevision: saved.Revision},
		}}}, nil
}

func (owner *AgentWork) recoveryAssignment(ctx context.Context, view workqueue.ResumeView) (*agentpb.ServiceLifecycleAssignment, workqueue.Change, error) {
	item, task, attempt := view.Work.Item.Item, view.Work.Task.Task, view.Attempt.Attempt
	if !view.CurrentExecutor || !view.ActiveAttempt || item.Operation != workexecution.ServiceLifecycle ||
		attempt.Direction != workexecution.Recovery || attempt.Executor.Executor != workexecution.Agent ||
		workexecution.ValidateDelivery(item, attempt) != nil || !task.PublicationComplete || task.FinishedAt != nil || task.CollectionStartedAt != nil {
		return nil, workqueue.Change{}, errs.New(errs.KindStateConflict, "Service lifecycle recovery no longer owns active host execution")
	}
	captured, guard, err := owner.inputs.Load(ctx, view.Work)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	input, err := BuildWorkInput(captured)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	saved, err := owner.inputs.LoadRecovery(ctx, view.Work, input)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	if err := saved.Source.ValidateFor(attempt, input); err != nil {
		return nil, workqueue.Change{}, err
	}
	raw, err := lifecycleexecution.EncodeRecovery(input, saved.Source)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	identity, err := workexecution.EncodeAgentAttempt(attempt)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	change := workqueue.Change{Conditions: []keyvalue.Condition{guard,
		{Key: servicelifecyclework.RecoveryKey(item.ID), ModRevision: saved.Revision},
		{Key: workqueue.TaskKey(task.ID), ModRevision: view.Work.Task.Revision},
		{Key: workqueue.ItemKey(item.ID), ModRevision: view.Work.Item.Revision},
	}}
	return &agentpb.ServiceLifecycleAssignment{Attempt: identity, Input: input, RecoverySource: raw}, change, nil
}
