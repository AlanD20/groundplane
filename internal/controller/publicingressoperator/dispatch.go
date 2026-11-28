package publicingressoperator

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ingressexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	storage "github.com/AlanD20/groundplane/internal/infra/etcd/publicingress"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (work *Work) Admit(
	ctx context.Context, request workqueue.Assignment, at time.Time,
) (workdispatch.Decision, error) {
	task, item := request.Task.Task, request.Item.Item
	if request.DecisionOnly || !matchesHost(task, item) ||
		request.Executor.Record.Identity.HostID != item.TargetID ||
		request.Executor.Record.Identity.Executor != workexecution.Agent {
		return workdispatch.Decision{}, conflict("public ingress cannot use this Host assignment")
	}
	state, _, err := work.captured(ctx, task, item)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	if request.Direction == workexecution.Recovery {
		if !at.Before(item.Deadline) {
			change, err := workqueue.PrepareCleanupRetryExpiry(request.Task, request.Item, at)
			return workdispatch.Decision{Disposition: workdispatch.NeedsAttention, Change: change}, err
		}
		if !state.Pending.Authorized {
			return workdispatch.Decision{}, conflict("public ingress recovery has no prior authorized effect")
		}
	} else if request.Direction != workexecution.Forward || state.Pending.Authorized {
		return workdispatch.Decision{}, conflict("public ingress requires recovery after a granted activation")
	} else if task.Stop != nil || item.ForwardRevoked || !at.Before(item.Deadline) {
		change, err := work.cancelUnstarted(ctx, request.Task, request.Item, at)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
	} else {
		reviewed, admission, err := work.operator.ingress.PrepareAuthorize(ctx, task.TargetID, task.ID)
		defer keyvalue.ClearMutationValues(admission.Mutations)
		if err != nil {
			kind, _ := errs.KindOf(err)
			if kind != errs.KindStateConflict && kind != errs.KindValidationFailed {
				return workdispatch.Decision{}, err
			}
			change, stopErr := work.cancelUnstarted(ctx, request.Task, request.Item, at)
			return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, stopErr
		}
		if reviewed.Revision != state.Revision {
			return workdispatch.Decision{}, conflict("public ingress changed before Agent assignment")
		}
		// Dispatch checks the same source authority without committing the
		// irreversible grant, which belongs to the native Agent Start CAS.
		return workdispatch.Decision{
			Disposition: workdispatch.Dispatch, Deadline: item.Deadline,
			Change: workqueue.Change{Conditions: admission.Conditions},
		}, nil
	}
	return workdispatch.Decision{
		Disposition: workdispatch.Dispatch, Deadline: item.Deadline, Change: stateGuard(state),
	}, nil
}

func (work *Work) Stop(
	ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot, at time.Time,
) (workqueue.Change, error) {
	if task.Task.Stop == nil || !matchesHost(task.Task, item.Item) {
		return workqueue.Change{}, conflict("public ingress work has no matching stop request")
	}
	if item.Item.AttemptNumber == 0 && item.Item.ActiveAttemptID == "" {
		return work.cancelUnstarted(ctx, task, item, at)
	}
	return workqueue.PrepareRevocation(task, item, task.Task.Stop.Outcome, at)
}

func (work *Work) cancelUnstarted(
	ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot, at time.Time,
) (workqueue.Change, error) {
	_, _, err := work.captured(ctx, task.Task, item.Item)
	if err != nil {
		return workqueue.Change{}, err
	}
	outcome := workexecution.Failed
	if task.Task.Stop != nil {
		outcome = task.Task.Stop.Outcome
	} else if item.Item.ForwardRevoked {
		outcome = workexecution.Superseded
	}
	settlement, err := workqueue.PrepareUnstartedSettlement(task, item, workexecution.Completion{
		Outcome: outcome, Evidence: *item.Item.Input, EffectsSettled: true,
	}, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	cancel, err := work.operator.ingress.PrepareCancel(ctx, item.Item.TargetID, task.Task.ID)
	if err != nil {
		keyvalue.ClearMutationValues(settlement.Mutations)
		return workqueue.Change{}, err
	}
	joined, err := workqueue.JoinChanges(settlement, cancel)
	if err != nil {
		keyvalue.ClearMutationValues(settlement.Mutations)
		keyvalue.ClearMutationValues(cancel.Mutations)
	}
	return joined, err
}

func (work *Work) AuthorizeAgentStart(ctx context.Context, view workqueue.ResumeView) (workqueue.Change, error) {
	if !view.CurrentExecutor || !view.ActiveAttempt || view.Attempt.Attempt.StartedAt != nil ||
		workexecution.ValidateDelivery(view.Work.Item.Item, view.Attempt.Attempt) != nil {
		return workqueue.Change{}, conflict("public ingress work has no fresh Agent assignment")
	}
	task, item := view.Work.Task.Task, view.Work.Item.Item
	state, _, err := work.captured(ctx, task, item)
	if err != nil {
		return workqueue.Change{}, err
	}
	if view.Attempt.Attempt.Direction == workexecution.Recovery {
		if !state.Pending.Authorized {
			return workqueue.Change{}, conflict("public ingress recovery has no prior authorized effect")
		}
		return stateGuard(state), nil
	}
	if state.Pending.Authorized {
		return workqueue.Change{}, conflict("public ingress already has an authorized effect")
	}
	reviewed, change, err := work.operator.ingress.PrepareAuthorize(ctx, task.TargetID, task.ID)
	if err != nil {
		return workqueue.Change{}, err
	}
	if reviewed.Revision != state.Revision {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, conflict("public ingress changed before Agent start")
	}
	return change, nil
}

func (work *Work) LoadWorkAssignment(
	ctx context.Context, view workqueue.ResumeView,
) (*agentpb.PublicIngressAssignment, error) {
	attempt, item := view.Attempt.Attempt, view.Work.Item.Item
	if !view.CurrentExecutor || !view.ActiveAttempt || workexecution.ValidateDelivery(item, attempt) != nil {
		return nil, conflict("public ingress work has no retained Agent assignment")
	}
	_, plan, err := work.captured(ctx, view.Work.Task.Task, item)
	if err != nil {
		return nil, err
	}
	return ingressexecution.EncodeAssignment(attempt, plan)
}

func (work *Work) captured(
	ctx context.Context, task workexecution.Task, item workexecution.Item,
) (storage.State, ingressexecution.Plan, error) {
	if !matchesHost(task, item) {
		return storage.State{}, ingressexecution.Plan{}, conflict(
			"public ingress operation has a different Host or input",
		)
	}
	state, err := work.operator.ingress.Read(ctx, item.TargetID)
	if err != nil {
		return storage.State{}, ingressexecution.Plan{}, err
	}
	if state.Pending == nil || state.Pending.TaskID != task.ID {
		return storage.State{}, ingressexecution.Plan{}, conflict("public ingress publication is no longer pending")
	}
	plan := planFrom(state)
	ref, err := planReference(plan, task.CreatedAt)
	if err != nil {
		return storage.State{}, ingressexecution.Plan{}, err
	}
	if ref != *item.Input {
		return storage.State{}, ingressexecution.Plan{}, conflict("public ingress plan changed since Task preparation")
	}
	return state, plan, nil
}

func matchesHost(task workexecution.Task, item workexecution.Item) bool {
	return task.Action == TaskAction && item.Operation == workexecution.PublicIngress &&
		item.TaskID == task.ID && item.TargetID == task.TargetID && item.Input != nil && *item.Input == task.Intent &&
		item.Placement.Executor == workexecution.Agent && item.Placement.HostID == task.TargetID
}
