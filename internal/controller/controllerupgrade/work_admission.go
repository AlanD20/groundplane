package controllerupgrade

import (
	"context"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/controllersoftware"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (owner *Work) Admit(ctx context.Context, request workqueue.Assignment, at time.Time) (workdispatch.Decision, error) {
	capture, err := owner.inputs.Load(ctx, request.Item.Item, request.ReadRevision)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	if !at.Before(request.Item.Item.Deadline) || owner.coordinator == nil {
		if request.Direction == workexecution.Recovery {
			change, err := workqueue.PrepareCleanupRetryExpiry(request.Task, request.Item, at)
			if at.Before(request.Item.Item.Deadline) {
				return workdispatch.Decision{}, bootstrapUnavailable()
			}
			change.Conditions = append(change.Conditions, captureGuard(capture).Conditions...)
			return workdispatch.Decision{Disposition: workdispatch.NeedsAttention, Change: change}, err
		}
		change, err := owner.settleUnstarted(ctx, request.Task, request.Item, capture, workexecution.Failed, at)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
	}
	deadline := request.Item.Item.Deadline
	if request.Direction == workexecution.Forward && at.Add(upgrade.TaskTimeoutSeconds*time.Second).Before(deadline) {
		deadline = at.Add(upgrade.TaskTimeoutSeconds * time.Second)
	}
	return workdispatch.Decision{Disposition: workdispatch.Dispatch, Deadline: deadline, Change: captureGuard(capture)}, nil
}

func (owner *Work) Stop(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot, at time.Time) (workqueue.Change, error) {
	if task.Task.Stop == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Controller activation has no stop request")
	}
	capture, err := owner.inputs.Load(ctx, item.Item, 0)
	if err != nil {
		return workqueue.Change{}, err
	}
	if item.Item.AttemptNumber == 0 {
		return owner.settleUnstarted(ctx, task, item, capture, task.Task.Stop.Outcome, at)
	}
	// The native coordinator observes the durable stop at its safe boundary.
	// After cutover, qualification/recovery must continue; completion may win.
	if owner.coordinator != nil {
		if err := owner.coordinator.RequestAbort(ctx, task.Task.ID); err != nil {
			return workqueue.Change{}, err
		}
	}
	return workqueue.PrepareRevocation(task, item, task.Task.Stop.Outcome, at)
}

func (owner *Work) settleUnstarted(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot,
	capture keyvalue.Versioned[controllersoftware.Capture], outcome workexecution.Outcome, at time.Time,
) (workqueue.Change, error) {
	reference, err := capture.Record.Reference()
	if err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareUnstartedSettlement(task, item, workexecution.Completion{Outcome: outcome, EffectsSettled: true, Evidence: reference}, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	projection, err := owner.inputs.PrepareCurrent(ctx, capture, capture.ReadRevision, false, outcome, true)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, captureGuard(capture).Conditions...)
	change.Conditions = append(change.Conditions, projection.Conditions...)
	change.Mutations = append(change.Mutations, projection.Mutations...)
	return change, nil
}

func (owner *Work) PrepareWorkRetry(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot,
	previous workqueue.AttemptSnapshot, revision int64, at time.Time,
) (workqueue.Change, error) {
	if owner.coordinator == nil {
		return workqueue.Change{}, bootstrapUnavailable()
	}
	if item.Item.Phase != workexecution.Attention || previous.Attempt.Result == nil {
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "Controller activation cannot be replayed; Retry is available only for captured handoff reconciliation")
	}
	capture, err := owner.inputs.Load(ctx, item.Item, revision)
	if err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareRetry(task, item, previous, workexecution.RetryDecision{Direction: workexecution.Recovery,
		Result: *previous.Attempt.Result, NotBefore: at, Deadline: at.Add(upgrade.RecoveryReserveSeconds * time.Second)}, at)
	change.Conditions = append(change.Conditions, captureGuard(capture).Conditions...)
	return change, err
}
