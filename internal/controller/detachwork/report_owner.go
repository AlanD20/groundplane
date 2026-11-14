package detachwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (owner *Owner) RemovalInput(ctx context.Context, view workqueue.ResumeView) (attachexecution.RemovalInput, error) {
	return owner.forwardInput(ctx, view)
}

func (owner *Owner) RecoveryInput(
	ctx context.Context,
	view workqueue.ResumeView,
) (attachexecution.RecoveryInput, error) {
	input, _, err := owner.inputs.DetachRecoveryInput(ctx, view.Work, view.Attempt.Attempt)
	return input, err
}

func (owner *Owner) PrepareRemovalCheckpoint(ctx context.Context, view workqueue.ResumeView,
	input attachexecution.RemovalInput, report attachexecution.RemovalReport,
) (workqueue.Change, error) {
	return owner.inputs.PrepareDetachCheckpoint(ctx, view, input, report)
}

func (owner *Owner) PrepareRemovalCompletion(ctx context.Context, view workqueue.ResumeView,
	input attachexecution.RemovalInput, at time.Time,
) (workqueue.Change, error) {
	change, err := owner.inputs.PrepareDetachCompletion(ctx, view, input, at)
	if err != nil || view.Work.Task.Task.Action != workexecution.ActionApply {
		return change, err
	}
	receipt, err := owner.units.PrepareDetachCompletionReceipt(ctx, view)
	return joinBlueprintReceipt(change, receipt, err)
}

func (owner *Owner) PrepareRemovalFailure(ctx context.Context, view workqueue.ResumeView,
	input attachexecution.RemovalInput, reason attachexecution.FailureReason, at time.Time,
) (workqueue.Change, workexecution.Completion, error) {
	return owner.inputs.PrepareDetachFailure(ctx, view, input, reason, at, false)
}

func (owner *Owner) LoadRemovalFailure(ctx context.Context, view workqueue.ResumeView) ([]byte, error) {
	return owner.inputs.LoadDetachFailure(ctx, view)
}

func (owner *Owner) PrepareRemovalInspection(ctx context.Context, view workqueue.ResumeView,
	report attachexecution.RemovalInspection, at time.Time,
) (workqueue.Change, workexecution.Completion, error) {
	change, completion, err := owner.inputs.PrepareDetachInspection(ctx, view, report, at, false)
	if err != nil || !completion.EffectsSettled || view.Work.Task.Task.Action != workexecution.ActionApply {
		return change, completion, err
	}
	receipt, err := owner.units.PrepareDetachRecoveryReceipt(ctx, view, completion.Evidence)
	change, err = joinBlueprintReceipt(change, receipt, err)
	return change, completion, err
}

func (owner *Owner) LoadRemovalInspection(ctx context.Context, view workqueue.ResumeView) ([]byte, error) {
	return owner.inputs.LoadDetachInspection(ctx, view)
}
