package attachprovision

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareForwardRetry is used only by an explicit operator action. Built-in
// provisioning reuses its captured identity; Custom hook idempotency remains the
// operator's responsibility. Neither may overlap unresolved previous execution.
func (repository *InputRepository) PrepareForwardRetry(ctx context.Context, view workqueue.View, previous workqueue.AttemptSnapshot, at time.Time) (workqueue.Change, error) {
	item, attempt := view.Item.Item, previous.Attempt
	if item.Operation != workexecution.Attach || item.Phase != workexecution.Settled || item.Outcome != workexecution.Failed ||
		item.Input == nil || attempt.Result == nil || attempt.FinishedAt == nil || attempt.Input != *item.Input ||
		attempt.TaskID != view.Task.Task.ID || attempt.WorkItemID != item.ID || attempt.Number != item.AttemptNumber ||
		previous.Revision <= 0 || previous.Revision > view.ReadRevision || item.ForwardRevoked ||
		view.Task.Task.Stop != nil || view.Task.Task.CollectionStartedAt != nil {
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "Attach provisioning has no settled failure to retry")
	}
	current, guards, err := repository.ReadDesiredMatch(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	if !current {
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "Attach provisioning has been superseded; submit a new Apply")
	}
	resume := workqueue.ResumeView{Work: view, Attempt: previous}
	budget := attempt.Deadline.Sub(attempt.AssignedAt)
	if attempt.Direction == workexecution.Forward {
		raw, err := repository.LoadFailure(ctx, resume)
		if err != nil {
			return workqueue.Change{}, err
		}
		failure, err := attachexecution.DecodeProvisionFailure(attempt, raw, *attempt.Result)
		if err != nil || failure.Checkpoint != nil || item.Cleanup != workexecution.CleanupNone {
			return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "Inspect the previous Attach execution before retrying provisioning")
		}
	} else {
		if attempt.Outcome != workexecution.Succeeded || item.Cleanup != workexecution.CleanupComplete {
			return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "Attach execution is not proven stopped")
		}
		if _, err := repository.LoadRecoveryResult(ctx, resume); err != nil {
			return workqueue.Change{}, err
		}
		saved, err := repository.LoadRecovery(ctx, view)
		if err != nil {
			return workqueue.Change{}, err
		}
		budget = saved.Source.Forward.Deadline.Sub(saved.Source.Forward.AssignedAt)
		guards = append(guards, keyvalue.Condition{Key: RecoveryKey(item.ID), ModRevision: saved.Revision})
	}
	if budget <= 0 {
		return workqueue.Change{}, checkpointConflict("Attach Retry has no captured execution budget")
	}
	binding, err := repository.prepareRetryBinding(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareRetry(view.Task, view.Item, previous, workexecution.RetryDecision{
		Direction: workexecution.Forward, Result: *attempt.Result, NotBefore: at, Deadline: at.Add(budget),
	}, at)
	if err != nil {
		keyvalue.ZeroMutationBytes(binding.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, guards...)
	change.Conditions = append(change.Conditions, binding.Conditions...)
	change.Mutations = append(change.Mutations, binding.Mutations...)
	return change, nil
}
