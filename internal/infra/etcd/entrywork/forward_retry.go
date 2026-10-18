package entrywork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/entryexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Forward retry retains the captured files and consumers. Only an action with
// no authorized effect, or one with proved restoration, may execute again.
func (repository *InputRepository) PrepareForwardRetry(ctx context.Context, view workqueue.View, previous workqueue.AttemptSnapshot, at time.Time) (workqueue.Change, error) {
	item, attempt := view.Item.Item, previous.Attempt
	if item.Operation != workexecution.EntryConfiguration || item.Phase != workexecution.Settled || item.Outcome != workexecution.Failed ||
		item.Input == nil || attempt.Result == nil || attempt.FinishedAt == nil || attempt.Input != *item.Input ||
		view.Task.Task.Stop != nil || view.Task.Task.CollectionStartedAt != nil {
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "Entry execution has no settled failure to retry")
	}
	current, guards, err := repository.ReadDesiredMatch(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	if !current {
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "Entry configuration has been superseded; submit a new action")
	}
	input, _, _, err := repository.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	resume := workqueue.ResumeView{Work: view, Attempt: previous}
	key := FailureKey(attempt.ID)
	if attempt.Direction == workexecution.Forward {
		raw, err := repository.LoadFailure(ctx, resume)
		if err != nil {
			return workqueue.Change{}, err
		}
		defer clear(raw)
		failure, err := entryexecution.DecodeFailure(attempt, input, raw, *attempt.Result)
		if err != nil || failure.Checkpoint != nil || item.Cleanup != workexecution.CleanupNone {
			return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "Entry effects require restoration before Retry")
		}
	} else {
		if attempt.Outcome != workexecution.Succeeded || item.Cleanup != workexecution.CleanupComplete {
			return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "Entry restoration is incomplete")
		}
		raw, err := repository.LoadRecoveryResult(ctx, resume)
		if err != nil {
			return workqueue.Change{}, err
		}
		clear(raw)
		key = RecoveryResultKey(attempt.ID)
	}
	read, err := repository.read(ctx, []string{key}, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return workqueue.Change{}, conflict("Entry Retry result is unavailable")
	}
	resources, err := repository.AdmissionConditions(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	budget := attempt.Deadline.Sub(attempt.AssignedAt)
	if budget <= 0 {
		return workqueue.Change{}, conflict("Entry Retry has no execution budget")
	}
	change, err := workqueue.PrepareRetry(view.Task, view.Item, previous, workexecution.RetryDecision{Direction: workexecution.Forward, Result: *attempt.Result, NotBefore: at, Deadline: at.Add(budget)}, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, guards...)
	change.Conditions = append(change.Conditions, resources...)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision})
	return change, nil
}
