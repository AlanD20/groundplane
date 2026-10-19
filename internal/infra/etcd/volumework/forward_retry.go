package volumework

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *InputRepository) PrepareForwardRetry(
	ctx context.Context,
	view workqueue.View,
	previous workqueue.AttemptSnapshot,
	at time.Time,
) (workqueue.Change, error) {
	task, item, attempt := view.Task.Task, view.Item.Item, previous.Attempt
	if item.Operation != workexecution.Volume || task.Stop != nil || task.CollectionStartedAt != nil ||
		item.ForwardRevoked ||
		item.Phase != workexecution.Settled ||
		item.Outcome != workexecution.Failed ||
		item.Input == nil ||
		item.Result == nil ||
		attempt.Result == nil ||
		attempt.FinishedAt == nil ||
		attempt.Input != *item.Input ||
		attempt.Number != item.AttemptNumber ||
		attempt.Executor.HostID != item.Placement.HostID {
		return workqueue.Change{}, errs.New(
			errs.KindTaskNotRetryable,
			"Volume Retry requires a failed action with settled effects",
		)
	}
	input, guards, err := repository.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	desired, err := repository.guardRetryDesired(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	guards = append(guards, desired...)
	if attempt.Direction == workexecution.Recovery {
		if item.Cleanup != workexecution.CleanupComplete || item.CleanupResult == nil ||
			*item.CleanupResult != *attempt.Result {
			return workqueue.Change{}, errs.New(
				errs.KindTaskNotRetryable,
				"Volume Retry requires completed inspection of its interrupted effect",
			)
		}
		resume := workqueue.ResumeView{Work: view, Attempt: previous}
		_, completed, err := repository.LoadRecoveryResult(ctx, resume)
		if err != nil {
			return workqueue.Change{}, err
		}
		if !completed {
			return workqueue.Change{}, errs.New(
				errs.KindTaskNotRetryable,
				"Volume recovery has not proved the interrupted stage",
			)
		}
		source, err := repository.LoadRecoveryInput(ctx, view, attempt)
		if err != nil {
			return workqueue.Change{}, err
		}
		if source.Source.Evidence != *item.Result {
			return workqueue.Change{}, conflict("Volume Retry original failure differs from its recovery source")
		}
		guards = append(guards, source.Conditions...)
		// Immutable recovery evidence cannot change; Task/item/attempt CAS protects
		// it from retention during this same Retry publication.
	} else {
		if attempt.Direction != workexecution.Forward || item.Cleanup != workexecution.CleanupNone || *item.Result != *attempt.Result {
			return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "Volume Retry has no settled forward failure")
		}
		read, err := repository.read(ctx, []string{FailureKey(attempt.ID)}, view.ReadRevision)
		if err != nil {
			return workqueue.Change{}, err
		}
		defer keyvalue.ClearValues(read.Values)
		if read.Values[0] == nil {
			return workqueue.Change{}, conflict("Volume Retry failure evidence is unavailable")
		}
		failure, err := volumeexecution.DecodeFailure(attempt, read.Values[0].Value, *attempt.Result)
		if err != nil {
			return workqueue.Change{}, err
		}
		if failure.Checkpoint != nil && failure.Checkpoint.Sequence%2 != 0 {
			return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "Volume Retry requires inspection of the interrupted stage first")
		}
		guards = append(guards, keyvalue.Condition{Key: FailureKey(attempt.ID), ModRevision: read.Values[0].ModRevision})
	}
	budget, err := volumeexecution.ExecutionBudget(input.Action)
	if err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareRetry(view.Task, view.Item, previous, workexecution.RetryDecision{
		Direction: workexecution.Forward, Result: *attempt.Result, NotBefore: at, Deadline: at.Add(budget),
	}, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, guards...)
	return change, nil
}
