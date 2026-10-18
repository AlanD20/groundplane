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

// PrepareRecoveryRetry grants a bounded inspection of the same failure. It
// cannot create credentials or repeat an ambiguous Custom hook.
func (repository *InputRepository) PrepareRecoveryRetry(ctx context.Context, view workqueue.View, previous workqueue.AttemptSnapshot, at time.Time) (workqueue.Change, error) {
	task, item, attempt := view.Task.Task, view.Item.Item, previous.Attempt
	if task.CollectionStartedAt != nil || item.Operation != workexecution.Attach || item.Placement.Executor != workexecution.Agent ||
		item.Phase != workexecution.Attention || item.ActiveAttemptID != "" || item.Input == nil || item.TaskID != task.ID ||
		attempt.TaskID != task.ID || attempt.WorkItemID != item.ID || attempt.Executor.HostID != item.Placement.HostID ||
		attempt.Executor.Executor != workexecution.Agent || attempt.FinishedAt == nil || attempt.Result == nil ||
		attempt.Input != *item.Input || attempt.Number != item.AttemptNumber || previous.Revision <= 0 || previous.Revision > view.ReadRevision ||
		item.Cleanup != workexecution.CleanupRequired && item.Cleanup != workexecution.CleanupStopped {
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "Attach has no captured unresolved execution to inspect")
	}
	saved, err := repository.LoadRecovery(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	if attempt.Direction == workexecution.Forward {
		if saved.Source.Forward.ID != attempt.ID || saved.Source.Evidence != *attempt.Result || item.Result == nil || *item.Result != *attempt.Result {
			return workqueue.Change{}, checkpointConflict("Attach inspection Retry lost its original failure")
		}
	} else {
		if item.CleanupResult == nil || *item.CleanupResult != *attempt.Result || saved.Source.ValidateFor(attempt) != nil {
			return workqueue.Change{}, checkpointConflict("Attach inspection Retry lost its preceding result")
		}
		if _, err := repository.LoadRecoveryResult(ctx, workqueue.ResumeView{Work: view, Attempt: previous}); err != nil {
			return workqueue.Change{}, err
		}
	}
	source, err := repository.LoadCaptured(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer source.Clear()
	change, err := workqueue.PrepareRetry(view.Task, view.Item, previous, workexecution.RetryDecision{
		Direction: workexecution.Recovery, Result: *attempt.Result, NotBefore: at, Deadline: at.Add(attachexecution.InspectionBudget),
	}, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, source.Conditions...)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: RecoveryKey(item.ID), ModRevision: saved.Revision})
	return change, nil
}
