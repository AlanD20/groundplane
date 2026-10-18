package runner

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Forward Runner retries require a fresh registration token through the Runner
// action. Ordinary Task Retry may only resume captured, stopped cleanup.
func (executor *Executor) PrepareWorkRetry(ctx context.Context, task workqueue.TaskSnapshot,
	item workqueue.ItemSnapshot, previous workqueue.AttemptSnapshot, revision int64, at time.Time,
) (workqueue.Change, error) {
	work := item.Item
	if work.Operation != workexecution.Runner || work.Phase != workexecution.Attention ||
		(work.Cleanup != workexecution.CleanupRequired && work.Cleanup != workexecution.CleanupStopped) ||
		work.ActiveAttemptID != "" || task.Task.CollectionStartedAt != nil ||
		previous.Revision <= 0 || previous.Attempt.Number != work.AttemptNumber || previous.Attempt.FinishedAt == nil || previous.Attempt.Result == nil {
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "Use Runner Retry with a fresh registration token; Task Retry is for stopped cleanup only")
	}
	input, err := executor.inputs.Load(ctx, work.ID, revision)
	if err != nil {
		return workqueue.Change{}, err
	}
	if err := input.Record.MatchWork(work); err != nil {
		return workqueue.Change{}, err
	}
	result, err := executor.inputs.LoadResult(ctx, previous.Attempt, revision)
	if err != nil {
		return workqueue.Change{}, err
	}
	completion, err := result.Record.Completion(input.Record)
	if err != nil || completion.EffectsSettled || completion.Evidence != *previous.Attempt.Result {
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "Runner cleanup lacks the preceding unresolved result")
	}
	change, err := workqueue.PrepareRetry(task, item, previous, workexecution.RetryDecision{
		Direction: workexecution.Recovery, Result: completion.Evidence, NotBefore: at, Deadline: at.Add(5 * time.Minute),
	}, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions,
		keyvalue.Condition{Key: runnerwork.InputKey(work.ID), ModRevision: input.Revision},
		keyvalue.Condition{Key: runnerwork.ResultKey(previous.Attempt.ID), ModRevision: result.Revision})
	return change, nil
}
