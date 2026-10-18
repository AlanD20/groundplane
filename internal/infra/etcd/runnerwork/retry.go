package runnerwork

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareRetry keeps the captured input and public identity. It admits only a
// settled forward failure: unknown external effects must be resolved first.
// Fresh registration-token delivery is a separate in-memory admission condition.
func PrepareRetry(input keyvalue.Versioned[Input], task workqueue.TaskSnapshot,
	item workqueue.ItemSnapshot, previous workqueue.AttemptSnapshot, at time.Time,
) (workqueue.Change, error) {
	work := item.Item
	if input.Revision <= 0 || input.Record.MatchWork(work) != nil ||
		input.Record.Operation != runnerallocation.RuntimeOperationCreate ||
		task.Task.CollectionStartedAt != nil || task.Task.Stop != nil ||
		work.Phase != workexecution.Settled || work.Outcome != workexecution.Failed ||
		work.ActiveAttemptID != "" || work.ForwardRevoked || work.AttemptNumber == ^uint64(0) ||
		(work.Cleanup != workexecution.CleanupNone && work.Cleanup != workexecution.CleanupComplete) {
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "Runner creation must finish cleanup before retrying with a fresh registration token")
	}
	deadline := at.Add(5 * time.Minute)
	var change workqueue.Change
	var err error
	if work.AttemptNumber == 0 {
		change, err = workqueue.PrepareUnstartedRetry(task, item, deadline, at)
	} else {
		if previous.Attempt.Number != work.AttemptNumber || previous.Attempt.Result == nil {
			return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "Runner retry requires the last recorded attempt")
		}
		change, err = workqueue.PrepareRetry(task, item, previous, workexecution.RetryDecision{
			Direction: workexecution.Forward, Result: *previous.Attempt.Result,
			NotBefore: at, Deadline: deadline,
		}, at)
	}
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: InputKey(work.ID), ModRevision: input.Revision})
	return change, nil
}
