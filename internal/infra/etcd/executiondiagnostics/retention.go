package executiondiagnostics

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type expiryProgress struct {
	TaskID       string `json:"task_id"`
	TaskRevision int64  `json:"task_revision"`
	AfterWork    string `json:"after_work,omitempty"`
	AfterAttempt uint64 `json:"after_attempt"`
	Complete     bool   `json:"complete"`
}

func TaskExpiryKey(taskID string) string { return "/v1/diagnostics/expiry/" + taskID }

func readExpiry(ctx context.Context, store Store, taskID string, revision int64) (*expiryProgress, int64, error) {
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{TaskExpiryKey(taskID)}, Revision: revision})
	if err != nil {
		return nil, 0, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return nil, 0, invalid()
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return nil, 0, nil
	}
	progress, err := recordcodec.Decode[expiryProgress](read.Values[0].Value, "diagnostic_expiry")
	if err != nil || progress.TaskID != taskID || progress.TaskRevision <= 0 ||
		progress.AfterWork != "" && ids.Validate(ids.KindWorkItem, progress.AfterWork) != nil {
		return nil, 0, invalid()
	}
	return &progress, read.Values[0].ModRevision, nil
}

// TaskExpired checks the exact terminal Task revision. Explicit Retry changes
// that revision, so it cannot inherit a previous run's completed expiry cursor.
func TaskExpired(ctx context.Context, store Store, task workqueue.TaskSnapshot, revision int64) (bool, error) {
	progress, _, err := readExpiry(ctx, store, task.Task.ID, revision)
	return progress != nil && progress.Complete && progress.TaskRevision == task.Revision, err
}

// ExpireTask advances one work item and at most four attempts. It removes only
// diagnostic records; exact attempts, checkpoints, failures and resource state
// remain untouched. A durable cursor bounds restart and large Blueprint work.
func (repository *Repository) ExpireTask(ctx context.Context, taskID string, at time.Time, retention time.Duration) (bool, error) {
	if retention < time.Hour || retention > workexecution.TaskHistoryRetention || at.IsZero() {
		return false, invalid()
	}
	task, revision, err := repository.queue.ReadTask(ctx, taskID, 0)
	if err != nil {
		return false, err
	}
	if task.Task.FinishedAt == nil || task.Task.CollectionStartedAt != nil || at.Before(task.Task.FinishedAt.Add(retention)) {
		return false, nil
	}
	previous, previousRevision, err := readExpiry(ctx, repository.store, taskID, revision)
	if err != nil {
		return false, err
	}
	next := expiryProgress{TaskID: taskID, TaskRevision: task.Revision}
	if previous != nil && previous.TaskRevision == task.Revision {
		next = *previous
	}
	if next.Complete {
		return false, nil
	}
	page, err := repository.queue.ListWork(ctx, taskID, next.AfterWork, 1, revision)
	if err != nil {
		return false, err
	}
	conditions := []keyvalue.Condition{
		{Key: workqueue.TaskKey(taskID), ModRevision: task.Revision},
		{Key: TaskExpiryKey(taskID), ModRevision: previousRevision},
	}
	mutations := []keyvalue.Mutation{}
	if len(page.Items) != 0 {
		item := page.Items[0]
		attempts, err := repository.queue.ListAttempts(ctx, taskID, item.Item.ID, next.AfterAttempt, 4, revision)
		if err != nil {
			return false, err
		}
		conditions = append(conditions, keyvalue.Condition{Key: workqueue.ItemKey(item.Item.ID), ModRevision: item.Revision})
		for _, attempt := range attempts.Attempts {
			if attempt.Attempt.FinishedAt == nil {
				return false, invalid()
			}
			conditions = append(conditions, keyvalue.Condition{Key: workqueue.AttemptKey(attempt.Attempt.ID), ModRevision: attempt.Revision})
			mutations = append(mutations,
				keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: Key(attempt.Attempt.ID)},
				keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: EventsKey(attempt.Attempt.ID)},
				keyvalue.Mutation{Type: keyvalue.MutationPut, Key: LossKey(attempt.Attempt.ID), Value: []byte("expired")})
		}
		next.AfterAttempt = attempts.LastNumber
		if !attempts.More {
			next.AfterWork, next.AfterAttempt = page.LastID, 0
			next.Complete = !page.More
		}
	} else {
		next.Complete = true
	}
	// Keep the event high-water mark even when its entire disposable tail is
	// gone. A later explicit Retry must not reuse an earlier stream sequence.
	if next.Complete {
		events, eventsRevision, err := repository.readTaskEvents(ctx, taskID, revision)
		if err != nil {
			return false, err
		}
		if events != nil {
			events.Items, events.Expired = []TaskEvent{}, true
			value, err := recordcodec.Encode("task_execution_events", events)
			if err != nil {
				return false, err
			}
			conditions = append(conditions, keyvalue.Condition{Key: TaskEventsKey(taskID), ModRevision: eventsRevision})
			mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: TaskEventsKey(taskID), Value: value})
		}
	}
	value, err := recordcodec.Encode("diagnostic_expiry", next)
	if err != nil {
		keyvalue.ClearMutationValues(mutations)
		return false, err
	}
	mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: TaskExpiryKey(taskID), Value: value})
	defer keyvalue.ClearMutationValues(mutations)
	result, err := repository.store.Transact(ctx, conditions, mutations)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return false, err
	}
	if !result.Succeeded {
		return false, errs.New(errs.KindStateConflict, "Task changed while expiring disposable diagnostics")
	}
	return next.Complete, nil
}
