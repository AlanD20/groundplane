package imageoperations

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareRetry reopens only a settled Fetch of the same captured content. It
// never resolves the requested tag again or changes its latest-selection fence.
// Unknown tag/removal effects and successful operations cannot grant a retry.
func (repository *Repository) PrepareRetry(ctx context.Context, view workqueue.View,
	previous workqueue.AttemptSnapshot, at time.Time,
) (workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Stop != nil || task.CollectionStartedAt != nil || item.Operation != workexecution.Image ||
		item.Input == nil || item.Result == nil || item.ForwardRevoked || item.ActiveAttemptID != "" ||
		item.Phase != workexecution.Settled || item.Outcome != workexecution.Failed || item.Cleanup != workexecution.CleanupNone {
		return workqueue.Change{}, notRetryable()
	}
	loaded, err := repository.LoadAt(ctx, task.ID, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	record := loaded.Record
	input, err := record.Input.Reference()
	if err != nil {
		return workqueue.Change{}, err
	}
	if record.Input.Fetch == nil || record.Phase != Finished || record.Outcome != workexecution.Failed ||
		record.Input.WorkItemID != item.ID || input != *item.Input || input != task.Intent ||
		record.Input.Target() != item.TargetID || record.Input.Placement != item.Placement || at.Before(record.UpdatedAt) {
		return workqueue.Change{}, notRetryable()
	}
	value, err := Encode(record)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(value)
	digest := sha256.Sum256(value)
	guards := []keyvalue.Condition{{Key: Key(task.ID), ModRevision: loaded.Revision}}
	resultID := ids.DeriveAt(ids.KindPlan, record.Input.CreatedAt, item.ID, "image-unstarted-result")
	if item.AttemptNumber == 0 {
		if record.AttemptID != "" || previous.Revision != 0 {
			return workqueue.Change{}, notRetryable()
		}
	} else {
		attempt := previous.Attempt
		if previous.Revision <= 0 || attempt.ID != record.AttemptID || attempt.TaskID != task.ID ||
			attempt.WorkItemID != item.ID || attempt.Number != item.AttemptNumber || attempt.Input != input ||
			attempt.Direction != workexecution.Forward || attempt.FinishedAt == nil || attempt.Outcome != workexecution.Failed ||
			attempt.Result == nil || *attempt.Result != *item.Result ||
			attempt.Executor.Executor != item.Placement.Executor || attempt.Executor.HostID != item.Placement.HostID {
			return workqueue.Change{}, notRetryable()
		}
		read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
			Keys: []string{ResultKey(attempt.ID)}, Revision: view.ReadRevision})
		if err != nil {
			return workqueue.Change{}, err
		}
		if read == nil {
			return workqueue.Change{}, conflict()
		}
		defer keyvalue.ClearValues(read.Values)
		if read.ReadRevision != view.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil ||
			!bytes.Equal(read.Values[0].Value, value) {
			return workqueue.Change{}, notRetryable()
		}
		guards = append(guards, keyvalue.Condition{Key: ResultKey(attempt.ID), ModRevision: read.Values[0].ModRevision})
		resultID = ids.DeriveAt(ids.KindPlan, record.Input.CreatedAt, attempt.ID, "image-result")
	}
	if *item.Result != (workexecution.Input{ID: resultID, SHA256: hex.EncodeToString(digest[:])}) {
		return workqueue.Change{}, notRetryable()
	}
	var change workqueue.Change
	if item.AttemptNumber == 0 {
		change, err = workqueue.PrepareUnstartedRetry(view.Task, view.Item, at.Add(operationTimeout), at)
	} else {
		change, err = workqueue.PrepareRetry(view.Task, view.Item, previous, workexecution.RetryDecision{
			Direction: workexecution.Forward, Result: *item.Result, NotBefore: at, Deadline: at.Add(operationTimeout),
		}, at)
	}
	if err != nil {
		return workqueue.Change{}, err
	}
	// Previous attempt receipts and diagnostics remain immutable. Only this
	// operation's mutable cursor is reset in the same transaction as scheduling.
	next := Record{Input: record.Input, Phase: Ready, UpdatedAt: at}
	encoded, err := Encode(next)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, guards...)
	change.Mutations = append(
		change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: Key(task.ID), Value: encoded},
	)
	return change, nil
}

func notRetryable() error {
	return errs.New(
		errs.KindTaskNotRetryable,
		"image Retry requires a failed Fetch with proof that its effects are settled",
	)
}
