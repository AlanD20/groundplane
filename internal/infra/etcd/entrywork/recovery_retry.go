package entrywork

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/entryexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

// PrepareRecoveryRetry retains the original forward failure across repeated
// cleanup attempts. It restores captured runtime/configuration, never newer intent.
func (repository *InputRepository) PrepareRecoveryRetry(ctx context.Context, view workqueue.View, previous workqueue.AttemptSnapshot, at time.Time) (workqueue.Change, error) {
	task, item, attempt := view.Task.Task, view.Item.Item, previous.Attempt
	if task.CollectionStartedAt != nil || item.Operation != workexecution.EntryConfiguration || item.Placement.Executor != workexecution.Agent ||
		item.Phase != workexecution.Attention || item.ActiveAttemptID != "" || item.Input == nil ||
		item.TaskID != task.ID || attempt.TaskID != task.ID || attempt.WorkItemID != item.ID ||
		attempt.Executor.Executor != workexecution.Agent || attempt.Executor.HostID != item.Placement.HostID ||
		attempt.FinishedAt == nil || attempt.Result == nil || attempt.Input != *item.Input || attempt.Number != item.AttemptNumber ||
		item.Cleanup != workexecution.CleanupRequired && item.Cleanup != workexecution.CleanupStopped {
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "Entry has no captured unresolved effects to recover")
	}
	input, _, guards, err := repository.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	key := RecoveryKey(item.ID)
	read, err := repository.read(ctx, []string{key, FailureKey(attempt.ID)}, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	var source entryexecution.Recovery
	var revision int64
	if read.Values[0] != nil {
		revision = read.Values[0].ModRevision
	}
	if attempt.Direction == workexecution.Forward {
		if read.Values[1] == nil || item.Result == nil || *item.Result != *attempt.Result {
			return workqueue.Change{}, conflict("Entry recovery is missing the original failure")
		}
		failure, err := entryexecution.DecodeFailure(attempt, input, read.Values[1].Value, *attempt.Result)
		if err != nil {
			return workqueue.Change{}, err
		}
		source = entryexecution.Recovery{Forward: attempt, Failure: failure, Evidence: *attempt.Result}
		guards = append(guards, keyvalue.Condition{Key: FailureKey(attempt.ID), ModRevision: read.Values[1].ModRevision})
	} else {
		if read.Values[0] == nil || item.CleanupResult == nil || *item.CleanupResult != *attempt.Result {
			return workqueue.Change{}, conflict("Entry recovery Retry is missing its preceding cleanup result")
		}
		source, err = entryexecution.DecodeRecovery(read.Values[0].Value, input)
		if err != nil {
			return workqueue.Change{}, err
		}
		if err := source.ValidateFor(attempt, input); err != nil {
			return workqueue.Change{}, err
		}
	}
	// Restoration gets the original forward execution budget, not an unbounded
	// extension and not a budget that shrinks after each failed cleanup attempt.
	budget := source.Forward.Deadline.Sub(source.Forward.AssignedAt)
	if budget <= 0 {
		return workqueue.Change{}, conflict("Entry recovery has no captured execution budget")
	}
	raw, err := entryexecution.EncodeRecovery(source, input)
	if err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareRetry(view.Task, view.Item, previous, workexecution.RetryDecision{
		Direction: workexecution.Recovery, Result: *attempt.Result, NotBefore: at, Deadline: at.Add(budget),
	}, at)
	if err != nil {
		clear(raw)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, guards...)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key, ModRevision: revision})
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: raw})
	return change, nil
}
