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

func RecoveryKey(workID string) string { return "/v1/volume-work-recovery/" + workID }

type RecoverySnapshot struct {
	Source   volumeexecution.Recovery
	Revision int64
}

func (repository *InputRepository) LoadRecovery(ctx context.Context, view workqueue.View) (RecoverySnapshot, error) {
	item := view.Item.Item
	if item.Operation != workexecution.Volume || item.Input == nil {
		return RecoverySnapshot{}, conflict("Volume recovery requires captured work")
	}
	read, err := repository.read(ctx, []string{RecoveryKey(item.ID)}, view.ReadRevision)
	if err != nil {
		return RecoverySnapshot{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return RecoverySnapshot{}, conflict("Volume recovery source is unavailable")
	}
	source, err := volumeexecution.DecodeRecovery(read.Values[0].Value)
	if err != nil {
		return RecoverySnapshot{}, err
	}
	if source.Forward.TaskID != view.Task.Task.ID || source.Forward.WorkItemID != item.ID ||
		source.Forward.Input != *item.Input ||
		source.Forward.Executor.HostID != item.Placement.HostID ||
		source.Forward.Number > item.AttemptNumber {
		return RecoverySnapshot{}, conflict("Volume recovery source belongs to different execution")
	}
	return RecoverySnapshot{Source: source, Revision: read.Values[0].ModRevision}, nil
}

// PrepareRecoveryRetry retains the original forward failure across repeated
// cleanup attempts. It grants inspection of captured effects, not new intent.
func (repository *InputRepository) PrepareRecoveryRetry(
	ctx context.Context,
	view workqueue.View,
	previous workqueue.AttemptSnapshot,
	at time.Time,
) (workqueue.Change, error) {
	task, item, attempt := view.Task.Task, view.Item.Item, previous.Attempt
	if task.CollectionStartedAt != nil || item.Operation != workexecution.Volume ||
		item.Placement.Executor != workexecution.Agent ||
		item.Phase != workexecution.Attention ||
		item.ActiveAttemptID != "" ||
		item.Input == nil ||
		attempt.FinishedAt == nil ||
		attempt.Result == nil ||
		attempt.Input != *item.Input ||
		attempt.Number != item.AttemptNumber ||
		item.Cleanup != workexecution.CleanupRequired && item.Cleanup != workexecution.CleanupStopped {
		return workqueue.Change{}, errs.New(
			errs.KindTaskNotRetryable,
			"Volume has no captured unresolved effects to recover",
		)
	}
	input, guards, err := repository.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	budget, err := volumeexecution.ExecutionBudget(input.Action)
	if err != nil {
		return workqueue.Change{}, err
	}
	key := RecoveryKey(item.ID)
	read, err := repository.read(ctx, []string{key, FailureKey(attempt.ID)}, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	var source volumeexecution.Recovery
	var revision int64
	if read.Values[0] != nil {
		revision = read.Values[0].ModRevision
	}
	if attempt.Direction == workexecution.Forward {
		if read.Values[1] == nil || item.Result == nil || *item.Result != *attempt.Result {
			return workqueue.Change{}, conflict("Volume recovery is missing the original failure")
		}
		failure, err := volumeexecution.DecodeFailure(attempt, read.Values[1].Value, *attempt.Result)
		if err != nil {
			return workqueue.Change{}, err
		}
		source = volumeexecution.Recovery{Forward: attempt, Failure: failure, Evidence: *attempt.Result}
		guards = append(
			guards,
			keyvalue.Condition{Key: FailureKey(attempt.ID), ModRevision: read.Values[1].ModRevision},
		)
	} else {
		if read.Values[0] == nil || item.CleanupResult == nil || *item.CleanupResult != *attempt.Result {
			return workqueue.Change{}, conflict("Volume recovery Retry is missing its preceding cleanup result")
		}
		source, err = volumeexecution.DecodeRecovery(read.Values[0].Value)
		if err != nil {
			return workqueue.Change{}, err
		}
		if err := source.ValidateFor(attempt); err != nil {
			return workqueue.Change{}, err
		}
	}
	raw, err := volumeexecution.EncodeRecovery(source)
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
