package servicelifecyclework

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func RecoveryKey(workID string) string { return "/v1/execution/service-lifecycle/recovery/" + workID }

type RecoverySnapshot struct {
	Source   lifecycleexecution.Recovery
	Revision int64
}

func (repository *Repository) LoadRecovery(
	ctx context.Context,
	view workqueue.View,
	input *agentpb.ServiceLifecycleWorkInput,
) (RecoverySnapshot, error) {
	captured, _, err := repository.Load(ctx, view)
	if err != nil {
		return RecoverySnapshot{}, err
	}
	if !sameCapture(captured, captured, input) {
		return RecoverySnapshot{}, conflict("Service lifecycle recovery input changed")
	}
	read, err := repository.read(ctx, []string{RecoveryKey(view.Item.Item.ID)}, view.ReadRevision)
	if err != nil {
		return RecoverySnapshot{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return RecoverySnapshot{}, conflict("Service lifecycle recovery source is missing")
	}
	source, err := lifecycleexecution.DecodeRecovery(input, read.Values[0].Value)
	if err != nil {
		return RecoverySnapshot{}, err
	}
	if source.Forward.TaskID != view.Task.Task.ID || source.Forward.WorkItemID != view.Item.Item.ID ||
		source.Forward.Executor.HostID != view.Item.Item.Placement.HostID || source.Forward.Number > view.Item.Item.AttemptNumber {
		return RecoverySnapshot{}, conflict("Service lifecycle recovery belongs to another execution")
	}
	return RecoverySnapshot{Source: source, Revision: read.Values[0].ModRevision}, nil
}

// PrepareRecoveryRetry atomically queues inspection with the failed execution's
// captured source. Repeated recovery retains the original forward failure.
func (repository *Repository) PrepareRecoveryRetry(
	ctx context.Context,
	view workqueue.View,
	previous workqueue.AttemptSnapshot,
	input *agentpb.ServiceLifecycleWorkInput,
	at, deadline time.Time,
) (workqueue.Change, error) {
	task, item, attempt := view.Task.Task, view.Item.Item, previous.Attempt
	if task.CollectionStartedAt != nil || item.Operation != workexecution.ServiceLifecycle ||
		item.Phase != workexecution.Attention ||
		item.ActiveAttemptID != "" ||
		item.Input == nil ||
		attempt.FinishedAt == nil ||
		attempt.Result == nil ||
		attempt.Input != *item.Input ||
		attempt.Number != item.AttemptNumber ||
		!deadline.After(at) ||
		(item.Cleanup != workexecution.CleanupRequired && item.Cleanup != workexecution.CleanupStopped) {
		return workqueue.Change{}, conflict("Service lifecycle has no unresolved captured effects to recover")
	}
	captured, guard, err := repository.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	if !sameCapture(captured, captured, input) {
		return workqueue.Change{}, conflict("Service lifecycle recovery input changed")
	}
	key := RecoveryKey(item.ID)
	read, err := repository.read(ctx, []string{key}, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	var source lifecycleexecution.Recovery
	guards := []keyvalue.Condition{guard}
	var revision int64
	if read.Values[0] != nil {
		revision = read.Values[0].ModRevision
	}
	if attempt.Direction == workexecution.Forward {
		var sourceGuards []keyvalue.Condition
		source, sourceGuards, err = repository.LoadRecoverySource(ctx, view, previous, input)
		if err != nil {
			return workqueue.Change{}, err
		}
		guards = append(guards, sourceGuards...)
	} else {
		if attempt.Direction != workexecution.Recovery || read.Values[0] == nil || item.CleanupResult == nil || *item.CleanupResult != *attempt.Result {
			return workqueue.Change{}, conflict("Service lifecycle recovery Retry lacks its previous cleanup result")
		}
		source, err = lifecycleexecution.DecodeRecovery(input, read.Values[0].Value)
		if err != nil {
			return workqueue.Change{}, err
		}
		if err := source.ValidateFor(attempt, input); err != nil {
			return workqueue.Change{}, err
		}
	}
	if source.Progress == nil || source.Progress.BeganAt == nil {
		return workqueue.Change{}, conflict("Service lifecycle recovery requires an interrupted effect boundary")
	}
	raw, err := lifecycleexecution.EncodeRecovery(input, source)
	if err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareRetry(view.Task, view.Item, previous, workexecution.RetryDecision{
		Direction: workexecution.Recovery, Result: *attempt.Result, NotBefore: at, Deadline: deadline,
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
