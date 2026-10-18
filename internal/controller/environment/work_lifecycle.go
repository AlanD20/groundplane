package environment

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprovision"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (owner *Work) PrepareWorkRetry(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot,
	previous workqueue.AttemptSnapshot, revision int64, at time.Time) (workqueue.Change, error) {
	view := workqueue.View{Task: task, Item: item, ReadRevision: revision}
	if task.Task.CollectionStartedAt != nil || item.Item.ActiveAttemptID != "" {
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "environment provisioning still has an active execution or expired history")
	}
	if _, _, err := owner.records.Load(ctx, view); err != nil {
		return workqueue.Change{}, err
	}
	if item.Item.AttemptNumber != 0 && (previous.Attempt.Validate() != nil || previous.Attempt.FinishedAt == nil || previous.Attempt.Result == nil ||
		previous.Attempt.Number != item.Item.AttemptNumber || previous.Attempt.TaskID != task.Task.ID || previous.Attempt.WorkItemID != item.Item.ID) {
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "environment Retry requires its exact accepted preceding result")
	}
	var state, retry workqueue.Change
	var err error
	if item.Item.Phase == workexecution.Attention {
		if previous.Attempt.Result == nil {
			return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "environment cleanup requires its accepted failed attempt")
		}
		state, err = owner.records.PrepareCleanupSource(ctx, view, previous)
		if err != nil {
			return workqueue.Change{}, err
		}
		retry, err = workqueue.PrepareRetry(task, item, previous, workexecution.RetryDecision{
			Direction: workexecution.Recovery, Result: *previous.Attempt.Result, NotBefore: at, Deadline: at.Add(120 * time.Second)}, at)
	} else {
		if task.Task.Stop != nil || item.Item.ForwardRevoked || item.Item.Phase != workexecution.Settled || item.Item.Outcome != workexecution.Failed {
			return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "environment Retry requires failed provisioning with settled effects")
		}
		if item.Item.AttemptNumber != 0 {
			receipt, err := owner.records.ReadResult(ctx, previous.Attempt, revision)
			if err != nil {
				return workqueue.Change{}, err
			}
			if !receipt.Record.Report.HelpersAbsent {
				return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "inspect retained environment helpers before retrying creation")
			}
		}
		state, err = owner.records.PrepareResourceRetry(ctx, view)
		if err != nil {
			return workqueue.Change{}, err
		}
		if item.Item.AttemptNumber == 0 {
			retry, err = workqueue.PrepareUnstartedRetry(task, item, at.Add(120*time.Second), at)
		} else {
			retry, err = workqueue.PrepareRetry(task, item, previous, workexecution.RetryDecision{
				Direction: workexecution.Forward, Result: *previous.Attempt.Result, NotBefore: at, Deadline: at.Add(120 * time.Second)}, at)
		}
	}
	if err != nil {
		keyvalue.ClearMutationValues(state.Mutations)
		return workqueue.Change{}, err
	}
	state.Conditions = append(state.Conditions, retry.Conditions...)
	state.Mutations = append(state.Mutations, retry.Mutations...)
	return state, nil
}

func (owner *Work) PrepareFinish(ctx context.Context, finished workqueue.FinishedWork, _ time.Time) (workqueue.Change, error) {
	if len(finished.Items) != 1 || finished.Task.Task.ExpectedItems != 1 {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "environment creation requires one settled work item")
	}
	item := finished.Items[0]
	if item.Item.Phase != workexecution.Settled || item.Item.Result == nil || item.Item.ActiveAttemptID != "" {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "environment provisioning has unresolved execution")
	}
	input, capture, err := owner.records.Load(ctx, workqueue.View{Task: finished.Task, Item: item, ReadRevision: finished.ReadRevision})
	if err != nil {
		return workqueue.Change{}, err
	}
	if item.Item.AttemptNumber == 0 {
		ref, err := input.Reference()
		if err != nil {
			return workqueue.Change{}, err
		}
		if *item.Item.Result != ref || item.Item.Outcome == workexecution.Succeeded {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "unexecuted environment creation cannot prove directory readiness")
		}
	} else {
		_, previous, err := owner.queue.ReadAttempt(ctx, finished.Task.Task.ID, item.Item.ID, item.Item.AttemptNumber, finished.ReadRevision)
		if err != nil {
			return workqueue.Change{}, err
		}
		receipt, err := owner.records.ReadResult(ctx, previous.Attempt, finished.ReadRevision)
		if err != nil {
			return workqueue.Change{}, err
		}
		if !receipt.Record.Report.HelpersAbsent || previous.Attempt.FinishedAt == nil {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "environment provisioning helper effects are unresolved")
		}
	}
	return workqueue.Change{Conditions: []keyvalue.Condition{capture, {Key: workqueue.ItemKey(item.Item.ID), ModRevision: item.Revision}}}, nil
}

func (owner *Work) RetireWork(ctx context.Context, view workqueue.View, at time.Time, final bool) (bool, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.CollectionStartedAt == nil || task.FinishedAt == nil || task.RetainUntil == nil || at.Before(*task.RetainUntil) ||
		at.Before(*task.CollectionStartedAt) || item.Phase != workexecution.Settled || item.ActiveAttemptID != "" ||
		item.Cleanup != workexecution.CleanupNone && item.Cleanup != workexecution.CleanupComplete {
		return false, workqueue.Change{}, errs.New(errs.KindStateConflict, "environment execution history is not ready for collection")
	}
	_, capture, err := owner.records.Load(ctx, view)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	change := workqueue.Change{Conditions: []keyvalue.Condition{capture, {Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision}, {Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision}}}
	if final {
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: workqueue.AttemptIndexPrefix(item.ID), Prefix: true})
		change.Mutations = []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: environmentprovision.InputKey(item.ID)}, {Type: keyvalue.MutationDelete, Key: environmentprovision.RecoveryKey(item.ID)}}
	}
	return true, change, nil
}

func (owner *Work) PrepareAttemptRetirement(view workqueue.View, attempt workqueue.AttemptSnapshot) (workqueue.Change, error) {
	if view.Item.Item.Operation != workexecution.Environment || view.Item.Item.Input == nil || attempt.Attempt.Input != *view.Item.Item.Input {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "environment attempt collection differs from captured input")
	}
	prepared, err := workqueue.PrepareAttemptCollection(view.Task, view.Item, attempt)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: prepared.Conditions, Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: environmentprovision.ResultKey(attempt.Attempt.ID)}}}, nil
}
