package backingprovisionwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
)

// PrepareRetention releases execution-only Secret holds in bounded passes.
// Applied runtime and configuration snapshots remain resource-owned records.
func (repository *Repository) PrepareRetention(
	ctx context.Context,
	view workqueue.View,
	at time.Time,
	final bool,
) (bool, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Validate() != nil || item.Validate() != nil || view.Task.Revision <= 0 || view.Item.Revision <= 0 ||
		task.CollectionStartedAt == nil || task.FinishedAt == nil || task.RetainUntil == nil || at.IsZero() || at.Location() != time.UTC ||
		at.Before(
			*task.CollectionStartedAt,
		) || at.Before(*task.RetainUntil) || item.Operation != workexecution.BackingProvision ||
		item.Phase != workexecution.Settled || item.ActiveAttemptID != "" || item.Result == nil ||
		item.Cleanup != workexecution.CleanupNone && item.Cleanup != workexecution.CleanupComplete {
		return false, workqueue.Change{}, invalid("backing creation history requires expired, settled execution")
	}
	input, capture, err := repository.Load(ctx, view)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	defer clear(input.Wire)
	procedure, err := input.Procedure()
	if err != nil {
		return false, workqueue.Change{}, err
	}
	change := workqueue.Change{Conditions: []keyvalue.Condition{capture,
		{
			Key:         workqueue.TaskKey(task.ID),
			ModRevision: view.Task.Revision,
		}, {Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision}}}
	if input.Hook != nil && input.Hook.Pins != nil {
		ready, guards, err := repository.retireHookPins(ctx, view, *input.Hook.Pins, change.Conditions)
		if err != nil || !ready {
			return false, workqueue.Change{}, err
		}
		change.Conditions = append(change.Conditions, guards...)
	}
	if final {
		collected, err := workqueue.PrepareWorkCollection(view.Task, view.Item)
		if err != nil {
			return false, workqueue.Change{}, err
		}
		change.Conditions = append(change.Conditions, collected.Conditions...)
		change.Mutations = append(
			change.Mutations,
			keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: InputKey(item.ID)},
		)
		if procedure.AfterStart != nil && procedure.AfterStart.EncryptedInputs != nil {
			change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete,
				Key: taskconfiguration.BackingHookTaskInputKey(procedure.OperationId)})
		}
	}
	return true, change, nil
}

func (repository *Repository) retireHookPins(ctx context.Context, view workqueue.View, binding tasksecretpins.Binding,
	guards []keyvalue.Condition,
) (bool, []keyvalue.Condition, error) {
	if binding.Validate() != nil || binding.TaskID != view.Task.Task.ID || binding.WorkItemID != view.Item.Item.ID {
		return false, nil, invalid("backing creation Secret retention belongs to another work item")
	}
	pins, err := tasksecretpins.NewEtcdRepository(repository.store, view.Task.Task.Scope.ProjectID)
	if err != nil {
		return false, nil, err
	}
	root, found, err := pins.LoadActive(ctx, binding.OperationID, view.ReadRevision)
	if err != nil {
		return false, nil, err
	}
	if !found {
		return true, []keyvalue.Condition{{Key: tasksecretpins.RootKey(binding.OperationID)},
			{
				Key: tasksecretpins.PreparationKey(binding.OperationID),
			}, {Key: tasksecretpins.ReversePrefix(binding.OperationID), Prefix: true}}, nil
	}
	_, releasing, err := root.RetentionCondition(binding)
	if err != nil {
		return false, nil, err
	}
	if releasing {
		_, _, err := pins.ReleaseBatch(ctx, binding.OperationID)
		return false, nil, err
	}
	fragment, err := tasksecretpins.BeginRelease(root)
	if err != nil {
		return false, nil, err
	}
	defer fragment.Clear()
	conditions, mutations, err := tasksecretpins.EtcdFragment(fragment)
	if err != nil {
		return false, nil, err
	}
	defer keyvalue.ClearMutationValues(mutations)
	result, err := repository.store.Transact(
		ctx,
		append(append([]keyvalue.Condition(nil), guards...), conditions...),
		mutations,
	)
	keyvalue.ClearValues(result.FailureReads)
	return false, nil, err
}

func PrepareAttemptRetirement(view workqueue.View, snapshot workqueue.AttemptSnapshot) (workqueue.Change, error) {
	item, attempt := view.Item.Item, snapshot.Attempt
	if item.Operation != workexecution.BackingProvision || item.Placement.Executor != workexecution.Agent ||
		item.Input == nil ||
		attempt.Input != *item.Input ||
		attempt.Executor.HostID != item.Placement.HostID ||
		attempt.Executor.Executor != workexecution.Agent {
		return workqueue.Change{}, invalid(
			"backing creation attempt collection differs from its captured work and host",
		)
	}
	collected, err := workqueue.PrepareAttemptCollection(view.Task, view.Item, snapshot)
	if err != nil {
		return workqueue.Change{}, err
	}
	failureID, err := backingprovision.FailureID(attempt.ID)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: collected.Conditions, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationDelete, Key: ReportPrefix(attempt.ID), Prefix: true},
		{Type: keyvalue.MutationDelete, Key: ResultKey(attempt.ID)},
		{Type: keyvalue.MutationDelete, Key: FailureKey(failureID)},
		{Type: keyvalue.MutationDelete, Key: RecoveryResultKey(attempt.ID)},
	}}, nil
}
