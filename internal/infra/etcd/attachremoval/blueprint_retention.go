package attachremoval

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
)

func (repository *InputRepository) prepareBlueprintInputRetention(ctx context.Context,
	view workqueue.View, final bool,
) (bool, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Action != workexecution.ActionApply || item.Input != nil || item.AttemptNumber != 0 ||
		item.Cleanup != workexecution.CleanupNone || item.ActiveAttemptID != "" {
		return false, workqueue.Change{}, checkpointConflict(
			"Unprepared Detach retention requires settled Apply input ownership",
		)
	}
	settlement, err := repository.transactions.ReadInputSettlement(ctx, view)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	if settlement == nil {
		return false, workqueue.Change{}, checkpointConflict("Unprepared Detach has no saved settlement")
	}
	frozen, guard, err := repository.LoadBlueprintInput(ctx, view)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	defer frozen.Clear()
	change := workqueue.Change{Conditions: []keyvalue.Condition{guard,
		{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision},
		{Key: workqueue.AttemptIndexPrefix(item.ID), Prefix: true},
		{Key: WorkCaptureKey(item.ID)}, {Key: DetachInputKey(item.ID)}}}
	// A saved settlement proves that executable input never existed. A
	// successful declaration retirement additionally has no real binding.
	if settlement.Outcome == workexecution.Succeeded && frozen.Snapshot != nil {
		return false, workqueue.Change{}, checkpointConflict(
			"Real binding removal cannot succeed without captured executable input",
		)
	}
	if !final {
		return true, change, nil
	}
	if frozen.Pins != nil {
		retired, release, err := RetireOwnedPins(ctx, repository.store, view.ReadRevision,
			frozen.Snapshot.Source.Attach.BackingProjectID, *frozen.Pins, change.Conditions)
		if err != nil || !retired {
			return false, workqueue.Change{}, err
		}
		change.Conditions = append(change.Conditions, release.Conditions...)
	} else {
		change.Conditions = append(change.Conditions,
			keyvalue.Condition{Key: tasksecretpins.RootKey(frozen.OperationID)},
			keyvalue.Condition{Key: tasksecretpins.PreparationKey(frozen.OperationID)},
			keyvalue.Condition{Key: tasksecretpins.ReversePrefix(frozen.OperationID), Prefix: true})
	}
	change.Mutations = []keyvalue.Mutation{
		{Type: keyvalue.MutationDelete, Key: guard.Key},
		{Type: keyvalue.MutationDelete, Key: DetachInputPrefix(item.ID), Prefix: true},
	}
	return true, change, nil
}
