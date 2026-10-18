package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionfinalization"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareHierarchyControllerSettlement joins metadata deletion, its immutable
// proof and work settlement. Dependency completion, resource conflicts and Abort
// are compared in the same transaction through the decision fence.
func (repository *HierarchyDeletionRepository) PrepareHierarchyControllerSettlement(ctx context.Context, request workqueue.Assignment, at time.Time) (workqueue.Change, error) {
	fence, err := workqueue.PrepareDecisionFence(request)
	if err != nil {
		return workqueue.Change{}, err
	}
	view := workqueue.View{Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision}
	item, task := view.Item.Item, view.Task.Task
	if task.Action != workexecution.ActionRemove || task.Stop != nil || task.FinishedAt != nil ||
		!task.PublicationComplete || item.Operation != workexecution.Hierarchy ||
		item.Placement.Executor != workexecution.Controller || item.AttemptNumber != 0 ||
		item.Phase != workexecution.Ready || !at.Before(item.Deadline) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "removal metadata work is not ready for execution")
	}
	captured, err := repository.LoadHierarchyWork(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	action := captured.Action
	effects, err := hierarchydeletionfinalization.NewPreparer(repository.store).PrepareCaptured(ctx, captured.Operation, action)
	if err != nil {
		return workqueue.Change{}, err
	}
	transferred := false
	defer func() {
		if !transferred {
			keyvalue.ClearByteSlices(effects.Values())
		}
	}()
	value, result, err := hierarchyActionCompletion(task, item, captured, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareUnstartedSettlement(view.Task, view.Item, workexecution.Completion{
		Outcome: workexecution.Succeeded, Evidence: result, EffectsSettled: true,
	}, at)
	if err != nil {
		clear(value)
		return workqueue.Change{}, err
	}
	key, err := hierarchydeletion.HierarchyDeletionCompletionKey(action.ParentOperationID, action.Ordinal)
	if err != nil {
		clear(value)
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, fence.Conditions...)
	change.Conditions = append(change.Conditions, captured.Conditions...)
	change.Conditions = append(change.Conditions, effects.Conditions()...)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key})
	change.Mutations = append(change.Mutations, effects.Mutations()...)
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value})
	transferred = true
	return change, nil
}
