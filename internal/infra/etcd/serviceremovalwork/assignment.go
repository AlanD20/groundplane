package serviceremovalwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PrepareAssignment joins captured resource ownership to the scheduler's claim,
// dependency, executor and capacity decision. It grants no execution by itself.
// Recovery deliberately does not use this forward-only admission function.
func (repository *Repository) PrepareAssignment(
	ctx context.Context,
	view workqueue.View,
) (Input, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if view.Task.Revision <= 0 || view.Item.Revision <= 0 || task.Stop != nil ||
		task.CollectionStartedAt != nil || item.ForwardRevoked || item.Phase != workexecution.Ready ||
		item.Cleanup != workexecution.CleanupNone && item.Cleanup != workexecution.CleanupComplete {
		return Input{}, workqueue.Change{}, conflict("Service removal is not ready for forward execution")
	}
	input, captureGuard, err := repository.Load(ctx, view)
	if err != nil {
		return Input{}, workqueue.Change{}, err
	}
	change, err := repository.PrepareForwardBoundary(ctx, view, input)
	if err != nil {
		return Input{}, workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, captureGuard)
	return input, change, nil
}

// PrepareForwardBoundary is shared by assignment, Start and each new member
// effect. A grant queued before an edit must not execute against a newer head.
// Completed observations and recovery deliberately do not use this guard: they
// must account for effects already authorized, even after forward revocation.
func (repository *Repository) PrepareForwardBoundary(
	ctx context.Context,
	view workqueue.View,
	input Input,
) (workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Stop != nil || task.FinishedAt != nil || task.CollectionStartedAt != nil ||
		!task.PublicationComplete || item.ForwardRevoked || item.Operation != workexecution.ServiceRemoval ||
		task.ID != input.TaskID || item.ID != input.WorkItemID {
		return workqueue.Change{}, conflict("Service removal no longer owns forward execution")
	}
	conditions, err := repository.RuntimeGuards(ctx, input, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	desiredGuard, err := repository.DesiredGuard(ctx, input, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	conditions = append(conditions, desiredGuard,
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision})
	if task.Action == workexecution.ActionRemove {
		// Direct removal has not yet published its candidate head. An intervening
		// edit cannot be overwritten by this action's eventual settlement.
		key := blueprints.EnvironmentBlueprintHeadKey(input.Scope.EnvironmentID)
		read, err := repository.read(ctx, []string{key}, view.ReadRevision)
		if err != nil {
			return workqueue.Change{}, err
		}
		defer keyvalue.ClearValues(read.Values)
		var revision int64
		if read.Values[0] != nil {
			revision = read.Values[0].ModRevision
		}
		if revision != input.HeadRevision {
			return workqueue.Change{}, conflict(
				"Environment changed after Service removal was requested; no new removal step was started",
			)
		}
		conditions = append(conditions, keyvalue.Condition{Key: key, ModRevision: revision})
	}
	return workqueue.Change{Conditions: conditions}, nil
}
