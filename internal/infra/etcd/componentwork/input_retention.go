package componentwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PrepareRetention drains execution-only holds before retiring attempts. The
// final pass removes captured input, never applied runtime or configuration.
func (repository *InputRepository) PrepareRetention(ctx context.Context, view workqueue.View, at time.Time, final bool) (bool, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Validate() != nil || item.Validate() != nil || view.Task.Revision <= 0 || view.Item.Revision <= 0 ||
		view.ReadRevision <= 0 || task.CollectionStartedAt == nil || task.FinishedAt == nil || task.RetainUntil == nil ||
		at.Before(*task.CollectionStartedAt) || at.Before(*task.RetainUntil) ||
		item.Operation != workexecution.Component || item.Placement.Executor != workexecution.Agent || item.TaskID != task.ID ||
		item.Phase != workexecution.Settled || item.ActiveAttemptID != "" ||
		item.Cleanup != workexecution.CleanupNone && item.Cleanup != workexecution.CleanupComplete {
		return false, workqueue.Change{}, conflict("Component input retention requires expired, fully settled work")
	}
	guards := []keyvalue.Condition{
		{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision},
	}
	if item.Input == nil {
		if item.AttemptNumber != 0 || !final {
			return false, workqueue.Change{}, conflict("Component without captured input cannot own attempts")
		}
		guards = append(guards, keyvalue.Condition{Key: InputKey(item.ID)})
	} else {
		_, capture, captured, err := repository.Load(ctx, view)
		if err != nil {
			return false, workqueue.Change{}, err
		}
		guards = append(guards, captured...)
		if capture.Secrets != nil {
			ready, pins, err := repository.retireSecretPins(ctx, view, *capture.Secrets, guards)
			if err != nil || !ready {
				return false, workqueue.Change{}, err
			}
			guards = append(guards, pins.Conditions...)
		}
	}
	change := workqueue.Change{Conditions: guards}
	if final {
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: workqueue.AttemptIndexPrefix(item.ID), Prefix: true})
		change.Mutations = []keyvalue.Mutation{
			{Type: keyvalue.MutationDelete, Key: InputPrefix(item.ID), Prefix: true},
			{Type: keyvalue.MutationDelete, Key: UnstartedKey(item.ID)},
			{Type: keyvalue.MutationDelete, Key: RecoveryKey(item.ID)},
		}
	}
	return true, change, nil
}
