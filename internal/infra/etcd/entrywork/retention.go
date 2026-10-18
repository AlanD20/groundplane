package entrywork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PrepareRetention releases only execution-owned inputs after settled work has
// expired. Applied configuration and Service runtimes are independent records.
func (repository *InputRepository) PrepareRetention(ctx context.Context, view workqueue.View, at time.Time, final bool) (bool, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Validate() != nil || item.Validate() != nil || view.Task.Revision <= 0 || view.Item.Revision <= 0 ||
		view.ReadRevision <= 0 || task.CollectionStartedAt == nil || task.FinishedAt == nil || task.RetainUntil == nil ||
		at.IsZero() || at.Location() != time.UTC || at.Before(*task.CollectionStartedAt) || at.Before(*task.RetainUntil) ||
		item.Operation != workexecution.EntryConfiguration || item.TaskID != task.ID ||
		item.Phase != workexecution.Settled || item.ActiveAttemptID != "" ||
		item.Cleanup != workexecution.CleanupNone && item.Cleanup != workexecution.CleanupComplete {
		return false, workqueue.Change{}, conflict("Entry input retention requires expired and fully settled work")
	}
	if item.Placement.Executor == workexecution.Controller {
		return repository.retireDefinition(ctx, view, at, final)
	}
	if item.Placement.Executor != workexecution.Agent || item.Input == nil {
		return false, workqueue.Change{}, conflict("Entry host retention requires captured execution input")
	}
	_, capture, guards, err := repository.Load(ctx, view)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	guards = append(guards, keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision})
	if capture.Secrets != nil {
		ready, pins, err := repository.retireSecretPins(ctx, view, *capture.Secrets, guards)
		if err != nil || !ready {
			return false, workqueue.Change{}, err
		}
		guards = append(guards, pins.Conditions...)
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
