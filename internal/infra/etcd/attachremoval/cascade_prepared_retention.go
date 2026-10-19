package attachremoval

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
)

// prepareUnactivatedCascadeRetention handles the native prepublication state,
// not a missing-input fallback for executed work. Activation atomically deletes
// prepared-hook and creates WorkCapture; both absence/presence proofs are required.
func (repository *InputRepository) prepareUnactivatedCascadeRetention(ctx context.Context, view workqueue.View,
	final bool,
) (bool, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Action != workexecution.ActionRemove || task.Scope.TenantID != "" || item.AttemptNumber != 0 ||
		item.Cleanup != workexecution.CleanupNone || item.Outcome == workexecution.Succeeded || item.Result == nil ||
		item.Result.ID != ids.DeriveAt(ids.KindPlan, task.CreatedAt, task.ID, "cascade-publication-rejection") {
		return false, workqueue.Change{}, checkpointConflict(
			"Detach without an active capture is not a rejected cascade publication",
		)
	}
	capture, err := repository.LoadPreparedCascadeCapture(ctx, view)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	defer capture.Clear()
	if capture.Input.Cascade == nil || capture.Input.Cascade.ZoneID != task.TargetID {
		return false, workqueue.Change{}, checkpointConflict("prepared Detach belongs to another removal")
	}
	operationID := DetachOperation(view)
	change := workqueue.Change{Conditions: append(capture.Conditions,
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision},
		keyvalue.Condition{Key: WorkCaptureKey(item.ID)},
		keyvalue.Condition{Key: workqueue.AttemptIndexPrefix(item.ID), Prefix: true},
		keyvalue.Condition{Key: tasksecretpins.RootKey(operationID)},
		keyvalue.Condition{Key: tasksecretpins.PreparationKey(operationID)},
		keyvalue.Condition{Key: tasksecretpins.ReversePrefix(operationID), Prefix: true})}
	if final {
		change.Mutations = []keyvalue.Mutation{
			{Type: keyvalue.MutationDelete, Key: DetachInputPrefix(item.ID), Prefix: true},
		}
	}
	return true, change, nil
}
