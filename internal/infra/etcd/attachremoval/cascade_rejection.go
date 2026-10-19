package attachremoval

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
)

// AbandonCascadePreparation drains one unpublished Secret-pin batch under the
// rejected Task and absent work/capture guards. It cannot release activated pins.
// A false result means another bounded publication pass is needed.
func (repository *InputRepository) AbandonCascadePreparation(ctx context.Context, view workqueue.View,
	rejectionGuards []keyvalue.Condition,
) (bool, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.PublicationComplete || task.Action != workexecution.ActionRemove || task.Scope.TenantID != "" ||
		task.Outcome == "" || task.Outcome == workexecution.Succeeded || item.Operation != workexecution.Detach ||
		view.Task.Revision <= 0 || view.Item.Revision != 0 || item.AttemptNumber != 0 ||
		item.ActiveAttemptID != "" || item.Phase != workexecution.Ready || len(rejectionGuards) == 0 {
		return false, checkpointConflict("Secret preparation cleanup requires rejected unpublished consumer work")
	}
	capture, err := repository.LoadPreparedCascadeCapture(ctx, view)
	if err != nil {
		return false, err
	}
	defer capture.Clear()
	if capture.Input.Cascade == nil || capture.Input.Cascade.ZoneID != task.TargetID {
		return false, checkpointConflict("Secret preparation cleanup belongs to another Zone")
	}
	guards := append([]keyvalue.Condition(nil), rejectionGuards...)
	guards = append(guards, capture.Conditions...)
	guards = append(guards,
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(item.ID)}, keyvalue.Condition{Key: WorkCaptureKey(item.ID)})
	owned := make([]tasksecretpins.Condition, len(guards))
	for index, guard := range guards {
		owned[index] = tasksecretpins.Condition{Key: guard.Key, ModRevision: guard.ModRevision, Prefix: guard.Prefix}
	}
	pins, err := tasksecretpins.NewEtcdRepository(repository.store, capture.Capture.BackingProjectID)
	if err != nil {
		return false, err
	}
	return pins.AbandonOwned(ctx, capture.Capture.OperationID, task.ID, item.ID, owned)
}

// PrepareCascadePublicationRejection accounts for resource ownership acquired
// before the graph opened. Published consumers become failed, not detached;
// their facts and pinned inputs remain available until normal retention. Missing
// members never acquired a binding, so no resource write is permitted for them.
// The caller must join the saved graph rejection and native item settlement.
func (repository *InputRepository) PrepareCascadePublicationRejection(ctx context.Context,
	view workqueue.View,
) (workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.PublicationComplete || task.Action != workexecution.ActionRemove || task.Scope.TenantID != "" ||
		task.Outcome == "" || task.Outcome == workexecution.Succeeded || item.Operation != workexecution.Detach ||
		item.AttemptNumber != 0 || item.ActiveAttemptID != "" || item.Cleanup != workexecution.CleanupNone ||
		item.Phase != workexecution.Ready || view.Task.Revision <= 0 || view.Item.Revision < 0 {
		return workqueue.Change{}, checkpointConflict(
			"consumer publication cleanup requires a rejected unexecuted graph",
		)
	}
	if view.Item.Revision > 0 {
		return repository.PrepareDetachFailed(ctx, view)
	}
	capture, err := repository.LoadPreparedCascadeCapture(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer capture.Clear()
	if capture.Input.Cascade == nil || capture.Input.Cascade.ZoneID != task.TargetID {
		return workqueue.Change{}, checkpointConflict("unpublished consumer belongs to another removal")
	}
	return workqueue.Change{Conditions: append(capture.Conditions,
		keyvalue.Condition{Key: WorkCaptureKey(item.ID)},
		keyvalue.Condition{Key: tasksecretpins.RootKey(capture.Capture.OperationID)},
		keyvalue.Condition{Key: tasksecretpins.PreparationKey(capture.Capture.OperationID)},
		keyvalue.Condition{Key: tasksecretpins.ReversePrefix(capture.Capture.OperationID), Prefix: true},
		keyvalue.Condition{Key: workqueue.ItemKey(item.ID)})}, nil
}
