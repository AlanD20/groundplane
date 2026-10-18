package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
)

// abandonUnstartedAttachPins is called only while settling stopped/expired
// unstarted work. Published captures retain their pins; abandoned preparations
// are removed in bounded batches under the same work/publication guards.
func (repository *HierarchyDeletionRepository) abandonUnstartedAttachPins(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot) (bool, []keyvalue.Condition, error) {
	if item.Item.AttemptNumber != 0 || ids.Validate(ids.KindAttach, item.Item.TargetID) != nil {
		return true, nil, nil
	}
	view := workqueue.View{Task: task, Item: item, ReadRevision: max(task.Revision, item.Revision)}
	capture, err := repository.LoadHierarchyAttachCapture(ctx, view)
	if err != nil {
		return false, nil, err
	}
	defer capture.Clear()
	if capture.Revision != 0 {
		return true, nil, nil
	}
	pins, err := tasksecretpins.NewEtcdRepository(repository.store, capture.Source.Input.Attach.BackingProjectID)
	if err != nil {
		return false, nil, err
	}
	operationID := HierarchyAttachOperation(view)
	done, err := pins.AbandonOwned(ctx, operationID, task.Task.ID, item.Item.ID, []tasksecretpins.Condition{
		{Key: workqueue.TaskKey(task.Task.ID), ModRevision: task.Revision},
		{Key: workqueue.ItemKey(item.Item.ID), ModRevision: item.Revision},
		{Key: attachremoval.WorkCaptureKey(item.Item.ID)},
	})
	if err != nil || !done {
		return false, nil, err
	}
	return true, []keyvalue.Condition{
		{Key: attachremoval.WorkCaptureKey(item.Item.ID)},
		{Key: tasksecretpins.PreparationKey(operationID)},
		{Key: tasksecretpins.RootKey(operationID)},
		{Key: tasksecretpins.ReversePrefix(operationID), Prefix: true},
	}, nil
}
