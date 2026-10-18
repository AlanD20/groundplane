package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PrepareHierarchyDeadline carries an explicit Task Retry's renewed horizon to
// never-started dependent actions. Each action still needs ordinary admission
// and dependency proofs on its next sweep before it can execute.
func (repository *HierarchyDeletionRepository) PrepareHierarchyDeadline(ctx context.Context, request workqueue.Assignment, at time.Time) (workqueue.Change, error) {
	item, task := request.Item.Item, request.Task.Task
	if (item.Operation != workexecution.Hierarchy && item.Operation != workexecution.RemovalBackupObject) || task.Action != workexecution.ActionRemove || item.AttemptNumber != 0 ||
		item.ActiveAttemptID != "" || task.Stop != nil || !task.Deadline.After(item.Deadline) || !task.Deadline.After(at) {
		return workqueue.Change{}, nil
	}
	captured, err := repository.LoadHierarchyWork(ctx, workqueue.View{Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision})
	if err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareUnstartedDeadline(request.Task, request.Item, request.QueueRevision, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, captured.Conditions...)
	return change, nil
}
