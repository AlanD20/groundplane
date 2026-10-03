package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionretention"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

func (repository *TaskRepository) hierarchyDeletionChildPruneHeld(
	ctx context.Context, task TaskRecord, revision int64,
) (bool, error) {
	parentOperationID := task.Params[taskjournal.TaskHierarchyDeletionParentParam]
	if parentOperationID == "" {
		return false, nil
	}
	if task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceHierarchyDeletion ||
		task.Params[taskjournal.TaskHierarchyDeletionChildParam] != task.OperationID {
		return false, hierarchydeletion.CorruptHierarchyDeletion()
	}
	return hierarchydeletionretention.ChildHeld(ctx, repository.store, parentOperationID, revision)
}
