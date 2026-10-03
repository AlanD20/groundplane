package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionattach"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

func (repository *TaskRepository) prepareHierarchyAttachInputTerminal(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) (taskMaterializationProjectionChange, error) {
	if task.Status != taskjournal.TaskStatusCompleted ||
		task.Params[taskjournal.TaskHierarchyDeletionProcedureParam] != "attach.deprovision" {
		return taskMaterializationProjectionChange{}, nil
	}
	change, err := hierarchydeletionattach.PrepareTerminal(ctx, repository.store,
		hierarchydeletionattach.TerminalIdentity{
			ParentOperationID: task.Params[taskjournal.TaskHierarchyDeletionParentParam],
			AttachID:          task.Target, PlanID: task.PlanID, TaskID: task.ID,
		}, revision)
	if err != nil {
		return taskMaterializationProjectionChange{}, err
	}
	return taskMaterializationProjectionChange{
		applies: true, conditions: change.Conditions, mutations: change.Mutations,
	}, nil
}
