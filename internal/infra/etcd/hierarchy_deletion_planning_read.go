package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (repository *HierarchyDeletionRepository) ReadPlanningRejection(ctx context.Context, task workqueue.TaskSnapshot, revision int64) (*hierarchydeletion.PlanningRejection, error) {
	if task.Task.Action != workexecution.ActionRemove || task.Task.ExpectedItems != 0 || task.Task.FinishedAt == nil || task.Task.Outcome == workexecution.Succeeded {
		return nil, nil
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{hierarchydeletion.TaskBindingKey(task.Task.ID), hierarchydeletion.PlanningRejectionKey(task.Task.ID)}, Revision: revision,
	})
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 2 {
		return nil, hierarchydeletion.CorruptHierarchyDeletion()
	}
	if read.Values[0] == nil && read.Values[1] == nil {
		return nil, nil
	}
	if read.Values[0] == nil || read.Values[1] == nil {
		return nil, hierarchydeletion.CorruptHierarchyDeletion()
	}
	binding, err := hierarchydeletion.DecodeTaskBinding(read.Values[0].Value)
	if err != nil {
		return nil, err
	}
	rejection, err := hierarchydeletion.DecodePlanningRejection(read.Values[1].Value, task.Task)
	if err != nil {
		return nil, err
	}
	if binding.TaskID != task.Task.ID || binding.OperationID != rejection.Operation.Tombstone.OperationID || binding.Marker != rejection.Operation.MarkerLocator {
		return nil, hierarchydeletion.CorruptHierarchyDeletion()
	}
	return &rejection, nil
}
