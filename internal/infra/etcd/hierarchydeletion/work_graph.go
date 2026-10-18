package hierarchydeletion

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// WorkGraph uses the sealed deletion plan as publication authority. Completing
// publication after Abort is intentional: the stop walker must see every action.
// No action is dispatched until the shared graph barrier is opened.
func WorkGraph(task workexecution.Task, operation HierarchyDeletionOperation, actions []HierarchyDeletionAction, hostID string) ([]workexecution.Item, []keyvalue.Condition, error) {
	tombstone := operation.Tombstone
	if task.Validate() != nil || task.Action != workexecution.ActionRemove || task.ID != tombstone.CurrentTaskID ||
		task.TargetID != tombstone.TargetID || task.PublicationComplete || tombstone.Phase != HierarchyDeletionExecuting ||
		tombstone.PlanCount == nil || *tombstone.PlanCount <= 0 || *tombstone.PlanCount != int64(len(actions)) ||
		uint64(len(actions)) != uint64(task.ExpectedItems) || tombstone.PlanDigest == nil || operation.Fence.PlanDigest == nil ||
		*tombstone.PlanDigest != *operation.Fence.PlanDigest || operation.TombstoneRevision <= 0 || operation.FenceRevision <= 0 {
		return nil, nil, errs.New(errs.KindStateConflict, "hierarchy work publication requires its complete sealed deletion plan")
	}
	values := make([][]byte, len(actions))
	defer keyvalue.ClearByteSlices(values)
	items := make([]workexecution.Item, len(actions))
	for index, action := range actions {
		if action.ParentOperationID != tombstone.OperationID || action.Ordinal != int64(index) {
			return nil, nil, errs.New(errs.KindStateConflict, "hierarchy work plan membership changed")
		}
		var err error
		items[index], values[index], err = ActionWork(task, action, hostID)
		if err != nil {
			return nil, nil, err
		}
	}
	if HierarchyDeletionPlanDigest(values) != *tombstone.PlanDigest {
		return nil, nil, errs.New(errs.KindStateConflict, "hierarchy work input differs from the sealed deletion plan")
	}
	if err := workexecution.ValidateGraph(task, items); err != nil {
		return nil, nil, err
	}
	fenceKey, err := HierarchyDeletionCleanupFenceKey(tombstone.OperationID)
	if err != nil {
		return nil, nil, err
	}
	return items, []keyvalue.Condition{
		{Key: HierarchyDeletionTombstoneKey(string(tombstone.TargetKind), tombstone.TargetID), ModRevision: operation.TombstoneRevision},
		{Key: fenceKey, ModRevision: operation.FenceRevision},
	}, nil
}
