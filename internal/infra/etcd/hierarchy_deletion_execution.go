package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyDeletionRepository) ReadyAction(
	ctx context.Context,
	operation HierarchyDeletionOperation,
) (*HierarchyDeletionAction, HierarchyDeletionOperation, error) {
	if err := validateContext(ctx); err != nil {
		return nil, HierarchyDeletionOperation{}, err
	}
	current, err := repository.OperationByTask(ctx, operation.Tombstone.CurrentTaskID)
	if err != nil {
		return nil, HierarchyDeletionOperation{}, err
	}
	if current.Tombstone.Phase != HierarchyDeletionExecuting || current.Tombstone.PlanCount == nil ||
		current.Tombstone.PlanDigest == nil || current.Fence.Dispatch != HierarchyDeletionDispatchOpen {
		return nil, HierarchyDeletionOperation{}, errs.New(errs.KindStateConflict, "hierarchy deletion is not executable")
	}
	ordinal := current.Tombstone.Checkpoint.NextOrdinal
	if ordinal >= *current.Tombstone.PlanCount {
		return nil, current, nil
	}
	if current.Fence.ActiveActionOrdinal != nil {
		if *current.Fence.ActiveActionOrdinal != ordinal {
			return nil, HierarchyDeletionOperation{}, corruptHierarchyDeletion()
		}
		action, readErr := repository.actionAtRevision(ctx, current, ordinal)
		return &action, current, readErr
	}
	action, err := repository.actionAtRevision(ctx, current, ordinal)
	if err != nil {
		return nil, HierarchyDeletionOperation{}, err
	}
	for _, prerequisite := range action.PrerequisiteOrdinals {
		if prerequisite >= ordinal || prerequisite >= current.Tombstone.Checkpoint.CompletedCount {
			return nil, HierarchyDeletionOperation{}, corruptHierarchyDeletion()
		}
	}
	nextFence := current.Fence
	nextFence.Generation++
	nextFence.ActiveActionOrdinal = &ordinal
	if action.ProcedureKind == HierarchyDeletionProcedureAgent {
		nextFence.ActiveChildOperationID = action.AgentProcedure.ChildOperationID
	}
	nextFence.UpdatedAt = nextFence.UpdatedAt.Add(1)
	fenceKey, _ := HierarchyDeletionCleanupFenceKey(current.Tombstone.OperationID)
	fenceValue, err := encodeHierarchyDeletionRecord(nextFence, hierarchyDeletionSmallRecordBytes)
	if err != nil {
		return nil, HierarchyDeletionOperation{}, err
	}
	defer clear(fenceValue)
	conditions := []Condition{{Key: fenceKey, ModRevision: current.FenceRevision}}
	mutations := []Mutation{{Type: MutationPut, Key: fenceKey, Value: fenceValue}}
	var nextTombstone HierarchyDeletionTombstone
	if action.ProcedureKind == HierarchyDeletionProcedureAgent {
		nextTombstone = current.Tombstone
		nextTombstone.Checkpoint.ActiveChildOperationID = action.AgentProcedure.ChildOperationID
		tombstoneKey := HierarchyDeletionTombstoneKey(string(current.Tombstone.TargetKind), current.Tombstone.TargetID)
		tombstoneValue, encodeErr := encodeHierarchyDeletionRecord(nextTombstone, hierarchyDeletionLargeRecordBytes)
		if encodeErr != nil {
			return nil, HierarchyDeletionOperation{}, encodeErr
		}
		defer clear(tombstoneValue)
		conditions = append(conditions, Condition{Key: tombstoneKey, ModRevision: current.TombstoneRevision})
		mutations = append(mutations, Mutation{Type: MutationPut, Key: tombstoneKey, Value: tombstoneValue})
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return nil, HierarchyDeletionOperation{}, err
	}
	clearKeyValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return nil, HierarchyDeletionOperation{}, errs.New(errs.KindStateConflict, "hierarchy deletion action selection changed")
	}
	current.Fence = nextFence
	current.FenceRevision = transaction.Revision
	if action.ProcedureKind == HierarchyDeletionProcedureAgent {
		current.Tombstone = nextTombstone
		current.TombstoneRevision = transaction.Revision
	}
	return &action, current, nil
}

func (repository *HierarchyDeletionRepository) actionAtRevision(
	ctx context.Context,
	operation HierarchyDeletionOperation,
	ordinal int64,
) (HierarchyDeletionAction, error) {
	key, err := HierarchyDeletionActionKey(operation.Tombstone.OperationID, ordinal)
	if err != nil {
		return HierarchyDeletionAction{}, err
	}
	stored, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{key}, Revision: operation.TombstoneRevision,
	})
	if err != nil {
		return HierarchyDeletionAction{}, err
	}
	if stored == nil || len(stored.Values) != 1 || stored.Values[0] == nil || stored.Values[0].Key != key {
		return HierarchyDeletionAction{}, corruptHierarchyDeletion()
	}
	action, err := decodeHierarchyDeletionAction(stored.Values[0].Value)
	if err != nil || action.ParentOperationID != operation.Tombstone.OperationID || action.Ordinal != ordinal {
		return HierarchyDeletionAction{}, corruptHierarchyDeletion()
	}
	return action, nil
}
