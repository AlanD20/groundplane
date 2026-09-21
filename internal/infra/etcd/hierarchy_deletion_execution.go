package etcd

import (
	"context"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyDeletionRepository) ReadyAction(
	ctx context.Context,
	operation HierarchyDeletionOperation,
) (*hierarchydeletion.HierarchyDeletionAction, HierarchyDeletionOperation, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return nil, HierarchyDeletionOperation{}, err
	}
	current, err := repository.OperationByTask(ctx, operation.Tombstone.CurrentTaskID)
	if err != nil {
		return nil, HierarchyDeletionOperation{}, err
	}
	if current.Tombstone.Phase != hierarchydeletion.HierarchyDeletionExecuting || current.Tombstone.PlanCount == nil ||
		current.Tombstone.PlanDigest == nil || current.Fence.Dispatch != hierarchydeletion.HierarchyDeletionDispatchOpen {
		return nil, HierarchyDeletionOperation{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion is not executable",
		)
	}
	ordinal := current.Tombstone.Checkpoint.NextOrdinal
	if ordinal >= *current.Tombstone.PlanCount {
		return nil, current, nil
	}
	if current.Fence.ActiveActionOrdinal != nil {
		if *current.Fence.ActiveActionOrdinal != ordinal {
			return nil, HierarchyDeletionOperation{}, hierarchydeletion.CorruptHierarchyDeletion()
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
			return nil, HierarchyDeletionOperation{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
	}
	nextFence := current.Fence
	nextFence.Generation++
	nextFence.ActiveActionOrdinal = &ordinal
	if action.ProcedureKind == hierarchydeletion.HierarchyDeletionProcedureAgent {
		nextFence.ActiveChildOperationID = action.AgentProcedure.ChildOperationID
	}
	nextFence.UpdatedAt = nextFence.UpdatedAt.Add(1)
	fenceKey, _ := hierarchydeletion.HierarchyDeletionCleanupFenceKey(current.Tombstone.OperationID)
	fenceValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(nextFence, hierarchydeletion.HierarchyDeletionSmallRecordBytes)
	if err != nil {
		return nil, HierarchyDeletionOperation{}, err
	}
	defer clear(fenceValue)
	conditions := []etcdstore.Condition{{Key: fenceKey, ModRevision: current.FenceRevision}}
	mutations := []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: fenceKey, Value: fenceValue}}
	var nextTombstone hierarchydeletion.HierarchyDeletionTombstone
	if action.ProcedureKind == hierarchydeletion.HierarchyDeletionProcedureAgent {
		nextTombstone = current.Tombstone
		nextTombstone.Checkpoint.ActiveChildOperationID = action.AgentProcedure.ChildOperationID
		tombstoneKey := hierarchydeletion.HierarchyDeletionTombstoneKey(string(current.Tombstone.TargetKind), current.Tombstone.TargetID)
		tombstoneValue, encodeErr := hierarchydeletion.EncodeHierarchyDeletionRecord(nextTombstone, hierarchydeletion.HierarchyDeletionLargeRecordBytes)
		if encodeErr != nil {
			return nil, HierarchyDeletionOperation{}, encodeErr
		}
		defer clear(tombstoneValue)
		conditions = append(conditions, etcdstore.Condition{Key: tombstoneKey, ModRevision: current.TombstoneRevision})
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: tombstoneKey, Value: tombstoneValue})
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return nil, HierarchyDeletionOperation{}, err
	}
	clearKeyValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return nil, HierarchyDeletionOperation{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion action selection changed",
		)
	}
	current.Fence = nextFence
	current.FenceRevision = transaction.Revision
	if action.ProcedureKind == hierarchydeletion.HierarchyDeletionProcedureAgent {
		current.Tombstone = nextTombstone
		current.TombstoneRevision = transaction.Revision
	}
	return &action, current, nil
}

func (repository *HierarchyDeletionRepository) actionAtRevision(
	ctx context.Context,
	operation HierarchyDeletionOperation,
	ordinal int64,
) (hierarchydeletion.HierarchyDeletionAction, error) {
	key, err := hierarchydeletion.HierarchyDeletionActionKey(operation.Tombstone.OperationID, ordinal)
	if err != nil {
		return hierarchydeletion.HierarchyDeletionAction{}, err
	}
	stored, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{key}, Revision: operation.TombstoneRevision,
	})
	if err != nil {
		return hierarchydeletion.HierarchyDeletionAction{}, err
	}
	if stored == nil || len(stored.Values) != 1 || stored.Values[0] == nil || stored.Values[0].Key != key {
		return hierarchydeletion.HierarchyDeletionAction{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	action, err := hierarchydeletion.DecodeHierarchyDeletionAction(stored.Values[0].Value)
	if err != nil || action.ParentOperationID != operation.Tombstone.OperationID || action.Ordinal != ordinal {
		return hierarchydeletion.HierarchyDeletionAction{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	return action, nil
}
