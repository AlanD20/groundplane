package hierarchydeletionexecution

import (
	"context"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Executor) ReadyAction(
	ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionOperation,
) (*hierarchydeletion.HierarchyDeletionAction, hierarchydeletion.HierarchyDeletionOperation, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return nil, hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	current, err := repository.operations.OperationByTask(ctx, operation.Tombstone.CurrentTaskID)
	if err != nil {
		return nil, hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	if current.Tombstone.Phase != hierarchydeletion.HierarchyDeletionExecuting || current.Tombstone.PlanCount == nil ||
		current.Tombstone.PlanDigest == nil || current.Fence.Dispatch != hierarchydeletion.HierarchyDeletionDispatchOpen {
		return nil, hierarchydeletion.HierarchyDeletionOperation{}, errs.New(
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
			return nil, hierarchydeletion.HierarchyDeletionOperation{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		action, readErr := repository.actionAtRevision(ctx, current, ordinal)
		return &action, current, readErr
	}
	action, err := repository.actionAtRevision(ctx, current, ordinal)
	if err != nil {
		return nil, hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	for _, prerequisite := range action.PrerequisiteOrdinals {
		if prerequisite >= ordinal || prerequisite >= current.Tombstone.Checkpoint.CompletedCount {
			return nil, hierarchydeletion.HierarchyDeletionOperation{}, hierarchydeletion.CorruptHierarchyDeletion()
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
	fenceValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(
		nextFence,
		hierarchydeletion.HierarchyDeletionSmallRecordBytes,
	)
	if err != nil {
		return nil, hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	defer clear(fenceValue)
	conditions := []etcdstore.Condition{{Key: fenceKey, ModRevision: current.FenceRevision}}
	mutations := []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: fenceKey, Value: fenceValue}}
	var nextTombstone hierarchydeletion.HierarchyDeletionTombstone
	if action.ProcedureKind == hierarchydeletion.HierarchyDeletionProcedureAgent {
		nextTombstone = current.Tombstone
		nextTombstone.Checkpoint.ActiveChildOperationID = action.AgentProcedure.ChildOperationID
		tombstoneKey := hierarchydeletion.HierarchyDeletionTombstoneKey(
			string(current.Tombstone.TargetKind),
			current.Tombstone.TargetID,
		)
		tombstoneValue, encodeErr := hierarchydeletion.EncodeHierarchyDeletionRecord(
			nextTombstone,
			hierarchydeletion.HierarchyDeletionLargeRecordBytes,
		)
		if encodeErr != nil {
			return nil, hierarchydeletion.HierarchyDeletionOperation{}, encodeErr
		}
		defer clear(tombstoneValue)
		conditions = append(conditions, etcdstore.Condition{Key: tombstoneKey, ModRevision: current.TombstoneRevision})
		mutations = append(
			mutations,
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: tombstoneKey, Value: tombstoneValue},
		)
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return nil, hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	etcdstore.ClearValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return nil, hierarchydeletion.HierarchyDeletionOperation{}, errs.New(
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

func (repository *Executor) actionAtRevision(
	ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionOperation,
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
