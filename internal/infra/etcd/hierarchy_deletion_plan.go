package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyDeletionRepository) AppendActions(
	ctx context.Context,
	operation HierarchyDeletionOperation,
	actions []HierarchyDeletionAction,
	start int64,
	seal bool,
) (HierarchyDeletionOperation, error) {
	if err := validateContext(ctx); err != nil {
		return HierarchyDeletionOperation{}, err
	}
	if len(actions) > hierarchyDeletionPlanBatchSize || start < 0 ||
		operation.Tombstone.Phase != HierarchyDeletionPlanning || operation.Tombstone.PlanCount != nil ||
		operation.Tombstone.PlanDigest != nil {
		return HierarchyDeletionOperation{}, errs.New(
			errs.KindValidationFailed,
			"hierarchy deletion plan append is invalid",
		)
	}
	current, err := repository.OperationByTask(ctx, operation.Tombstone.CurrentTaskID)
	if err != nil {
		return HierarchyDeletionOperation{}, err
	}
	encoded := make([][]byte, len(actions))
	keys := make([]string, len(actions))
	for index, action := range actions {
		expected := start + int64(index)
		if action.ParentOperationID != current.Tombstone.OperationID || action.Ordinal != expected {
			clearByteSlices(encoded)
			return HierarchyDeletionOperation{}, errs.New(
				errs.KindValidationFailed,
				"hierarchy deletion plan is not contiguous",
			)
		}
		encoded[index], err = encodeHierarchyDeletionAction(action)
		if err != nil {
			clearByteSlices(encoded)
			return HierarchyDeletionOperation{}, err
		}
		keys[index], err = HierarchyDeletionActionKey(action.ParentOperationID, action.Ordinal)
		if err != nil {
			clearByteSlices(encoded)
			return HierarchyDeletionOperation{}, err
		}
	}
	defer clearByteSlices(encoded)
	if current.PlanCursor > start {
		if current.PlanCursor < start+int64(len(actions)) {
			return HierarchyDeletionOperation{}, corruptHierarchyDeletion()
		}
		if err := repository.verifyActionBatch(ctx, current, keys, encoded); err != nil {
			return HierarchyDeletionOperation{}, err
		}
		if seal && current.Tombstone.Phase == HierarchyDeletionPlanning {
			return repository.sealPlan(ctx, current)
		}
		return current, nil
	}
	if current.PlanCursor != start {
		return HierarchyDeletionOperation{}, errs.New(errs.KindStateConflict, "hierarchy deletion plan cursor changed")
	}
	if len(actions) > 0 {
		nextTombstone := current.Tombstone
		nextTombstone.Checkpoint.NextOrdinal = start + int64(len(actions))
		tombstoneValue, err := encodeHierarchyDeletionRecord(nextTombstone, hierarchyDeletionLargeRecordBytes)
		if err != nil {
			return HierarchyDeletionOperation{}, err
		}
		defer clear(tombstoneValue)
		conditions := make([]Condition, 0, len(actions)+1)
		conditions = append(conditions, Condition{
			Key: HierarchyDeletionTombstoneKey(
				string(current.Tombstone.TargetKind),
				current.Tombstone.TargetID,
			),
			ModRevision: current.TombstoneRevision,
		})
		mutations := make([]Mutation, 0, len(actions)+1)
		for index, key := range keys {
			conditions = append(conditions, Condition{Key: key})
			mutations = append(mutations, Mutation{Type: MutationPut, Key: key, Value: encoded[index]})
		}
		mutations = append(mutations, Mutation{
			Type:  MutationPut,
			Key:   HierarchyDeletionTombstoneKey(string(current.Tombstone.TargetKind), current.Tombstone.TargetID),
			Value: tombstoneValue,
		})
		if err := validateHierarchyDeletionTransaction(
			conditions,
			mutations,
			maximumTransactionOperations,
		); err != nil {
			return HierarchyDeletionOperation{}, err
		}
		transaction, err := repository.store.Transact(ctx, conditions, mutations)
		if err != nil {
			return HierarchyDeletionOperation{}, err
		}
		clearKeyValues(transaction.FailureReads)
		if !transaction.Succeeded {
			return HierarchyDeletionOperation{}, errs.New(
				errs.KindStateConflict,
				"hierarchy deletion plan append changed",
			)
		}
		current.Tombstone = nextTombstone
		current.TombstoneRevision = transaction.Revision
		current.PlanCursor = nextTombstone.Checkpoint.NextOrdinal
	}
	if seal {
		return repository.sealPlan(ctx, current)
	}
	return current, nil
}

func (repository *HierarchyDeletionRepository) verifyActionBatch(
	ctx context.Context,
	operation HierarchyDeletionOperation,
	keys []string,
	expected [][]byte,
) error {
	if len(keys) == 0 {
		return nil
	}
	stored, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: keys, Revision: operation.TombstoneRevision,
	})
	if err != nil {
		return err
	}
	if stored == nil || len(stored.Values) != len(keys) {
		return corruptHierarchyDeletion()
	}
	for index, value := range stored.Values {
		if value == nil || value.Key != keys[index] || !equalBytes(value.Value, expected[index]) {
			return corruptHierarchyDeletion()
		}
	}
	return nil
}

func (repository *HierarchyDeletionRepository) sealPlan(
	ctx context.Context,
	operation HierarchyDeletionOperation,
) (HierarchyDeletionOperation, error) {
	current, err := repository.OperationByTask(ctx, operation.Tombstone.CurrentTaskID)
	if err != nil {
		return HierarchyDeletionOperation{}, err
	}
	if current.Tombstone.Phase == HierarchyDeletionExecuting && current.Tombstone.PlanCount != nil &&
		current.Tombstone.PlanDigest != nil && current.Fence.PlanDigest != nil {
		return current, nil
	}
	if current.Tombstone.Phase != HierarchyDeletionPlanning || current.PlanCursor <= 0 {
		return HierarchyDeletionOperation{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion plan cannot be sealed",
		)
	}
	values := make([][]byte, 0, current.PlanCursor)
	for ordinal := int64(0); ordinal < current.PlanCursor; ordinal++ {
		key, keyErr := HierarchyDeletionActionKey(current.Tombstone.OperationID, ordinal)
		if keyErr != nil {
			clearByteSlices(values)
			return HierarchyDeletionOperation{}, keyErr
		}
		stored, getErr := repository.store.GetMany(ctx, GetManyRequest{
			Keys: []string{key}, Revision: current.TombstoneRevision,
		})
		if getErr != nil {
			clearByteSlices(values)
			return HierarchyDeletionOperation{}, getErr
		}
		if stored == nil || len(stored.Values) != 1 || stored.Values[0] == nil {
			clearByteSlices(values)
			return HierarchyDeletionOperation{}, corruptHierarchyDeletion()
		}
		action, decodeErr := decodeHierarchyDeletionAction(stored.Values[0].Value)
		if decodeErr != nil || action.Ordinal != ordinal || action.ParentOperationID != current.Tombstone.OperationID {
			clearByteSlices(values)
			return HierarchyDeletionOperation{}, corruptHierarchyDeletion()
		}
		values = append(values, append([]byte(nil), stored.Values[0].Value...))
	}
	defer clearByteSlices(values)
	digest := hierarchyDeletionPlanDigest(values)
	count := current.PlanCursor
	nextTombstone := current.Tombstone
	nextTombstone.PlanCount = &count
	nextTombstone.PlanDigest = &digest
	nextTombstone.Phase = HierarchyDeletionExecuting
	nextTombstone.Checkpoint.NextOrdinal = 0
	nextFence := current.Fence
	nextFence.PlanDigest = &digest
	nextFence.Generation++
	nextFence.Phase = HierarchyDeletionExecuting
	nextFence.UpdatedAt = nextFence.UpdatedAt.Add(1)
	tombstoneValue, err := encodeHierarchyDeletionRecord(nextTombstone, hierarchyDeletionLargeRecordBytes)
	if err != nil {
		return HierarchyDeletionOperation{}, err
	}
	defer clear(tombstoneValue)
	fenceValue, err := encodeHierarchyDeletionRecord(nextFence, hierarchyDeletionSmallRecordBytes)
	if err != nil {
		return HierarchyDeletionOperation{}, err
	}
	defer clear(fenceValue)
	tombstoneKey := HierarchyDeletionTombstoneKey(string(current.Tombstone.TargetKind), current.Tombstone.TargetID)
	fenceKey, _ := HierarchyDeletionCleanupFenceKey(current.Tombstone.OperationID)
	transaction, err := repository.store.Transact(
		ctx,
		[]Condition{{Key: tombstoneKey, ModRevision: current.TombstoneRevision}, {
			Key: fenceKey, ModRevision: current.FenceRevision,
		}},
		[]Mutation{{Type: MutationPut, Key: tombstoneKey, Value: tombstoneValue}, {
			Type: MutationPut, Key: fenceKey, Value: fenceValue,
		}},
	)
	if err != nil {
		return HierarchyDeletionOperation{}, err
	}
	clearKeyValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return HierarchyDeletionOperation{}, errs.New(errs.KindStateConflict, "hierarchy deletion plan seal changed")
	}
	current.Tombstone = nextTombstone
	current.TombstoneRevision = transaction.Revision
	current.Fence = nextFence
	current.FenceRevision = transaction.Revision
	current.PlanCursor = count
	return current, nil
}

func clearByteSlices(values [][]byte) {
	for _, value := range values {
		clear(value)
	}
}

func equalBytes(left []byte, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
