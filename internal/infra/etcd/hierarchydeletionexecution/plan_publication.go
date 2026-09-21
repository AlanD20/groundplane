package hierarchydeletionexecution

import (
	"context"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Executor) AppendActions(
	ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionOperation,
	actions []hierarchydeletion.HierarchyDeletionAction,
	start int64,
	seal bool,
) (hierarchydeletion.HierarchyDeletionOperation, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	if len(actions) > hierarchydeletion.PlanBatchSize || start < 0 ||
		operation.Tombstone.Phase != hierarchydeletion.HierarchyDeletionPlanning || operation.Tombstone.PlanCount != nil ||
		operation.Tombstone.PlanDigest != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, errs.New(
			errs.KindValidationFailed,
			"hierarchy deletion plan append is invalid",
		)
	}
	current, err := repository.operations.OperationByTask(ctx, operation.Tombstone.CurrentTaskID)
	if err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	encoded := make([][]byte, len(actions))
	keys := make([]string, len(actions))
	for index, action := range actions {
		expected := start + int64(index)
		if action.ParentOperationID != current.Tombstone.OperationID || action.Ordinal != expected {
			ClearByteSlices(encoded)
			return hierarchydeletion.HierarchyDeletionOperation{}, errs.New(
				errs.KindValidationFailed,
				"hierarchy deletion plan is not contiguous",
			)
		}
		encoded[index], err = hierarchydeletion.EncodeHierarchyDeletionAction(action)
		if err != nil {
			ClearByteSlices(encoded)
			return hierarchydeletion.HierarchyDeletionOperation{}, err
		}
		keys[index], err = hierarchydeletion.HierarchyDeletionActionKey(action.ParentOperationID, action.Ordinal)
		if err != nil {
			ClearByteSlices(encoded)
			return hierarchydeletion.HierarchyDeletionOperation{}, err
		}
	}
	defer ClearByteSlices(encoded)
	if current.PlanCursor > start {
		if current.PlanCursor < start+int64(len(actions)) {
			return hierarchydeletion.HierarchyDeletionOperation{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		if err := repository.verifyActionBatch(ctx, current, keys, encoded); err != nil {
			return hierarchydeletion.HierarchyDeletionOperation{}, err
		}
		if seal && current.Tombstone.Phase == hierarchydeletion.HierarchyDeletionPlanning {
			return repository.sealPlan(ctx, current)
		}
		return current, nil
	}
	if current.PlanCursor != start {
		return hierarchydeletion.HierarchyDeletionOperation{}, errs.New(errs.KindStateConflict, "hierarchy deletion plan cursor changed")
	}
	if len(actions) > 0 {
		nextTombstone := current.Tombstone
		nextTombstone.Checkpoint.NextOrdinal = start + int64(len(actions))
		tombstoneValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(nextTombstone, hierarchydeletion.HierarchyDeletionLargeRecordBytes)
		if err != nil {
			return hierarchydeletion.HierarchyDeletionOperation{}, err
		}
		defer clear(tombstoneValue)
		conditions := make([]etcdstore.Condition, 0, len(actions)+1)
		conditions = append(conditions, etcdstore.Condition{
			Key: hierarchydeletion.HierarchyDeletionTombstoneKey(
				string(current.Tombstone.TargetKind),
				current.Tombstone.TargetID,
			),
			ModRevision: current.TombstoneRevision,
		})
		mutations := make([]etcdstore.Mutation, 0, len(actions)+1)
		for index, key := range keys {
			conditions = append(conditions, etcdstore.Condition{Key: key})
			mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: key, Value: encoded[index]})
		}
		mutations = append(mutations, etcdstore.Mutation{
			Type:  etcdstore.MutationPut,
			Key:   hierarchydeletion.HierarchyDeletionTombstoneKey(string(current.Tombstone.TargetKind), current.Tombstone.TargetID),
			Value: tombstoneValue,
		})
		if err := hierarchydeletion.ValidateHierarchyDeletionTransaction(
			conditions,
			mutations,
			etcdstore.MaximumOperations,
		); err != nil {
			return hierarchydeletion.HierarchyDeletionOperation{}, err
		}
		transaction, err := repository.store.Transact(ctx, conditions, mutations)
		if err != nil {
			return hierarchydeletion.HierarchyDeletionOperation{}, err
		}
		etcdstore.ClearValues(transaction.FailureReads)
		if !transaction.Succeeded {
			return hierarchydeletion.HierarchyDeletionOperation{}, errs.New(
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

func (repository *Executor) verifyActionBatch(
	ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionOperation,
	keys []string,
	expected [][]byte,
) error {
	if len(keys) == 0 {
		return nil
	}
	stored, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: keys, Revision: operation.TombstoneRevision,
	})
	if err != nil {
		return err
	}
	if stored == nil || len(stored.Values) != len(keys) {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	for index, value := range stored.Values {
		if value == nil || value.Key != keys[index] || !equalBytes(value.Value, expected[index]) {
			return hierarchydeletion.CorruptHierarchyDeletion()
		}
	}
	return nil
}

func (repository *Executor) sealPlan(
	ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionOperation,
) (hierarchydeletion.HierarchyDeletionOperation, error) {
	current, err := repository.operations.OperationByTask(ctx, operation.Tombstone.CurrentTaskID)
	if err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	if current.Tombstone.Phase == hierarchydeletion.HierarchyDeletionExecuting && current.Tombstone.PlanCount != nil &&
		current.Tombstone.PlanDigest != nil && current.Fence.PlanDigest != nil {
		return current, nil
	}
	if current.Tombstone.Phase != hierarchydeletion.HierarchyDeletionPlanning || current.PlanCursor <= 0 {
		return hierarchydeletion.HierarchyDeletionOperation{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion plan cannot be sealed",
		)
	}
	values := make([][]byte, 0, current.PlanCursor)
	for ordinal := int64(0); ordinal < current.PlanCursor; ordinal++ {
		key, keyErr := hierarchydeletion.HierarchyDeletionActionKey(current.Tombstone.OperationID, ordinal)
		if keyErr != nil {
			ClearByteSlices(values)
			return hierarchydeletion.HierarchyDeletionOperation{}, keyErr
		}
		stored, getErr := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{key}, Revision: current.TombstoneRevision,
		})
		if getErr != nil {
			ClearByteSlices(values)
			return hierarchydeletion.HierarchyDeletionOperation{}, getErr
		}
		if stored == nil || len(stored.Values) != 1 || stored.Values[0] == nil {
			ClearByteSlices(values)
			return hierarchydeletion.HierarchyDeletionOperation{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		action, decodeErr := hierarchydeletion.DecodeHierarchyDeletionAction(stored.Values[0].Value)
		if decodeErr != nil || action.Ordinal != ordinal || action.ParentOperationID != current.Tombstone.OperationID {
			ClearByteSlices(values)
			return hierarchydeletion.HierarchyDeletionOperation{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		values = append(values, append([]byte(nil), stored.Values[0].Value...))
	}
	defer ClearByteSlices(values)
	digest := hierarchydeletion.HierarchyDeletionPlanDigest(values)
	count := current.PlanCursor
	nextTombstone := current.Tombstone
	nextTombstone.PlanCount = &count
	nextTombstone.PlanDigest = &digest
	nextTombstone.Phase = hierarchydeletion.HierarchyDeletionExecuting
	nextTombstone.Checkpoint.NextOrdinal = 0
	nextFence := current.Fence
	nextFence.PlanDigest = &digest
	nextFence.Generation++
	nextFence.Phase = hierarchydeletion.HierarchyDeletionExecuting
	nextFence.UpdatedAt = nextFence.UpdatedAt.Add(1)
	tombstoneValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(nextTombstone, hierarchydeletion.HierarchyDeletionLargeRecordBytes)
	if err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	defer clear(tombstoneValue)
	fenceValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(nextFence, hierarchydeletion.HierarchyDeletionSmallRecordBytes)
	if err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	defer clear(fenceValue)
	tombstoneKey := hierarchydeletion.HierarchyDeletionTombstoneKey(string(current.Tombstone.TargetKind), current.Tombstone.TargetID)
	fenceKey, _ := hierarchydeletion.HierarchyDeletionCleanupFenceKey(current.Tombstone.OperationID)
	transaction, err := repository.store.Transact(
		ctx,
		[]etcdstore.Condition{{Key: tombstoneKey, ModRevision: current.TombstoneRevision}, {
			Key: fenceKey, ModRevision: current.FenceRevision,
		}},
		[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: tombstoneKey, Value: tombstoneValue}, {
			Type: etcdstore.MutationPut, Key: fenceKey, Value: fenceValue,
		}},
	)
	if err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	etcdstore.ClearValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return hierarchydeletion.HierarchyDeletionOperation{}, errs.New(errs.KindStateConflict, "hierarchy deletion plan seal changed")
	}
	current.Tombstone = nextTombstone
	current.TombstoneRevision = transaction.Revision
	current.Fence = nextFence
	current.FenceRevision = transaction.Revision
	current.PlanCursor = count
	return current, nil
}

func ClearByteSlices(values [][]byte) {
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
