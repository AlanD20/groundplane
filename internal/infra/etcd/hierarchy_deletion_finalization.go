package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyDeletionRepository) PrepareRootFinalization(
	ctx context.Context,
	operation HierarchyDeletionOperation,
	action HierarchyDeletionAction,
	preparedAt time.Time,
) (HierarchyDeletionOperation, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return HierarchyDeletionOperation{}, err
	}
	if recordcodec.ValidateTimestamp("hierarchy deletion root finalization", preparedAt) != nil {
		return HierarchyDeletionOperation{}, errs.New(
			errs.KindValidationFailed,
			"hierarchy deletion root finalization time is invalid",
		)
	}
	current, err := repository.OperationByTask(ctx, operation.Tombstone.CurrentTaskID)
	if err != nil {
		return HierarchyDeletionOperation{}, err
	}
	if current.Tombstone.Phase == HierarchyDeletionFinalizing {
		if current.Tombstone.PlanCount != nil && action.Ordinal == *current.Tombstone.PlanCount-1 &&
			current.Fence.Dispatch == HierarchyDeletionDispatchRetiring {
			return current, nil
		}
		return HierarchyDeletionOperation{}, corruptHierarchyDeletion()
	}
	if err := validateHierarchyDeletionActiveAction(current, action); err != nil {
		return HierarchyDeletionOperation{}, err
	}
	if current.Tombstone.PlanCount == nil || action.Ordinal != *current.Tombstone.PlanCount-1 ||
		current.Tombstone.Checkpoint.CompletedCount != action.Ordinal ||
		action.ProcedureKind != HierarchyDeletionProcedureController || action.ControllerProcedure == nil ||
		action.TargetKind != HierarchyDeletionActionTargetKind(current.Tombstone.TargetKind) ||
		action.TargetID != current.Tombstone.TargetID || !hierarchyDeletionRootFinalizerMatches(current.Tombstone.TargetKind, action.ActionKind) {
		return HierarchyDeletionOperation{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion root finalizer is not ready",
		)
	}
	nextTombstone := current.Tombstone
	nextTombstone.Phase = HierarchyDeletionFinalizing
	nextFence := current.Fence
	nextFence.Phase = HierarchyDeletionFinalizing
	nextFence.Dispatch = HierarchyDeletionDispatchRetiring
	nextFence.Generation++
	nextFence.UpdatedAt = preparedAt
	tombstoneKey := HierarchyDeletionTombstoneKey(string(current.Tombstone.TargetKind), current.Tombstone.TargetID)
	fenceKey, _ := HierarchyDeletionCleanupFenceKey(current.Tombstone.OperationID)
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
	transaction, err := repository.store.Transact(
		ctx,
		[]etcdstore.Condition{
			{Key: tombstoneKey, ModRevision: current.TombstoneRevision},
			{Key: fenceKey, ModRevision: current.FenceRevision},
		},
		[]etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: tombstoneKey, Value: tombstoneValue},
			{Type: etcdstore.MutationPut, Key: fenceKey, Value: fenceValue},
		},
	)
	if err != nil {
		return HierarchyDeletionOperation{}, err
	}
	clearKeyValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return HierarchyDeletionOperation{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion root finalization changed",
		)
	}
	current.Tombstone = nextTombstone
	current.TombstoneRevision = transaction.Revision
	current.Fence = nextFence
	current.FenceRevision = transaction.Revision
	current.UpdatedAt = preparedAt
	return current, nil
}

func hierarchyDeletionRootFinalizerMatches(
	target HierarchyDeletionTargetKind,
	action HierarchyDeletionActionKind,
) bool {
	switch target {
	case HierarchyDeletionTargetTenant:
		return action == HierarchyDeletionTenantFinalize
	case HierarchyDeletionTargetProject:
		return action == HierarchyDeletionProjectFinalize
	case HierarchyDeletionTargetEnvironment:
		return action == HierarchyDeletionEnvironmentFinalize
	case HierarchyDeletionTargetBacking:
		return action == HierarchyDeletionBackingServiceFinalize
	default:
		return false
	}
}
