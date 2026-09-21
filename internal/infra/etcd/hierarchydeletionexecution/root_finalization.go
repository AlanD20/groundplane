package hierarchydeletionexecution

import (
	"context"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Executor) PrepareRootFinalization(
	ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionOperation,
	action hierarchydeletion.HierarchyDeletionAction,
	preparedAt time.Time,
) (hierarchydeletion.HierarchyDeletionOperation, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	if recordcodec.ValidateTimestamp("hierarchy deletion root finalization", preparedAt) != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, errs.New(
			errs.KindValidationFailed,
			"hierarchy deletion root finalization time is invalid",
		)
	}
	current, err := repository.operations.OperationByTask(ctx, operation.Tombstone.CurrentTaskID)
	if err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	if current.Tombstone.Phase == hierarchydeletion.HierarchyDeletionFinalizing {
		if current.Tombstone.PlanCount != nil && action.Ordinal == *current.Tombstone.PlanCount-1 &&
			current.Fence.Dispatch == hierarchydeletion.HierarchyDeletionDispatchRetiring {
			return current, nil
		}
		return hierarchydeletion.HierarchyDeletionOperation{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	if err := ValidateHierarchyDeletionActiveAction(current, action); err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	if current.Tombstone.PlanCount == nil || action.Ordinal != *current.Tombstone.PlanCount-1 ||
		current.Tombstone.Checkpoint.CompletedCount != action.Ordinal ||
		action.ProcedureKind != hierarchydeletion.HierarchyDeletionProcedureController || action.ControllerProcedure == nil ||
		action.TargetKind != hierarchydeletion.HierarchyDeletionActionTargetKind(current.Tombstone.TargetKind) ||
		action.TargetID != current.Tombstone.TargetID || !HierarchyDeletionRootFinalizerMatches(current.Tombstone.TargetKind, action.ActionKind) {
		return hierarchydeletion.HierarchyDeletionOperation{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion root finalizer is not ready",
		)
	}
	nextTombstone := current.Tombstone
	nextTombstone.Phase = hierarchydeletion.HierarchyDeletionFinalizing
	nextFence := current.Fence
	nextFence.Phase = hierarchydeletion.HierarchyDeletionFinalizing
	nextFence.Dispatch = hierarchydeletion.HierarchyDeletionDispatchRetiring
	nextFence.Generation++
	nextFence.UpdatedAt = preparedAt
	tombstoneKey := hierarchydeletion.HierarchyDeletionTombstoneKey(
		string(current.Tombstone.TargetKind),
		current.Tombstone.TargetID,
	)
	fenceKey, _ := hierarchydeletion.HierarchyDeletionCleanupFenceKey(current.Tombstone.OperationID)
	tombstoneValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(
		nextTombstone,
		hierarchydeletion.HierarchyDeletionLargeRecordBytes,
	)
	if err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	defer clear(tombstoneValue)
	fenceValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(
		nextFence,
		hierarchydeletion.HierarchyDeletionSmallRecordBytes,
	)
	if err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
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
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	etcdstore.ClearValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return hierarchydeletion.HierarchyDeletionOperation{}, errs.New(
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

func HierarchyDeletionRootFinalizerMatches(
	target hierarchydeletion.HierarchyDeletionTargetKind,
	action hierarchydeletion.HierarchyDeletionActionKind,
) bool {
	switch target {
	case hierarchydeletion.HierarchyDeletionTargetTenant:
		return action == hierarchydeletion.HierarchyDeletionTenantFinalize
	case hierarchydeletion.HierarchyDeletionTargetProject:
		return action == hierarchydeletion.HierarchyDeletionProjectFinalize
	case hierarchydeletion.HierarchyDeletionTargetEnvironment:
		return action == hierarchydeletion.HierarchyDeletionEnvironmentFinalize
	case hierarchydeletion.HierarchyDeletionTargetBacking:
		return action == hierarchydeletion.HierarchyDeletionBackingServiceFinalize
	default:
		return false
	}
}
