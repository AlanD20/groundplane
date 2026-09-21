package etcd

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionCompletedRoot(
	ctx context.Context,
	operation HierarchyDeletionOperation,
	terminalAt time.Time,
	revision int64,
	nextTombstone *HierarchyDeletionTombstone,
	nextFence *HierarchyDeletionCleanupFence,
	replay *HierarchyDeletionReplayLocator,
) (hierarchyDeletionRootAckChange, error) {
	if operation.Tombstone.Phase != HierarchyDeletionFinalizing ||
		operation.Fence.Phase != HierarchyDeletionFinalizing ||
		operation.Fence.Dispatch != HierarchyDeletionDispatchRetiring ||
		operation.Tombstone.PlanCount == nil ||
		operation.Tombstone.PlanDigest == nil ||
		*operation.Tombstone.PlanCount <= 0 ||
		operation.Tombstone.Checkpoint.CompletedCount != *operation.Tombstone.PlanCount-1 ||
		operation.Fence.ActiveActionOrdinal == nil ||
		*operation.Fence.ActiveActionOrdinal != *operation.Tombstone.PlanCount-1 {
		return hierarchyDeletionRootAckChange{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion root is not ready",
		)
	}
	rootOrdinal := *operation.Tombstone.PlanCount - 1
	actionKey, _ := HierarchyDeletionActionKey(operation.Tombstone.OperationID, rootOrdinal)
	actionRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{actionKey}, Revision: revision})
	if err != nil {
		return hierarchyDeletionRootAckChange{}, err
	}
	if actionRead == nil || len(actionRead.Values) != 1 || actionRead.Values[0] == nil {
		return hierarchyDeletionRootAckChange{}, corruptHierarchyDeletion()
	}
	action, err := decodeHierarchyDeletionAction(actionRead.Values[0].Value)
	if err != nil || action.Ordinal != rootOrdinal || action.ParentOperationID != operation.Tombstone.OperationID ||
		action.TargetID != operation.Tombstone.TargetID || action.ProcedureKind != HierarchyDeletionProcedureController ||
		action.ControllerProcedure == nil || !hierarchyDeletionRootFinalizerMatches(operation.Tombstone.TargetKind, action.ActionKind) {
		return hierarchyDeletionRootAckChange{}, corruptHierarchyDeletion()
	}
	if err := repository.validateHierarchyDeletionRootProjection(ctx, operation); err != nil {
		return hierarchyDeletionRootAckChange{}, err
	}
	effects, err := repository.prepareHierarchyDeletionControllerEffects(ctx, operation, action)
	if err != nil {
		return hierarchyDeletionRootAckChange{}, err
	}
	change := hierarchyDeletionRootAckChange{
		conditions: effects.conditions,
		mutations:  effects.mutations,
		values:     effects.values,
	}
	expected, err := bindHierarchyDeletionControllerProcedure(
		HierarchyDeletionPlannedAction{
			NodeID: action.NodeID, Ordinal: action.Ordinal, ParentOperationID: action.ParentOperationID,
			ActionKind: action.ActionKind, TargetKind: action.TargetKind, TargetID: action.TargetID,
			TargetRevision: action.TargetRevision,
		},
		HierarchyDeletionControllerFinalizerInput{
			Finalizer: action.ControllerProcedure.Finalizer, TargetKind: action.TargetKind,
			TargetID: action.TargetID, FixedInputRevision: action.TargetRevision,
			FixedInputDigest: effects.fixedInputDigest, BatchOrdinal: 0, BatchCount: 1,
		},
	)
	if err != nil || expected != *action.ControllerProcedure {
		change.clear()
		return hierarchyDeletionRootAckChange{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion root template changed",
		)
	}
	actionValue, err := encodeHierarchyDeletionAction(action)
	if err != nil {
		change.clear()
		return hierarchyDeletionRootAckChange{}, err
	}
	defer clear(actionValue)
	completion := HierarchyDeletionActionCompletion{
		Schema: 1, ParentOperationID: operation.Tombstone.OperationID,
		DeletionEpoch: operation.Tombstone.DeletionEpoch, Ordinal: action.Ordinal,
		ActionDigest: hierarchyDeletionBytesDigest(actionValue), TargetKind: action.TargetKind,
		TargetID: action.TargetID, TargetRevision: action.TargetRevision,
		Executor: HierarchyDeletionProcedureController,
		ControllerProof: &HierarchyDeletionControllerCompletionProof{
			Finalizer: action.ControllerProcedure.Finalizer, FixedInputRevision: action.ControllerProcedure.FixedInputRevision,
			CompareTemplateDigest:       action.ControllerProcedure.CompareTemplateDigest,
			MutationTemplateDigest:      action.ControllerProcedure.MutationTemplateDigest,
			PostconditionTemplateDigest: action.ControllerProcedure.PostconditionTemplateDigest,
		},
		CompletedAt: terminalAt,
	}
	completionValue, err := encodeHierarchyDeletionRecord(completion, hierarchyDeletionCompletionRecordBytes)
	if err != nil {
		change.clear()
		return hierarchyDeletionRootAckChange{}, err
	}
	nextTombstoneValue, nextFenceValue := advanceHierarchyDeletionCheckpoint(
		operation, action, completion.ActionDigest, action.ControllerProcedure.MutationTemplateDigest, terminalAt,
	)
	*nextTombstone = nextTombstoneValue
	*nextFence = nextFenceValue
	nextTombstone.Phase = HierarchyDeletionRetained
	nextFence.Phase = HierarchyDeletionRetained
	nextFence.Dispatch = HierarchyDeletionDispatchClosed
	nextFence.ActiveActionOrdinal = nil
	nextTombstone.Terminal = &HierarchyDeletionTerminal{
		Status: string(taskjournal.TaskStatusCompleted), TaskID: operation.Tombstone.CurrentTaskID,
		CompletedAt: terminalAt, RetainUntil: terminalAt.Add(TaskRetention),
		CompletionSummaryDigest: nextTombstone.Checkpoint.CompletedPrefixDigest,
	}
	replay.RetainUntil = &nextTombstone.Terminal.RetainUntil
	summary, err := repository.buildHierarchyDeletionSummaries(
		ctx, operation, completionValue, nextTombstone.Checkpoint.CompletedPrefixDigest, terminalAt, revision,
	)
	if err != nil {
		clear(completionValue)
		change.clear()
		return hierarchyDeletionRootAckChange{}, err
	}
	change.conditions = append(change.conditions, summary.conditions...)
	change.mutations = append(change.mutations, etcdstore.Mutation{
		Type: etcdstore.MutationPut, Key: mustHierarchyDeletionCompletionKey(operation.Tombstone.OperationID, rootOrdinal), Value: completionValue,
	})
	change.mutations = append(change.mutations, summary.mutations...)
	change.values = append(change.values, completionValue)
	change.values = append(change.values, summary.values...)
	return change, nil
}

func (repository *HierarchyDeletionRepository) validateHierarchyDeletionRootProjection(
	ctx context.Context,
	operation HierarchyDeletionOperation,
) error {
	key := hierarchyDeletionPrimaryKey(operation.Tombstone.TargetKind, operation.Tombstone.TargetID)
	read, err := repository.store.Get(ctx, key)
	if err != nil {
		return err
	}
	if read.Entry == nil {
		return errs.New(errs.KindStateConflict, "hierarchy deletion root projection is missing")
	}
	defer clear(read.Entry.Value)
	deletionTaskID := ""
	switch operation.Tombstone.TargetKind {
	case HierarchyDeletionTargetTenant:
		record, decodeErr := hierarchyrecord.DecodeTenant(read.Entry.Value)
		if decodeErr != nil || record.ID != operation.Tombstone.TargetID {
			return corruptHierarchyDeletion()
		}
		deletionTaskID = record.DeletionTaskID
	case HierarchyDeletionTargetProject, HierarchyDeletionTargetBacking:
		record, decodeErr := hierarchyrecord.DecodeProject(read.Entry.Value)
		if decodeErr != nil || record.ID != operation.Tombstone.TargetID {
			return corruptHierarchyDeletion()
		}
		deletionTaskID = record.DeletionTaskID
	case HierarchyDeletionTargetEnvironment:
		record, decodeErr := hierarchyrecord.DecodeEnvironment(read.Entry.Value)
		if decodeErr != nil || record.ID != operation.Tombstone.TargetID {
			return corruptHierarchyDeletion()
		}
		deletionTaskID = record.DeletionTaskID
	default:
		return corruptHierarchyDeletion()
	}
	if deletionTaskID != operation.Tombstone.CurrentTaskID {
		return errs.New(errs.KindStateConflict, "hierarchy deletion root Task projection changed")
	}
	return nil
}

func mustHierarchyDeletionCompletionKey(operationID string, ordinal int64) string {
	key, _ := HierarchyDeletionCompletionKey(operationID, ordinal)
	return key
}
