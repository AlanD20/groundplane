package etcd

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyDeletionRepository) OperationByTask(
	ctx context.Context,
	taskID string,
) (HierarchyDeletionOperation, error) {
	return repository.OperationByTaskAtRevision(ctx, taskID, 0)
}

func (repository *HierarchyDeletionRepository) GetDeletionTaskIDAtRevision(
	ctx context.Context,
	targetKind hierarchydeletion.HierarchyDeletionTargetKind,
	targetID string,
	revision int64,
) (*string, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return nil, err
	}
	if !hierarchydeletion.ValidHierarchyDeletionTarget(targetKind, targetID) || revision <= 0 {
		return nil, errs.New(errs.KindValidationFailed, "hierarchy deletion projection lookup is invalid")
	}
	stored, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{hierarchyDeletionPrimaryKey(targetKind, targetID)}, Revision: revision,
	})
	if err != nil {
		return nil, err
	}
	if stored == nil || stored.ReadRevision != revision || len(stored.Values) != 1 {
		return nil, hierarchydeletion.CorruptHierarchyDeletion()
	}
	if stored.Values[0] == nil {
		return nil, nil
	}
	var taskID string
	switch targetKind {
	case hierarchydeletion.HierarchyDeletionTargetTenant:
		record, decodeErr := hierarchyrecord.DecodeTenant(stored.Values[0].Value)
		if decodeErr != nil || record.ID != targetID {
			return nil, hierarchydeletion.CorruptHierarchyDeletion()
		}
		taskID = record.DeletionTaskID
	case hierarchydeletion.HierarchyDeletionTargetProject:
		record, decodeErr := hierarchyrecord.DecodeProject(stored.Values[0].Value)
		if decodeErr != nil || record.ID != targetID {
			return nil, hierarchydeletion.CorruptHierarchyDeletion()
		}
		taskID = record.DeletionTaskID
	case hierarchydeletion.HierarchyDeletionTargetBacking:
		record, decodeErr := hierarchyrecord.DecodeProject(stored.Values[0].Value)
		if decodeErr != nil || record.ID != targetID || record.Kind != hierarchyrecord.ProjectKindBacking || record.TenantID != "" {
			return nil, hierarchydeletion.CorruptHierarchyDeletion()
		}
		taskID = record.DeletionTaskID
	case hierarchydeletion.HierarchyDeletionTargetEnvironment:
		record, decodeErr := hierarchyrecord.DecodeEnvironment(stored.Values[0].Value)
		if decodeErr != nil || record.ID != targetID {
			return nil, hierarchydeletion.CorruptHierarchyDeletion()
		}
		taskID = record.DeletionTaskID
	}
	if taskID == "" {
		return nil, nil
	}
	if ids.Validate(ids.KindTask, taskID) != nil {
		return nil, hierarchydeletion.CorruptHierarchyDeletion()
	}
	return &taskID, nil
}

func (repository *HierarchyDeletionRepository) OperationByTaskAtRevision(
	ctx context.Context,
	taskID string,
	revision int64,
) (HierarchyDeletionOperation, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return HierarchyDeletionOperation{}, err
	}
	if ids.Validate(ids.KindTask, taskID) != nil || revision < 0 {
		return HierarchyDeletionOperation{}, errs.New(
			errs.KindValidationFailed,
			"hierarchy deletion Task lookup is invalid",
		)
	}
	taskResult, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{taskjournal.TaskStorageKey(taskID)}, Revision: revision},
	)
	if err != nil {
		return HierarchyDeletionOperation{}, err
	}
	if taskResult == nil || len(taskResult.Values) != 1 || taskResult.Values[0] == nil {
		return HierarchyDeletionOperation{}, errs.New(errs.KindTaskNotFound, "hierarchy deletion Task was not found")
	}
	task, err := decodeTaskRecord(taskResult.Values[0].Value)
	if err != nil || task.ID != taskID || task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceHierarchyDeletion {
		return HierarchyDeletionOperation{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	if task.idempotencyMarker == nil || idempotencyrecord.ValidateIdempotencyLocator(*task.idempotencyMarker) != nil {
		return HierarchyDeletionOperation{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	operationID := task.Params[taskjournal.TaskHierarchyDeletionOperationParam]
	if !hierarchydeletion.ValidHierarchyDeletionPrivateID(operationID, "del") {
		return HierarchyDeletionOperation{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	intentKey, err := hierarchydeletion.HierarchyDeletionIntentKey(operationID)
	if err != nil {
		return HierarchyDeletionOperation{}, err
	}
	fenceKey, err := hierarchydeletion.HierarchyDeletionCleanupFenceKey(operationID)
	if err != nil {
		return HierarchyDeletionOperation{}, err
	}
	replayKey, err := hierarchydeletion.HierarchyDeletionReplayTargetKey(operationID)
	if err != nil {
		return HierarchyDeletionOperation{}, err
	}
	replayResult, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{intentKey, fenceKey, replayKey}, Revision: taskResult.ReadRevision,
	})
	if err != nil {
		return HierarchyDeletionOperation{}, err
	}
	if replayResult == nil || replayResult.ReadRevision != taskResult.ReadRevision ||
		len(replayResult.Values) != 3 || replayResult.Values[0] == nil ||
		replayResult.Values[1] == nil || replayResult.Values[2] == nil {
		return HierarchyDeletionOperation{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	var intent hierarchydeletion.HierarchyDeletionIntent
	if err := hierarchydeletion.DecodeHierarchyDeletionRecord(
		replayResult.Values[0].Value,
		hierarchydeletion.HierarchyDeletionLargeRecordBytes,
		&intent,
	); err != nil {
		return HierarchyDeletionOperation{}, err
	}
	var fence hierarchydeletion.HierarchyDeletionCleanupFence
	if err := hierarchydeletion.DecodeHierarchyDeletionRecord(
		replayResult.Values[1].Value,
		hierarchydeletion.HierarchyDeletionSmallRecordBytes,
		&fence,
	); err != nil {
		return HierarchyDeletionOperation{}, err
	}
	var replay hierarchydeletion.HierarchyDeletionReplayLocator
	if err := hierarchydeletion.DecodeHierarchyDeletionRecord(
		replayResult.Values[2].Value,
		hierarchydeletion.HierarchyDeletionSmallRecordBytes,
		&replay,
	); err != nil {
		return HierarchyDeletionOperation{}, err
	}
	tombstoneKey := hierarchydeletion.HierarchyDeletionTombstoneKey(string(intent.TargetKind), intent.TargetID)
	tombstoneResult, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{tombstoneKey}, Revision: taskResult.ReadRevision,
	})
	if err != nil {
		return HierarchyDeletionOperation{}, err
	}
	if tombstoneResult == nil || len(tombstoneResult.Values) != 1 || tombstoneResult.Values[0] == nil {
		return HierarchyDeletionOperation{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	var tombstone hierarchydeletion.HierarchyDeletionTombstone
	if err := hierarchydeletion.DecodeHierarchyDeletionRecord(
		tombstoneResult.Values[0].Value,
		hierarchydeletion.HierarchyDeletionLargeRecordBytes,
		&tombstone,
	); err != nil {
		return HierarchyDeletionOperation{}, err
	}
	if err := validateHierarchyDeletionOperationSet(task, tombstone, intent, fence, replay); err != nil {
		return HierarchyDeletionOperation{}, err
	}
	return HierarchyDeletionOperation{
		Tombstone: tombstone, RootTaskID: replay.RootTaskID, MarkerLocator: *task.idempotencyMarker, Owner: task.Owner,
		TombstoneRevision: tombstoneResult.Values[0].ModRevision,
		Fence:             fence, FenceRevision: replayResult.Values[1].ModRevision,
		Intent: intent, IntentRevision: replayResult.Values[0].ModRevision,
		PlanCursor:     hierarchyDeletionPlanCursor(tombstone),
		SucceededCount: tombstone.Checkpoint.CompletedCount,
		UpdatedAt:      fence.UpdatedAt,
	}, nil
}

func validateHierarchyDeletionOperationSet(
	task TaskRecord,
	tombstone hierarchydeletion.HierarchyDeletionTombstone,
	intent hierarchydeletion.HierarchyDeletionIntent,
	fence hierarchydeletion.HierarchyDeletionCleanupFence,
	replay hierarchydeletion.HierarchyDeletionReplayLocator,
) error {
	if tombstone.Schema != 1 || intent.Schema != 1 || fence.Schema != 1 || replay.Schema != 1 ||
		tombstone.OperationID != intent.OperationID || tombstone.OperationID != fence.ParentOperationID ||
		tombstone.OperationID != replay.ParentOperationID || tombstone.TaskOperationID != task.OperationID ||
		tombstone.DeletionEpoch != intent.DeletionEpoch || tombstone.DeletionEpoch != fence.DeletionEpoch ||
		tombstone.DeletionEpoch != replay.DeletionEpoch || tombstone.TargetKind != intent.TargetKind ||
		tombstone.TargetID != intent.TargetID || tombstone.OperationKind != intent.OperationKind ||
		tombstone.OperationKind != replay.OperationKind || tombstone.TargetKind != replay.TargetKind ||
		tombstone.TargetID != replay.TargetID || tombstone.CurrentTaskID != replay.CurrentTaskID ||
		fence.CurrentTaskID != replay.CurrentTaskID || replay.TombstoneKey != hierarchydeletion.HierarchyDeletionTombstoneKey(
		string(tombstone.TargetKind), tombstone.TargetID,
	) || replay.ResponseDigest != hierarchyDeletionResponseDigest(replay.RootTaskID) ||
		!hierarchydeletion.ValidHierarchyDeletionPhase(tombstone.Phase) || tombstone.Phase != fence.Phase ||
		fence.Generation <= 0 || !hierarchydeletion.ValidHierarchyDeletionTimestamp(fence.UpdatedAt) {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	if task.ID != tombstone.CurrentTaskID && task.ID != replay.RootTaskID && task.RetryOf == "" {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	return nil
}

func hierarchyDeletionPlanCursor(tombstone hierarchydeletion.HierarchyDeletionTombstone) int64 {
	if tombstone.Phase == hierarchydeletion.HierarchyDeletionPlanning {
		return tombstone.Checkpoint.NextOrdinal
	}
	if tombstone.PlanCount != nil {
		return *tombstone.PlanCount
	}
	return 0
}

func cloneHierarchyDeletionOperation(operation HierarchyDeletionOperation) HierarchyDeletionOperation {
	cloned := operation
	cloned.Tombstone.PlanCount = cloneInt64Pointer(operation.Tombstone.PlanCount)
	cloned.Tombstone.PlanDigest = cloneStringPointer(operation.Tombstone.PlanDigest)
	cloned.Tombstone.Terminal = cloneHierarchyDeletionTerminal(operation.Tombstone.Terminal)
	cloned.Fence.PlanDigest = cloneStringPointer(operation.Fence.PlanDigest)
	cloned.Fence.ActiveActionOrdinal = cloneInt64Pointer(operation.Fence.ActiveActionOrdinal)
	cloned.Intent = operation.Intent
	return cloned
}

func clearHierarchyDeletionOperation(operation HierarchyDeletionOperation) {
	operation.Tombstone.PlanDigest = nil
	operation.Fence.PlanDigest = nil
	operation.Intent.RootSlug = ""
}

func cloneInt64Pointer(value *int64) *int64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneStringPointer(value *string) *string {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneHierarchyDeletionTerminal(value *hierarchydeletion.HierarchyDeletionTerminal) *hierarchydeletion.HierarchyDeletionTerminal {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
