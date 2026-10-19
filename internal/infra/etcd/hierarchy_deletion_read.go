package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyDeletionRepository) OperationByTask(
	ctx context.Context,
	taskID string,
) (hierarchydeletion.HierarchyDeletionOperation, error) {
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
		if decodeErr != nil || record.ID != targetID || record.Kind != hierarchyrecord.ProjectKindBacking ||
			record.TenantID != "" {
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
) (hierarchydeletion.HierarchyDeletionOperation, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	if ids.Validate(ids.KindTask, taskID) != nil || revision < 0 {
		return hierarchydeletion.HierarchyDeletionOperation{}, errs.New(
			errs.KindValidationFailed,
			"hierarchy deletion Task lookup is invalid",
		)
	}
	taskResult, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{
			Keys:     []string{workqueue.TaskKey(taskID), hierarchydeletion.TaskBindingKey(taskID)},
			Revision: revision,
		},
	)
	if err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	if taskResult == nil || len(taskResult.Values) != 2 || taskResult.Values[0] == nil || taskResult.Values[1] == nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, errs.New(
			errs.KindTaskNotFound,
			"hierarchy deletion Task was not found",
		)
	}
	task, err := workqueue.DecodeTask(taskResult.Values[0].Value)
	if err != nil || task.ID != taskID || task.Action != workexecution.ActionRemove {
		return hierarchydeletion.HierarchyDeletionOperation{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	binding, err := hierarchydeletion.DecodeTaskBinding(taskResult.Values[1].Value)
	if err != nil || binding.TaskID != task.ID {
		return hierarchydeletion.HierarchyDeletionOperation{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	if task.ExpectedItems == 0 && task.FinishedAt != nil && task.Outcome != workexecution.Succeeded {
		failed, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{hierarchydeletion.PlanningRejectionKey(task.ID)}, Revision: taskResult.ReadRevision,
		})
		if failed != nil {
			defer etcdstore.ClearValues(failed.Values)
		}
		if err != nil {
			return hierarchydeletion.HierarchyDeletionOperation{}, err
		}
		if failed == nil || failed.ReadRevision != taskResult.ReadRevision || len(failed.Values) != 1 ||
			failed.Values[0] == nil {
			return hierarchydeletion.HierarchyDeletionOperation{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		failure, err := hierarchydeletion.DecodePlanningRejection(failed.Values[0].Value, task)
		if err != nil {
			return hierarchydeletion.HierarchyDeletionOperation{}, err
		}
		if failure.Operation.Tombstone.OperationID != binding.OperationID ||
			failure.Operation.MarkerLocator != binding.Marker {
			return hierarchydeletion.HierarchyDeletionOperation{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		return failure.Operation, nil
	}
	operationID := binding.OperationID
	intentKey, err := hierarchydeletion.HierarchyDeletionIntentKey(operationID)
	if err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	fenceKey, err := hierarchydeletion.HierarchyDeletionCleanupFenceKey(operationID)
	if err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	replayKey, err := hierarchydeletion.HierarchyDeletionReplayTargetKey(operationID)
	if err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	replayResult, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{intentKey, fenceKey, replayKey}, Revision: taskResult.ReadRevision,
	})
	if err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	if replayResult == nil || replayResult.ReadRevision != taskResult.ReadRevision ||
		len(replayResult.Values) != 3 || replayResult.Values[0] == nil ||
		replayResult.Values[1] == nil || replayResult.Values[2] == nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	var intent hierarchydeletion.HierarchyDeletionIntent
	if err := hierarchydeletion.DecodeHierarchyDeletionRecord(
		replayResult.Values[0].Value,
		hierarchydeletion.HierarchyDeletionLargeRecordBytes,
		&intent,
	); err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	var fence hierarchydeletion.HierarchyDeletionCleanupFence
	if err := hierarchydeletion.DecodeHierarchyDeletionRecord(
		replayResult.Values[1].Value,
		hierarchydeletion.HierarchyDeletionSmallRecordBytes,
		&fence,
	); err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	var replay hierarchydeletion.HierarchyDeletionReplayLocator
	if err := hierarchydeletion.DecodeHierarchyDeletionRecord(
		replayResult.Values[2].Value,
		hierarchydeletion.HierarchyDeletionSmallRecordBytes,
		&replay,
	); err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	tombstoneKey := hierarchydeletion.HierarchyDeletionTombstoneKey(string(intent.TargetKind), intent.TargetID)
	tombstoneResult, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{tombstoneKey}, Revision: taskResult.ReadRevision,
	})
	if err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	if tombstoneResult == nil || len(tombstoneResult.Values) != 1 || tombstoneResult.Values[0] == nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	var tombstone hierarchydeletion.HierarchyDeletionTombstone
	if err := hierarchydeletion.DecodeHierarchyDeletionRecord(
		tombstoneResult.Values[0].Value,
		hierarchydeletion.HierarchyDeletionLargeRecordBytes,
		&tombstone,
	); err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	if err := validateHierarchyDeletionOperationSet(task, tombstone, intent, fence, replay); err != nil {
		return hierarchydeletion.HierarchyDeletionOperation{}, err
	}
	if intent.OperationID != binding.OperationID ||
		hierarchydeletion.HierarchyDeletionDigest(replayResult.Values[0].Value) != task.Intent.SHA256 ||
		task.Intent.ID != ids.DeriveAt(ids.KindPlan, task.CreatedAt, task.ID, "hierarchy-removal") {
		return hierarchydeletion.HierarchyDeletionOperation{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	return hierarchydeletion.HierarchyDeletionOperation{
		Tombstone: tombstone, RootTaskID: replay.RootTaskID, MarkerLocator: binding.Marker, Scope: task.Scope,
		TombstoneRevision: tombstoneResult.Values[0].ModRevision,
		Fence:             fence, FenceRevision: replayResult.Values[1].ModRevision,
		Intent: intent, IntentRevision: replayResult.Values[0].ModRevision,
		PlanCursor:     hierarchyDeletionPlanCursor(tombstone),
		SucceededCount: tombstone.Checkpoint.CompletedCount,
		UpdatedAt:      fence.UpdatedAt,
	}, nil
}

func validateHierarchyDeletionOperationSet(
	task workexecution.Task,
	tombstone hierarchydeletion.HierarchyDeletionTombstone,
	intent hierarchydeletion.HierarchyDeletionIntent,
	fence hierarchydeletion.HierarchyDeletionCleanupFence,
	replay hierarchydeletion.HierarchyDeletionReplayLocator,
) error {
	if tombstone.Schema != 1 || intent.Schema != 1 || fence.Schema != 1 || replay.Schema != 1 ||
		tombstone.OperationID != intent.OperationID || tombstone.OperationID != fence.ParentOperationID ||
		tombstone.OperationID != replay.ParentOperationID ||
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
	if task.ID != tombstone.CurrentTaskID || task.ID != replay.RootTaskID || task.TargetID != tombstone.TargetID {
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
