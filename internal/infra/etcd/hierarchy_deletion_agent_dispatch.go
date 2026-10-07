package etcd

import (
	"context"
	"encoding/hex"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	hierarchydeletionexecution "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionexecution"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"strconv"
	"time"

	"github.com/AlanD20/groundplane/internal/common/hierarchyplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyDeletionRepository) PublishOrResumeAgentAction(
	ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionOperation,
	action hierarchydeletion.HierarchyDeletionAction,
	now time.Time,
) (hierarchydeletion.HierarchyDeletionChildEntry, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return hierarchydeletion.HierarchyDeletionChildEntry{}, err
	}
	if recordcodec.ValidateTimestamp("hierarchy deletion child dispatch", now) != nil ||
		action.ProcedureKind != hierarchydeletion.HierarchyDeletionProcedureAgent || action.AgentProcedure == nil {
		return hierarchydeletion.HierarchyDeletionChildEntry{}, errs.New(
			errs.KindValidationFailed,
			"hierarchy deletion child dispatch is invalid",
		)
	}
	current, err := repository.OperationByTask(ctx, operation.Tombstone.CurrentTaskID)
	if err != nil {
		return hierarchydeletion.HierarchyDeletionChildEntry{}, err
	}
	if err := hierarchydeletionexecution.ValidateHierarchyDeletionActiveAction(current, action); err != nil {
		return hierarchydeletion.HierarchyDeletionChildEntry{}, err
	}
	childKey, _ := hierarchydeletion.HierarchyDeletionChildKey(
		current.Tombstone.OperationID,
		action.AgentProcedure.ChildOperationID,
	)
	stored, err := repository.store.Get(ctx, childKey)
	if err != nil {
		return hierarchydeletion.HierarchyDeletionChildEntry{}, err
	}
	if stored.Entry != nil {
		entry, decodeErr := hierarchydeletionexecution.DecodeHierarchyDeletionChildEntry(stored.Entry.Value)
		if decodeErr != nil || !hierarchydeletionexecution.HierarchyDeletionChildMatches(entry, current, action) {
			clear(stored.Entry.Value)
			return hierarchydeletion.HierarchyDeletionChildEntry{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		clear(stored.Entry.Value)
		if entry.DispatchState != hierarchydeletion.HierarchyDeletionChildTerminal ||
			(entry.Terminal != nil && *entry.Terminal == hierarchydeletion.HierarchyDeletionAgentCompleted) {
			return entry, nil
		}
		return repository.publishHierarchyDeletionChildAttempt(
			ctx,
			current,
			action,
			&entry,
			stored.Entry.ModRevision,
			now,
		)
	}
	return repository.publishHierarchyDeletionChildAttempt(ctx, current, action, nil, 0, now)
}

func (repository *HierarchyDeletionRepository) publishHierarchyDeletionChildAttempt(
	ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionOperation,
	action hierarchydeletion.HierarchyDeletionAction,
	previous *hierarchydeletion.HierarchyDeletionChildEntry,
	previousRevision int64,
	now time.Time,
) (_ hierarchydeletion.HierarchyDeletionChildEntry, returnErr error) {
	generation := int64(1)
	retryOf := ""
	if previous != nil {
		previousTaskRead, getErr := repository.store.Get(ctx, taskjournal.TaskStorageKey(previous.CurrentTaskID))
		if getErr != nil {
			return hierarchydeletion.HierarchyDeletionChildEntry{}, getErr
		}
		if previousTaskRead.Entry == nil {
			return hierarchydeletion.HierarchyDeletionChildEntry{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		previousTask, decodeErr := DecodeTaskRecord(previousTaskRead.Entry.Value)
		clear(previousTaskRead.Entry.Value)
		if decodeErr != nil || previousTask.ID != previous.CurrentTaskID ||
			previousTask.OperationID != previous.ChildOperationID {
			return hierarchydeletion.HierarchyDeletionChildEntry{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		previousGeneration, parseErr := strconv.ParseInt(
			previousTask.Params[taskjournal.TaskHierarchyDeletionGenerationParam], 10, 64,
		)
		if parseErr != nil || previousGeneration <= 0 {
			return hierarchydeletion.HierarchyDeletionChildEntry{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		generation = previousGeneration + 1
		retryOf = previous.CurrentTaskID
	}
	attemptID := hierarchydeletionexecution.HierarchyDeletionStableRawID(
		"attempt",
		action.ParentOperationID,
		action.AgentProcedure.ChildOperationID,
		strconv.FormatInt(generation, 10),
	)
	taskID := hierarchydeletionexecution.HierarchyDeletionChildStableID(
		ids.KindTask,
		action.AgentProcedure.ChildOperationID,
		attemptID,
	)
	planID := hierarchydeletionexecution.HierarchyDeletionChildStableID(
		ids.KindPlan,
		action.AgentProcedure.ChildOperationID,
		attemptID,
	)
	stepID := hierarchydeletionexecution.HierarchyDeletionChildStableID(
		ids.KindStep,
		action.AgentProcedure.ChildOperationID,
		attemptID,
	)
	planHash := action.AgentProcedure.InputDigest
	taskSteps := []taskjournal.TaskStepRecord{{Kind: taskjournal.TaskStepOperation, ID: stepID}}
	if action.AgentProcedure.TypedProcedure == "environment.cleanup" {
		environmentResult, getErr := repository.store.Get(ctx, hierarchyrecord.EnvironmentKey(action.TargetID))
		if getErr != nil {
			return hierarchydeletion.HierarchyDeletionChildEntry{}, getErr
		}
		if environmentResult.Entry == nil {
			return hierarchydeletion.HierarchyDeletionChildEntry{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		environment, decodeErr := hierarchyrecord.DecodeEnvironment(environmentResult.Entry.Value)
		clear(environmentResult.Entry.Value)
		if decodeErr != nil || environment.ID != action.TargetID {
			return hierarchydeletion.HierarchyDeletionChildEntry{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		hierarchy, hierarchyErr := newHierarchyRepository(repository.store)
		if hierarchyErr != nil {
			return hierarchydeletion.HierarchyDeletionChildEntry{}, hierarchyErr
		}
		projection, found, projectionErr := hierarchy.GetEnvironmentComposeProjection(ctx, environment.ID)
		if projectionErr != nil {
			return hierarchydeletion.HierarchyDeletionChildEntry{}, projectionErr
		}
		composeStepID := ""
		directoryStepID := stepID
		var composeArtifact []byte
		if found {
			composeStepID = stepID
			directoryStepID = hierarchydeletionexecution.HierarchyDeletionChildStableID(
				ids.KindStep, action.AgentProcedure.ChildOperationID, attemptID, "directory",
			)
			taskSteps = []taskjournal.TaskStepRecord{
				{Kind: taskjournal.TaskStepOperation, ID: composeStepID},
				{Kind: taskjournal.TaskStepOperation, ID: directoryStepID},
			}
			composeArtifact = projection.Record.ComposeArtifact
		}
		plan, planErr := hierarchyplan.EnvironmentCleanup(
			planID,
			1,
			environment.ID,
			composeStepID,
			directoryStepID,
			uint32(action.AgentProcedure.TimeoutSeconds),
			environment.VolumeDir,
			composeArtifact,
		)
		if planErr != nil {
			return hierarchydeletion.HierarchyDeletionChildEntry{}, planErr
		}
		planHash = hex.EncodeToString(plan.PlanHash)
		taskSteps = taskjournal.CaptureStepDescriptions(taskSteps, plan.Steps)
	}
	parentTask, err := repository.store.Get(ctx, taskjournal.TaskStorageKey(operation.Tombstone.CurrentTaskID))
	if err != nil {
		return hierarchydeletion.HierarchyDeletionChildEntry{}, err
	}
	if parentTask.Entry == nil || parentTask.Entry.ModRevision <= 0 {
		return hierarchydeletion.HierarchyDeletionChildEntry{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion parent Task changed",
		)
	}
	defer clear(parentTask.Entry.Value)
	parent, err := DecodeTaskRecord(parentTask.Entry.Value)
	if err != nil || parent.ID != operation.Tombstone.CurrentTaskID ||
		parent.Executor != taskjournal.TaskExecutorController ||
		parent.Status != taskjournal.TaskStatusRunning ||
		parent.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceHierarchyDeletion ||
		parent.Params[taskjournal.TaskHierarchyDeletionOperationParam] != operation.Tombstone.OperationID {
		return hierarchydeletion.HierarchyDeletionChildEntry{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion parent Task is not running",
		)
	}
	task := TaskRecord{
		ID: taskID, OperationID: action.AgentProcedure.ChildOperationID, RetryOf: retryOf,
		Owner: parent.Owner, Actor: taskjournal.TaskActorSystem, Executor: taskjournal.TaskExecutorAgent,
		PlanID: planID, PlanHash: planHash, RenderGeneration: 1,
		Type: action.AgentProcedure.TaskType, Target: action.TargetID,
		Params: map[string]string{
			taskjournal.TaskResourceKindParam:                taskjournal.TaskResourceHierarchyDeletion,
			taskjournal.TaskHierarchyDeletionParentParam:     operation.Tombstone.OperationID,
			taskjournal.TaskHierarchyDeletionChildParam:      action.AgentProcedure.ChildOperationID,
			taskjournal.TaskHierarchyDeletionAttemptParam:    attemptID,
			taskjournal.TaskHierarchyDeletionGenerationParam: strconv.FormatInt(generation, 10),
			taskjournal.TaskHierarchyDeletionOrdinalParam:    strconv.FormatInt(action.Ordinal, 10),
			taskjournal.TaskHierarchyDeletionActionKindParam: string(action.ActionKind),
			taskjournal.TaskHierarchyDeletionProcedureParam:  action.AgentProcedure.TypedProcedure,
			taskjournal.TaskHierarchyDeletionInputParam:      action.AgentProcedure.InputDigest,
		},
		Steps: taskSteps, TimeoutSeconds: action.AgentProcedure.TimeoutSeconds,
		Status: taskjournal.TaskStatusPending, NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	var attachPublication hierarchyAttachPublication
	if action.ActionKind == hierarchydeletion.HierarchyDeletionAttachDeprovision {
		attachPublication, err = repository.prepareHierarchyAttachPublication(ctx, operation, action, task, previous)
		if err != nil {
			return hierarchydeletion.HierarchyDeletionChildEntry{}, err
		}
		defer attachPublication.clear()
		defer func() { returnErr = attachPublication.hook.finish(ctx, repository.store, returnErr) }()
		task = attachPublication.task
	}
	if err := ValidateTaskRecord(task); err != nil {
		return hierarchydeletion.HierarchyDeletionChildEntry{}, err
	}
	taskValue, err := EncodeTaskRecord(task)
	if err != nil {
		return hierarchydeletion.HierarchyDeletionChildEntry{}, err
	}
	defer clear(taskValue)
	taskReference, err := idempotencyrecord.EncodeTaskReference(task.ID)
	if err != nil {
		return hierarchydeletion.HierarchyDeletionChildEntry{}, err
	}
	defer clear(taskReference)
	checkpointDigest := hierarchydeletion.HierarchyDeletionFoldDigest(
		"gp-deletion-child-checkpoint-v1", operation.Tombstone.OperationID,
		action.AgentProcedure.ChildOperationID, attemptID, taskID, strconv.FormatInt(action.Ordinal, 10),
	)
	entry := hierarchydeletion.HierarchyDeletionChildEntry{
		Schema: 1, ParentOperationID: operation.Tombstone.OperationID,
		ChildOperationID: action.AgentProcedure.ChildOperationID,
		CurrentAttemptID: attemptID, CurrentTaskID: taskID,
		CurrentTaskIdentityDigest: hierarchydeletion.HierarchyDeletionBytesDigest(taskValue),
		DispatchState:             hierarchydeletion.HierarchyDeletionChildDispatchVisible,
		CheckpointDigest:          checkpointDigest, RetryInputDigest: action.AgentProcedure.InputDigest,
		RetrySharedOwner: hierarchydeletion.HierarchyDeletionRetrySharedOwner{
			Kind:      "attempt-generation",
			AttemptID: strconv.FormatInt(generation, 10),
		},
	}
	entryValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(
		entry,
		hierarchydeletion.HierarchyDeletionLargeRecordBytes,
	)
	if err != nil {
		return hierarchydeletion.HierarchyDeletionChildEntry{}, err
	}
	defer clear(entryValue)
	successor := hierarchydeletion.HierarchyDeletionSuccessor{
		Schema: 1, ParentOperationID: operation.Tombstone.OperationID,
		ChildOperationID: action.AgentProcedure.ChildOperationID, ActionOrdinal: action.Ordinal,
		AttemptID: attemptID, TaskID: taskID,
	}
	successorValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(
		successor,
		hierarchydeletion.HierarchyDeletionSmallRecordBytes,
	)
	if err != nil {
		return hierarchydeletion.HierarchyDeletionChildEntry{}, err
	}
	defer clear(successorValue)
	childKey, _ := hierarchydeletion.HierarchyDeletionChildKey(
		operation.Tombstone.OperationID,
		action.AgentProcedure.ChildOperationID,
	)
	successorKey, _ := hierarchydeletion.HierarchyDeletionSuccessorKey(
		operation.Tombstone.OperationID,
		action.AgentProcedure.ChildOperationID,
		attemptID,
	)
	nextTombstone := operation.Tombstone
	nextTombstone.Checkpoint.ActiveChildOperationID = action.AgentProcedure.ChildOperationID
	nextTombstone.Checkpoint.ActiveChildAttemptID = attemptID
	nextFence := operation.Fence
	nextFence.Generation++
	nextFence.ActiveChildOperationID = action.AgentProcedure.ChildOperationID
	nextFence.ActiveChildAttemptID = attemptID
	nextFence.Dispatch = hierarchydeletion.HierarchyDeletionDispatchOpen
	nextFence.UpdatedAt = now
	tombstoneValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(
		nextTombstone,
		hierarchydeletion.HierarchyDeletionLargeRecordBytes,
	)
	if err != nil {
		return hierarchydeletion.HierarchyDeletionChildEntry{}, err
	}
	defer clear(tombstoneValue)
	fenceValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(
		nextFence,
		hierarchydeletion.HierarchyDeletionSmallRecordBytes,
	)
	if err != nil {
		return hierarchydeletion.HierarchyDeletionChildEntry{}, err
	}
	defer clear(fenceValue)
	conditions := []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(task.ID)}, {Key: taskjournal.TaskOperationIndexKey(task.OperationID, task.ID)},
		{
			Key: taskjournal.TaskActiveOperationKey(task.OperationID),
		}, {Key: taskjournal.TaskQueueKey(task.Executor, task.ID)},
		{Key: childKey, ModRevision: previousRevision}, {Key: successorKey},
		{Key: taskjournal.TaskStorageKey(parent.ID), ModRevision: parentTask.Entry.ModRevision},
		{
			Key: hierarchydeletion.HierarchyDeletionTombstoneKey(
				string(operation.Tombstone.TargetKind),
				operation.Tombstone.TargetID,
			),
			ModRevision: operation.TombstoneRevision,
		},
	}
	fenceKey, _ := hierarchydeletion.HierarchyDeletionCleanupFenceKey(operation.Tombstone.OperationID)
	conditions = append(conditions, etcdstore.Condition{Key: fenceKey, ModRevision: operation.FenceRevision})
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(task.ID), Value: taskValue},
		{
			Type:  etcdstore.MutationPut,
			Key:   taskjournal.TaskOperationIndexKey(task.OperationID, task.ID),
			Value: taskReference,
		},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskActiveOperationKey(task.OperationID), Value: taskReference},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskQueueKey(task.Executor, task.ID), Value: taskReference},
		{Type: etcdstore.MutationPut, Key: childKey, Value: entryValue},
		{Type: etcdstore.MutationPut, Key: successorKey, Value: successorValue},
		{
			Type: etcdstore.MutationPut,
			Key: hierarchydeletion.HierarchyDeletionTombstoneKey(
				string(operation.Tombstone.TargetKind),
				operation.Tombstone.TargetID,
			),
			Value: tombstoneValue,
		},
		{Type: etcdstore.MutationPut, Key: fenceKey, Value: fenceValue},
	}
	conditions = append(conditions, attachPublication.conditions...)
	mutations = append(mutations, attachPublication.mutations...)
	ownerKeys, err := taskOwnerIndexKeys(task.Owner, task.ID)
	if err != nil {
		return hierarchydeletion.HierarchyDeletionChildEntry{}, err
	}
	for _, key := range ownerKeys {
		conditions = append(conditions, etcdstore.Condition{Key: key})
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: key, Value: []byte(task.ID)})
	}
	if err := hierarchydeletion.EnforceHierarchyDeletionTransaction(conditions, mutations); err != nil {
		return hierarchydeletion.HierarchyDeletionChildEntry{}, err
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return hierarchydeletion.HierarchyDeletionChildEntry{}, err
	}
	etcdstore.ClearValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return hierarchydeletion.HierarchyDeletionChildEntry{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion child dispatch changed",
		)
	}
	return entry, nil
}
