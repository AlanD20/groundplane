package etcd

import (
	"context"
	"encoding/hex"
	"encoding/json"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"strconv"
	"time"

	"github.com/AlanD20/groundplane/internal/common/hierarchyplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyDeletionRepository) PublishOrResumeAgentAction(
	ctx context.Context,
	operation HierarchyDeletionOperation,
	action HierarchyDeletionAction,
	now time.Time,
) (HierarchyDeletionChildEntry, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return HierarchyDeletionChildEntry{}, err
	}
	if recordcodec.ValidateTimestamp("hierarchy deletion child dispatch", now) != nil ||
		action.ProcedureKind != HierarchyDeletionProcedureAgent || action.AgentProcedure == nil {
		return HierarchyDeletionChildEntry{}, errs.New(
			errs.KindValidationFailed,
			"hierarchy deletion child dispatch is invalid",
		)
	}
	current, err := repository.OperationByTask(ctx, operation.Tombstone.CurrentTaskID)
	if err != nil {
		return HierarchyDeletionChildEntry{}, err
	}
	if err := validateHierarchyDeletionActiveAction(current, action); err != nil {
		return HierarchyDeletionChildEntry{}, err
	}
	childKey, _ := HierarchyDeletionChildKey(current.Tombstone.OperationID, action.AgentProcedure.ChildOperationID)
	stored, err := repository.store.Get(ctx, childKey)
	if err != nil {
		return HierarchyDeletionChildEntry{}, err
	}
	if stored.Entry != nil {
		entry, decodeErr := decodeHierarchyDeletionChildEntry(stored.Entry.Value)
		if decodeErr != nil || !hierarchyDeletionChildMatches(entry, current, action) {
			clear(stored.Entry.Value)
			return HierarchyDeletionChildEntry{}, corruptHierarchyDeletion()
		}
		clear(stored.Entry.Value)
		if entry.DispatchState != HierarchyDeletionChildTerminal ||
			(entry.Terminal != nil && *entry.Terminal == HierarchyDeletionAgentCompleted) {
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
	operation HierarchyDeletionOperation,
	action HierarchyDeletionAction,
	previous *HierarchyDeletionChildEntry,
	previousRevision int64,
	now time.Time,
) (HierarchyDeletionChildEntry, error) {
	generation := int64(1)
	retryOf := ""
	if previous != nil {
		previousTaskRead, getErr := repository.store.Get(ctx, taskKey(previous.CurrentTaskID))
		if getErr != nil {
			return HierarchyDeletionChildEntry{}, getErr
		}
		if previousTaskRead.Entry == nil {
			return HierarchyDeletionChildEntry{}, corruptHierarchyDeletion()
		}
		previousTask, decodeErr := decodeTaskRecord(previousTaskRead.Entry.Value)
		clear(previousTaskRead.Entry.Value)
		if decodeErr != nil || previousTask.ID != previous.CurrentTaskID ||
			previousTask.OperationID != previous.ChildOperationID {
			return HierarchyDeletionChildEntry{}, corruptHierarchyDeletion()
		}
		previousGeneration, parseErr := strconv.ParseInt(
			previousTask.Params[TaskHierarchyDeletionGenerationParam], 10, 64,
		)
		if parseErr != nil || previousGeneration <= 0 {
			return HierarchyDeletionChildEntry{}, corruptHierarchyDeletion()
		}
		generation = previousGeneration + 1
		retryOf = previous.CurrentTaskID
	}
	attemptID := hierarchyDeletionStableRawID(
		"attempt",
		action.ParentOperationID,
		action.AgentProcedure.ChildOperationID,
		strconv.FormatInt(generation, 10),
	)
	taskID := hierarchyDeletionChildStableID(ids.KindTask, action.AgentProcedure.ChildOperationID, attemptID)
	planID := hierarchyDeletionChildStableID(ids.KindPlan, action.AgentProcedure.ChildOperationID, attemptID)
	stepID := hierarchyDeletionChildStableID(ids.KindStep, action.AgentProcedure.ChildOperationID, attemptID)
	planHash := action.AgentProcedure.InputDigest
	taskSteps := []TaskStepRecord{{Kind: TaskStepOperation, ID: stepID}}
	if action.AgentProcedure.TypedProcedure == "environment.cleanup" {
		environmentResult, getErr := repository.store.Get(ctx, hierarchyrecord.EnvironmentKey(action.TargetID))
		if getErr != nil {
			return HierarchyDeletionChildEntry{}, getErr
		}
		if environmentResult.Entry == nil {
			return HierarchyDeletionChildEntry{}, corruptHierarchyDeletion()
		}
		environment, decodeErr := hierarchyrecord.DecodeEnvironment(environmentResult.Entry.Value)
		clear(environmentResult.Entry.Value)
		if decodeErr != nil || environment.ID != action.TargetID {
			return HierarchyDeletionChildEntry{}, corruptHierarchyDeletion()
		}
		hierarchy, hierarchyErr := newHierarchyRepository(repository.store)
		if hierarchyErr != nil {
			return HierarchyDeletionChildEntry{}, hierarchyErr
		}
		projection, found, projectionErr := hierarchy.GetEnvironmentComposeProjection(ctx, environment.ID)
		if projectionErr != nil {
			return HierarchyDeletionChildEntry{}, projectionErr
		}
		composeStepID := ""
		directoryStepID := stepID
		var composeArtifact []byte
		if found {
			composeStepID = stepID
			directoryStepID = hierarchyDeletionChildStableID(
				ids.KindStep, action.AgentProcedure.ChildOperationID, attemptID, "directory",
			)
			taskSteps = []TaskStepRecord{
				{Kind: TaskStepOperation, ID: composeStepID},
				{Kind: TaskStepOperation, ID: directoryStepID},
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
			return HierarchyDeletionChildEntry{}, planErr
		}
		planHash = hex.EncodeToString(plan.PlanHash)
	}
	parentTask, err := repository.store.Get(ctx, taskKey(operation.Tombstone.CurrentTaskID))
	if err != nil {
		return HierarchyDeletionChildEntry{}, err
	}
	if parentTask.Entry == nil || parentTask.Entry.ModRevision <= 0 {
		return HierarchyDeletionChildEntry{}, errs.New(errs.KindStateConflict, "hierarchy deletion parent Task changed")
	}
	defer clear(parentTask.Entry.Value)
	parent, err := decodeTaskRecord(parentTask.Entry.Value)
	if err != nil || parent.ID != operation.Tombstone.CurrentTaskID || parent.Executor != TaskExecutorController ||
		parent.Status != TaskStatusRunning || parent.Params[TaskResourceKindParam] != TaskResourceHierarchyDeletion ||
		parent.Params[TaskHierarchyDeletionOperationParam] != operation.Tombstone.OperationID {
		return HierarchyDeletionChildEntry{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion parent Task is not running",
		)
	}
	task := TaskRecord{
		ID: taskID, OperationID: action.AgentProcedure.ChildOperationID, RetryOf: retryOf,
		Owner: parent.Owner, Actor: TaskActorSystem, Executor: TaskExecutorAgent,
		PlanID: planID, PlanHash: planHash, RenderGeneration: 1,
		Type: action.AgentProcedure.TaskType, Target: action.TargetID,
		Params: map[string]string{
			TaskResourceKindParam:                TaskResourceHierarchyDeletion,
			TaskHierarchyDeletionParentParam:     operation.Tombstone.OperationID,
			TaskHierarchyDeletionChildParam:      action.AgentProcedure.ChildOperationID,
			TaskHierarchyDeletionAttemptParam:    attemptID,
			TaskHierarchyDeletionGenerationParam: strconv.FormatInt(generation, 10),
			TaskHierarchyDeletionOrdinalParam:    strconv.FormatInt(action.Ordinal, 10),
			TaskHierarchyDeletionActionKindParam: string(action.ActionKind),
			TaskHierarchyDeletionProcedureParam:  action.AgentProcedure.TypedProcedure,
			TaskHierarchyDeletionInputParam:      action.AgentProcedure.InputDigest,
		},
		Steps: taskSteps, TimeoutSeconds: action.AgentProcedure.TimeoutSeconds,
		Status: TaskStatusPending, NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	if err := validateTaskRecord(task); err != nil {
		return HierarchyDeletionChildEntry{}, err
	}
	taskValue, err := encodeTaskRecord(task)
	if err != nil {
		return HierarchyDeletionChildEntry{}, err
	}
	defer clear(taskValue)
	taskReference, err := idempotencyrecord.EncodeTaskReference(task.ID)
	if err != nil {
		return HierarchyDeletionChildEntry{}, err
	}
	defer clear(taskReference)
	checkpointDigest := hierarchyDeletionFoldDigest(
		"gp-deletion-child-checkpoint-v1", operation.Tombstone.OperationID,
		action.AgentProcedure.ChildOperationID, attemptID, taskID, strconv.FormatInt(action.Ordinal, 10),
	)
	entry := HierarchyDeletionChildEntry{
		Schema: 1, ParentOperationID: operation.Tombstone.OperationID,
		ChildOperationID: action.AgentProcedure.ChildOperationID,
		CurrentAttemptID: attemptID, CurrentTaskID: taskID,
		CurrentTaskIdentityDigest: hierarchyDeletionBytesDigest(taskValue),
		DispatchState:             HierarchyDeletionChildDispatchVisible,
		CheckpointDigest:          checkpointDigest, RetryInputDigest: action.AgentProcedure.InputDigest,
		RetrySharedOwner: HierarchyDeletionRetrySharedOwner{
			Kind:      "attempt-generation",
			AttemptID: strconv.FormatInt(generation, 10),
		},
	}
	entryValue, err := encodeHierarchyDeletionRecord(entry, hierarchyDeletionLargeRecordBytes)
	if err != nil {
		return HierarchyDeletionChildEntry{}, err
	}
	defer clear(entryValue)
	successor := HierarchyDeletionSuccessor{
		Schema: 1, ParentOperationID: operation.Tombstone.OperationID,
		ChildOperationID: action.AgentProcedure.ChildOperationID, ActionOrdinal: action.Ordinal,
		AttemptID: attemptID, TaskID: taskID,
	}
	successorValue, err := encodeHierarchyDeletionRecord(successor, hierarchyDeletionSmallRecordBytes)
	if err != nil {
		return HierarchyDeletionChildEntry{}, err
	}
	defer clear(successorValue)
	childKey, _ := HierarchyDeletionChildKey(operation.Tombstone.OperationID, action.AgentProcedure.ChildOperationID)
	successorKey, _ := HierarchyDeletionSuccessorKey(
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
	nextFence.Dispatch = HierarchyDeletionDispatchOpen
	nextFence.UpdatedAt = now
	tombstoneValue, err := encodeHierarchyDeletionRecord(nextTombstone, hierarchyDeletionLargeRecordBytes)
	if err != nil {
		return HierarchyDeletionChildEntry{}, err
	}
	defer clear(tombstoneValue)
	fenceValue, err := encodeHierarchyDeletionRecord(nextFence, hierarchyDeletionSmallRecordBytes)
	if err != nil {
		return HierarchyDeletionChildEntry{}, err
	}
	defer clear(fenceValue)
	conditions := []etcdstore.Condition{
		{Key: taskKey(task.ID)}, {Key: taskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskActiveOperationKey(task.OperationID)}, {Key: taskQueueKey(task.Executor, task.ID)},
		{Key: childKey, ModRevision: previousRevision}, {Key: successorKey},
		{Key: taskKey(parent.ID), ModRevision: parentTask.Entry.ModRevision},
		{
			Key: HierarchyDeletionTombstoneKey(
				string(operation.Tombstone.TargetKind),
				operation.Tombstone.TargetID,
			),
			ModRevision: operation.TombstoneRevision,
		},
	}
	fenceKey, _ := HierarchyDeletionCleanupFenceKey(operation.Tombstone.OperationID)
	conditions = append(conditions, etcdstore.Condition{Key: fenceKey, ModRevision: operation.FenceRevision})
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{Type: etcdstore.MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: taskReference},
		{Type: etcdstore.MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: taskReference},
		{Type: etcdstore.MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: taskReference},
		{Type: etcdstore.MutationPut, Key: childKey, Value: entryValue},
		{Type: etcdstore.MutationPut, Key: successorKey, Value: successorValue},
		{
			Type:  etcdstore.MutationPut,
			Key:   HierarchyDeletionTombstoneKey(string(operation.Tombstone.TargetKind), operation.Tombstone.TargetID),
			Value: tombstoneValue,
		},
		{Type: etcdstore.MutationPut, Key: fenceKey, Value: fenceValue},
	}
	ownerKeys, err := taskOwnerIndexKeys(task.Owner, task.ID)
	if err != nil {
		return HierarchyDeletionChildEntry{}, err
	}
	for _, key := range ownerKeys {
		conditions = append(conditions, etcdstore.Condition{Key: key})
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: key, Value: []byte(task.ID)})
	}
	if err := enforceHierarchyDeletionTransaction(conditions, mutations); err != nil {
		return HierarchyDeletionChildEntry{}, err
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return HierarchyDeletionChildEntry{}, err
	}
	clearKeyValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return HierarchyDeletionChildEntry{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion child dispatch changed",
		)
	}
	return entry, nil
}

func hierarchyDeletionChildMatches(
	entry HierarchyDeletionChildEntry,
	operation HierarchyDeletionOperation,
	action HierarchyDeletionAction,
) bool {
	return entry.Schema == 1 && entry.ParentOperationID == operation.Tombstone.OperationID &&
		entry.ChildOperationID == action.AgentProcedure.ChildOperationID &&
		entry.RetryInputDigest == action.AgentProcedure.InputDigest && entry.CheckpointDigest != "" &&
		entry.CurrentAttemptID != "" && entry.CurrentTaskID != ""
}

func decodeHierarchyDeletionChildEntry(value []byte) (HierarchyDeletionChildEntry, error) {
	var entry HierarchyDeletionChildEntry
	if decodeHierarchyDeletionRecord(value, hierarchyDeletionLargeRecordBytes, &entry) != nil || entry.Schema != 1 {
		return HierarchyDeletionChildEntry{}, corruptHierarchyDeletion()
	}
	return entry, nil
}

func hierarchyDeletionChildStableID(kind ids.Kind, values ...string) string {
	return string(
		kind,
	) + "_" + hierarchyDeletionStableULID(
		"gp-deletion-stable-id-v1",
		append([]string{string(kind)}, values...)...)
}

func hierarchyDeletionStableRawID(prefix string, values ...string) string {
	return prefix + "_" + hierarchyDeletionStableULID(
		"gp-deletion-stable-raw-id-v1",
		append([]string{prefix}, values...)...)
}

func hierarchyDeletionStableULID(domain string, values ...string) string {
	operation := hierarchyDeletionStableOperationID(append([]string{domain}, values...)...)
	return operation[len(string(ids.KindOperation))+1:]
}

func hierarchyDeletionTaskResultDigest(result *TaskResultRecord, terminal TaskStatus) (string, string, error) {
	if result == nil {
		return "", "", errs.New(errs.KindInternal, "hierarchy deletion child Task lost its result")
	}
	value, err := json.Marshal(result)
	if err != nil {
		return "", "", errs.Wrap(errs.KindInternal, err)
	}
	defer clear(value)
	digest := hierarchyDeletionBytesDigest(value)
	if terminal == TaskStatusCompleted {
		return digest, "", nil
	}
	return "", hierarchyDeletionFoldDigest("gp-deletion-child-error-v1", string(terminal), digest), nil
}

func hierarchyDeletionAgentTerminalFromTask(status TaskStatus) (HierarchyDeletionAgentTerminal, error) {
	switch status {
	case TaskStatusCompleted:
		return HierarchyDeletionAgentCompleted, nil
	case TaskStatusFailed:
		return HierarchyDeletionAgentFailed, nil
	case TaskStatusAborted:
		return HierarchyDeletionAgentAborted, nil
	case TaskStatusTimedOut:
		return HierarchyDeletionAgentTimedOut, nil
	default:
		return "", errs.Newf(errs.KindStateConflict, "hierarchy deletion child Task is %s", status)
	}
}
