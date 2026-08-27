package etcd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// TaskAssignmentRecord is the durable claim binding one running Task to one
// Agent daemon generation. ClaimedTaskRevision is the pending Task revision
// consumed by the assignment transaction, not the transaction's new revision.
type TaskAssignmentRecord struct {
	AssignmentID        string
	TaskID              string
	Executor            TaskExecutor
	AgentID             string
	AgentGeneration     uint64
	ClaimedTaskRevision int64
	AssignedAt          time.Time
	Deadline            time.Time
}

// TaskAssignment contains both records created by one successful claim.
type TaskAssignment struct {
	Assignment Versioned[TaskAssignmentRecord]
	Task       Versioned[TaskRecord]
}

type taskAssignmentJSON struct {
	Schema              int          `json:"schema"`
	AssignmentID        string       `json:"assignment_id"`
	TaskID              string       `json:"task_id"`
	Executor            TaskExecutor `json:"executor"`
	AgentID             string       `json:"agent_id"`
	AgentGeneration     uint64       `json:"agent_generation"`
	ClaimedTaskRevision int64        `json:"claimed_task_revision"`
	AssignedAt          string       `json:"assigned_at"`
	Deadline            string       `json:"deadline"`
}

func encodeTaskAssignment(record TaskAssignmentRecord) ([]byte, error) {
	if err := validateTaskAssignment(record); err != nil {
		return nil, err
	}
	return json.Marshal(taskAssignmentJSON{
		Schema: 1, AssignmentID: record.AssignmentID, TaskID: record.TaskID,
		Executor: record.Executor, AgentID: record.AgentID,
		AgentGeneration: record.AgentGeneration, ClaimedTaskRevision: record.ClaimedTaskRevision,
		AssignedAt: record.AssignedAt.Format(time.RFC3339Nano),
		Deadline:   record.Deadline.Format(time.RFC3339Nano),
	})
}

func decodeTaskAssignment(value []byte) (TaskAssignmentRecord, error) {
	if rejectDuplicateJSONFields(value) != nil {
		return TaskAssignmentRecord{}, corruptTaskAssignment()
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var data taskAssignmentJSON
	if err := decoder.Decode(&data); err != nil || requireJSONEOF(decoder) != nil || data.Schema != 1 {
		return TaskAssignmentRecord{}, corruptTaskAssignment()
	}
	assignedAt, err := parseCanonicalTimestamp(data.AssignedAt)
	if err != nil {
		return TaskAssignmentRecord{}, corruptTaskAssignment()
	}
	deadline, err := parseCanonicalTimestamp(data.Deadline)
	if err != nil {
		return TaskAssignmentRecord{}, corruptTaskAssignment()
	}
	record := TaskAssignmentRecord{
		AssignmentID: data.AssignmentID, TaskID: data.TaskID,
		Executor: data.Executor, AgentID: data.AgentID, AgentGeneration: data.AgentGeneration,
		ClaimedTaskRevision: data.ClaimedTaskRevision, AssignedAt: assignedAt, Deadline: deadline,
	}
	if err := validateTaskAssignment(record); err != nil {
		return TaskAssignmentRecord{}, corruptTaskAssignment()
	}
	return record, nil
}

func validateTaskAssignment(record TaskAssignmentRecord) error {
	if validateStableID(ids.KindAssignment, record.AssignmentID) != nil ||
		validateStableID(ids.KindTask, record.TaskID) != nil || !validTaskExecutor(record.Executor) ||
		record.ClaimedTaskRevision <= 0 ||
		validateTimestamp("task assignment assigned_at", record.AssignedAt) != nil ||
		validateTimestamp("task assignment deadline", record.Deadline) != nil ||
		!record.Deadline.After(record.AssignedAt) {
		return corruptTaskAssignment()
	}
	if record.Executor == TaskExecutorAgent &&
		(validateStableID(ids.KindAgent, record.AgentID) != nil || record.AgentGeneration == 0) {
		return corruptTaskAssignment()
	}
	if record.Executor == TaskExecutorController && (record.AgentID != "" || record.AgentGeneration != 0) {
		return corruptTaskAssignment()
	}
	return nil
}

func corruptTaskAssignment() error {
	return errs.New(errs.KindInternal, "task assignment record is corrupt")
}

// CreateTask atomically claims idempotency and publishes the Task, immutable
// history index, active-operation index, and FIFO queue membership.
func (repository *TaskRepository) CreateTask(
	ctx context.Context,
	record TaskRecord,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if marker.Kind != IdempotencyMarkerTask || marker.State != IdempotencyMarkerPending ||
		marker.TaskID != record.ID || !marker.CreatedAt.Equal(record.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) || record.Status != TaskStatusPending {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"task creation marker does not match its Task",
		)
	}
	initiation, err := newPlatformTaskInitiation(TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateTaskInitiation(record, initiation, true); err != nil {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"generic task creation accepts only platform operator tasks",
		)
	}
	record = cloneTaskRecord(record)
	if record.IdempotencyKey == "" {
		record.IdempotencyKey = marker.Locator.Key
	}
	record.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	if err := validateTaskRecord(record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}

	taskValue, err := encodeTaskRecord(record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	reference, err := encodeTaskReference(record.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(reference)
	conditions := []Condition{
		{Key: taskKey(record.ID)},
		{Key: taskOperationIndexKey(record.OperationID, record.ID)},
		{Key: taskActiveOperationKey(record.OperationID)},
		{Key: taskQueueKey(record.Executor, record.ID)},
	}
	mutations := []Mutation{
		{Type: MutationPut, Key: taskKey(record.ID), Value: taskValue},
		{Type: MutationPut, Key: taskOperationIndexKey(record.OperationID, record.ID), Value: reference},
		{Type: MutationPut, Key: taskActiveOperationKey(record.OperationID), Value: reference},
		{Type: MutationPut, Key: taskQueueKey(record.Executor, record.ID), Value: reference},
	}
	plan, err := newTaskIdempotencyMutationPlan(
		record,
		initiation,
		conditions,
		mutations,
		classifyTaskCreateConflict(record.OperationID),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func classifyTaskCreateConflict(operationID string) idempotencyPlanClassifier {
	return func(_ int64, values []*KeyValue) error {
		if len(values) != 4 {
			return errs.New(errs.KindInternal, "task creation compare evidence is incomplete")
		}
		if values[2] != nil {
			activeTaskID, err := decodeTaskReference(values[2].Value)
			if err != nil {
				return err
			}
			return errs.Newf(
				errs.KindStateConflict,
				"operation %s already has active task %s",
				operationID,
				activeTaskID,
			)
		}
		for index, value := range values {
			if index != 2 && value != nil {
				return errs.New(errs.KindInternal, "task creation collided with durable Task state")
			}
		}
		return errs.New(errs.KindInternal, "task creation compare failure was not classified")
	}
}

// RetryTask atomically clones one retryable terminal attempt and publishes the
// new Task, immutable history index, active-operation index, FIFO queue
// membership, and the retry request's protected idempotency marker. The Task's
// operation idempotency key remains the original operation key; its private
// marker locator identifies this retry request for exact HTTP replay.
func (repository *TaskRepository) RetryTask(
	ctx context.Context,
	sourceTaskID string,
	retryTaskID string,
	actor TaskActor,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if actor != TaskActorOperator {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"ordinary task retry actor must be operator",
		)
	}
	return repository.retryTask(ctx, sourceTaskID, retryTaskID, TaskActorOperator, nil, marker)
}

func (repository *TaskRepository) RetryTaskWithInitiation(
	ctx context.Context,
	sourceTaskID string,
	retryTaskID string,
	initiation TaskInitiation,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	return repository.retryTask(ctx, sourceTaskID, retryTaskID, initiation.actor, &initiation, marker)
}

func (repository *TaskRepository) retryTask(
	ctx context.Context,
	sourceTaskID string,
	retryTaskID string,
	actor TaskActor,
	provided *TaskInitiation,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if validateStableID(ids.KindTask, sourceTaskID) != nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "source Task id is invalid")
	}
	source, err := repository.GetTask(ctx, sourceTaskID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if source.Record.Type == TaskBackup || source.Record.Type == TaskBackupPrune {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindTaskNotRetryable,
			"backup retry requires its atomic domain retry protocol",
		)
	}
	if source.Record.Type == TaskRotate {
		return repository.retryBackupKeyRotationTask(ctx, source, retryTaskID, actor, marker)
	}
	if source.Record.Params[TaskResourceKindParam] == TaskResourceHierarchyDeletion {
		return repository.retryHierarchyDeletionTask(
			ctx, source, retryTaskID, actor, provided, marker,
		)
	}
	retry, err := cloneRetryTask(source.Record, retryTaskID, actor, marker.CreatedAt)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	initiation, err := newInheritedTaskInitiation(source, actor)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if provided != nil {
		if err := validateTaskInitiation(
			TaskRecord{},
			*provided,
			false,
		); err != nil ||
			provided.actor != TaskActorSystem {
			return IdempotencyTransactionResult{}, errs.New(
				errs.KindValidationFailed,
				"system task retry initiation is invalid",
			)
		}
		fences := append(append([]Condition(nil), initiation.fences...), provided.fences...)
		initiation, err = newTaskInitiation(source.Record.Owner, TaskActorSystem, fences...)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
	}
	if marker.Kind != IdempotencyMarkerTask || marker.State != IdempotencyMarkerPending ||
		marker.TaskID != retry.ID || !marker.CreatedAt.Equal(retry.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"task retry marker does not match its Task",
		)
	}
	if err := validateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	retry.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	if err := validateTaskRecord(retry); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if existing, found, err := existingIdempotencyTransaction(
		ctx,
		repository.store,
		marker,
	); err != nil || found {
		return existing, err
	}

	taskValue, err := encodeTaskRecord(retry)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	reference, err := encodeTaskReference(retry.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(reference)
	conditions := []Condition{
		{Key: taskKey(sourceTaskID), ModRevision: source.Revision},
		{Key: taskKey(retry.ID)},
		{Key: taskOperationIndexKey(retry.OperationID, retry.ID)},
		{Key: taskActiveOperationKey(retry.OperationID)},
		{Key: taskQueueKey(retry.Executor, retry.ID)},
	}
	mutations := []Mutation{
		{Type: MutationPut, Key: taskKey(retry.ID), Value: taskValue},
		{Type: MutationPut, Key: taskOperationIndexKey(retry.OperationID, retry.ID), Value: reference},
		{Type: MutationPut, Key: taskActiveOperationKey(retry.OperationID), Value: reference},
		{Type: MutationPut, Key: taskQueueKey(retry.Executor, retry.ID), Value: reference},
	}
	releaseChange, err := repository.prepareReleaseTaskRetry(ctx, source.Record, retry, source.ReadRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if releaseChange.applies {
		conditions = append(conditions, releaseChange.conditions...)
		mutations = append(mutations, releaseChange.mutations...)
	}
	defer releaseChange.clear()
	attachChange, err := repository.prepareAttachTaskRetry(ctx, source.Record, retry, source.ReadRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if attachChange.applies {
		conditions = append(conditions, attachChange.conditions...)
		mutations = append(mutations, attachChange.mutations...)
	}
	defer clearAttachTaskChange(attachChange)
	environmentChange, err := repository.prepareEnvironmentTaskRetry(
		ctx,
		source.Record,
		retry,
		source.ReadRevision,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if environmentChange.applies {
		conditions = append(conditions, environmentChange.conditions...)
		mutations = append(mutations, environmentChange.mutations...)
	}
	defer clearEnvironmentTaskChange(environmentChange)
	secretChange, err := repository.prepareSecretTaskRetry(ctx, source.Record, retry, source.ReadRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if secretChange.applies {
		conditions = append(conditions, secretChange.conditions...)
		mutations = append(mutations, secretChange.mutations...)
	}
	defer clearSecretTaskChange(secretChange)
	scriptChange, err := repository.prepareScriptTaskRetry(ctx, source.Record, retry, source.ReadRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if scriptChange.applies {
		conditions = append(conditions, scriptChange.conditions...)
		mutations = append(mutations, scriptChange.mutations...)
	}
	defer clearScriptTaskChange(scriptChange)
	releaseGroupChange, err := repository.prepareReleaseGroupTaskRetry(ctx, source.Record, retry, source.ReadRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if releaseGroupChange.applies {
		conditions = append(conditions, releaseGroupChange.conditions...)
		mutations = append(mutations, releaseGroupChange.mutations...)
	}
	defer clearReleaseGroupTaskChange(releaseGroupChange)
	routeChange, err := repository.prepareRemovalTaskRetry(ctx, source.Record, retry, source.ReadRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if routeChange.applies {
		conditions = append(conditions, routeChange.conditions...)
		mutations = append(mutations, routeChange.mutations...)
	}
	defer clearRouteTaskChange(routeChange)
	serviceChange, err := repository.prepareServiceTaskRetry(
		ctx, source.Record, retry, source.ReadRevision,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if serviceChange.applies {
		conditions = append(conditions, serviceChange.conditions...)
		mutations = append(mutations, serviceChange.mutations...)
	}
	defer clearServiceTaskChange(serviceChange)
	backingZoneChange, err := repository.prepareBackingZoneTaskRetry(ctx, source.Record, retry, source.ReadRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if backingZoneChange.applies {
		conditions = append(conditions, backingZoneChange.conditions...)
		mutations = append(mutations, backingZoneChange.mutations...)
	}
	defer clearBackingZoneTaskChange(backingZoneChange)
	componentChange, err := repository.prepareComponentTaskRetry(ctx, source.Record, retry, source.ReadRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if componentChange.applies {
		conditions = append(conditions, componentChange.conditions...)
		mutations = append(mutations, componentChange.mutations...)
	}
	defer clearComponentTaskChange(componentChange)
	connectorChange, err := repository.prepareConnectorTaskRetry(
		ctx, source.Record, retry, source.ReadRevision,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if connectorChange.applies {
		conditions = append(conditions, connectorChange.conditions...)
		mutations = append(mutations, connectorChange.mutations...)
	}
	defer clearConnectorTaskChange(connectorChange)
	runnerChange, err := repository.prepareRunnerTaskRetry(
		ctx, source.Record, retry, source.ReadRevision,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if runnerChange.applies {
		conditions = append(conditions, runnerChange.conditions...)
		mutations = append(mutations, runnerChange.mutations...)
	}
	defer clearRunnerTaskChange(runnerChange)
	retryClassifier := classifyTaskRetryConflict(
		sourceTaskID,
		retry.OperationID,
		source.Record.Target,
		attachChange.applies && source.Record.Type == TaskDetach,
		len(attachChange.conditions),
		len(environmentChange.conditions),
		len(secretChange.conditions),
		len(scriptChange.conditions),
		len(routeChange.conditions),
		len(serviceChange.conditions),
		len(backingZoneChange.conditions),
		len(componentChange.conditions),
		len(connectorChange.conditions),
		len(runnerChange.conditions),
		len(releaseChange.conditions),
	)
	environmentBinding, err := repository.bindOrdinaryTaskEnvironmentMutation(
		ctx,
		source.Record,
		source.ReadRevision,
		conditions,
		mutations,
		attachChange.applies,
		routeChange.applies && source.Record.Params[TaskEntryEnvironmentParam] != "",
		serviceChange.applies,
		connectorChange.applies,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if environmentBinding != nil {
		defer environmentBinding.clear()
		defer clear(environmentBinding.mutations[len(environmentBinding.mutations)-1].Value)
		conditions = environmentBinding.conditions
		mutations = environmentBinding.mutations
		baseClassifier := retryClassifier
		retryClassifier = func(revision int64, values []*KeyValue) error {
			return environmentBinding.classify(revision, values, baseClassifier)
		}
	}
	plan, err := newTaskIdempotencyMutationPlan(
		retry,
		initiation,
		conditions,
		mutations,
		retryClassifier,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func classifyTaskRetryConflict(
	sourceTaskID string,
	operationID string,
	attachTargetID string,
	attachDetach bool,
	attachConditions int,
	environmentConditions int,
	secretConditions int,
	scriptConditions int,
	routeConditions int,
	serviceConditions int,
	backingZoneConditions int,
	componentConditions int,
	connectorConditions int,
	runnerConditions int,
	releaseConditions int,
) idempotencyPlanClassifier {
	return func(_ int64, values []*KeyValue) error {
		expectedValues := 5 + attachConditions + environmentConditions + secretConditions +
			scriptConditions + routeConditions + serviceConditions + backingZoneConditions + componentConditions +
			connectorConditions + runnerConditions + releaseConditions
		if len(values) != expectedValues {
			return errs.New(errs.KindInternal, "task retry compare evidence is incomplete")
		}
		if values[0] == nil {
			return errs.Newf(errs.KindTaskNotFound, "task not found: %s", sourceTaskID)
		}
		if values[3] != nil {
			activeTaskID, err := decodeTaskReference(values[3].Value)
			if err != nil {
				return err
			}
			return errs.Newf(
				errs.KindTaskRetryInFlight,
				"operation %s already has active retry %s",
				operationID,
				activeTaskID,
			)
		}
		if values[1] != nil || values[2] != nil || values[4] != nil {
			return errs.New(errs.KindInternal, "task retry collided with durable Task state")
		}
		if attachDetach {
			if attachConditions < 2 {
				return errs.New(errs.KindInternal, "attach detach retry exclusion evidence is incomplete")
			}
			if exclusionErr := classifyAttachBackupSourceExclusionEvidence(
				values[6],
				attachTargetID,
			); exclusionErr != nil {
				return exclusionErr
			}
		}
		return errs.New(errs.KindStateConflict, "source Task changed while creating its retry")
	}
}

// ClaimNextTask claims the oldest queued Task by ascending ULID. Queue
// membership, pending Task state, active-operation membership, and the absent
// assignment are compared in the same transaction.
func (repository *TaskRepository) ClaimNextTask(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	assignedAt time.Time,
) (TaskAssignment, bool, error) {
	return repository.claimNextTask(ctx, TaskExecutorAgent, agentID, agentGeneration, assignedAt)
}

// ClaimNextControllerTask claims the oldest native Controller Task without
// manufacturing an Agent identity or assignment generation.
func (repository *TaskRepository) ClaimNextControllerTask(
	ctx context.Context,
	assignedAt time.Time,
) (TaskAssignment, bool, error) {
	return repository.claimNextTask(ctx, TaskExecutorController, "", 0, assignedAt)
}

func (repository *TaskRepository) claimNextTask(
	ctx context.Context,
	executor TaskExecutor,
	agentID string,
	agentGeneration uint64,
	assignedAt time.Time,
) (TaskAssignment, bool, error) {
	if err := validateContext(ctx); err != nil {
		return TaskAssignment{}, false, err
	}
	if !validTaskExecutor(executor) ||
		(executor == TaskExecutorAgent && (validateStableID(ids.KindAgent, agentID) != nil || agentGeneration == 0)) ||
		(executor == TaskExecutorController && (agentID != "" || agentGeneration != 0)) {
		return TaskAssignment{}, false, errs.New(errs.KindValidationFailed, "task execution claim identity is invalid")
	}
	if err := validateTimestamp("task assignment assigned_at", assignedAt); err != nil {
		return TaskAssignment{}, false, err
	}

	conflicts := 0
	for {
		candidate, found, err := repository.nextTaskClaimCandidate(ctx, executor)
		if err != nil || !found {
			return TaskAssignment{}, false, err
		}
		queued := candidate.queued
		taskValue := candidate.taskValue
		task := candidate.task
		claimAt, err := nextTaskControllerTimestamp(task.UpdatedAt, assignedAt)
		if err != nil {
			return TaskAssignment{}, false, err
		}
		activeKey := taskActiveOperationKey(task.OperationID)
		assignmentKey := taskExecutionClaimKey(executor, agentID, task.ID)
		assignmentIndexKey := taskAssignmentIndexKey(task.ID)
		deadline := claimAt.Add(time.Duration(task.TimeoutSeconds) * time.Second)
		timeoutIndexKey := taskTimeoutIndexKey(task.ID, deadline)
		companionKeys := []string{activeKey, assignmentKey, assignmentIndexKey, timeoutIndexKey}
		if candidate.writerKey != "" {
			companionKeys = append(companionKeys, candidate.writerKey)
		}
		companions, err := repository.store.GetMany(ctx, GetManyRequest{
			Keys: companionKeys, Revision: candidate.readRevision,
		})
		if err != nil {
			return TaskAssignment{}, false, err
		}
		if len(companions.Values) != len(companionKeys) || companions.Values[0] == nil || companions.Values[1] != nil ||
			companions.Values[2] != nil || companions.Values[3] != nil {
			return TaskAssignment{}, false, errs.New(
				errs.KindInternal,
				"queued Task lifecycle records are inconsistent",
			)
		}
		activeTaskID, err := decodeTaskReference(companions.Values[0].Value)
		if err != nil || activeTaskID != task.ID {
			return TaskAssignment{}, false, errs.New(
				errs.KindInternal,
				"active-operation record does not match queued Task",
			)
		}
		if candidate.writerKey != "" && companions.Values[4] != nil {
			conflicts++
			if err := repository.retryPolicy.waitAfterConflict(ctx, conflicts); err != nil {
				return TaskAssignment{}, false, err
			}
			continue
		}
		running, err := transitionTaskStatus(task, TaskStatusPending, TaskStatusRunning, claimAt)
		if err != nil {
			return TaskAssignment{}, false, err
		}
		assignment := TaskAssignmentRecord{
			AssignmentID: ids.New(ids.KindAssignment),
			TaskID:       task.ID, Executor: executor, AgentID: agentID, AgentGeneration: agentGeneration,
			ClaimedTaskRevision: taskValue.ModRevision, AssignedAt: claimAt, Deadline: deadline,
		}
		runningValue, err := encodeTaskRecord(running)
		if err != nil {
			return TaskAssignment{}, false, err
		}
		assignmentValue, err := encodeTaskAssignment(assignment)
		if err != nil {
			clear(runningValue)
			return TaskAssignment{}, false, err
		}
		conditions := []Condition{
			{Key: queued.Key, ModRevision: queued.ModRevision},
			{Key: taskKey(task.ID), ModRevision: taskValue.ModRevision},
			{Key: activeKey, ModRevision: companions.Values[0].ModRevision},
			{Key: assignmentKey},
			{Key: assignmentIndexKey},
			{Key: timeoutIndexKey},
		}
		mutations := []Mutation{
			{Type: MutationPut, Key: taskKey(task.ID), Value: runningValue},
			{Type: MutationDelete, Key: queued.Key},
			{Type: MutationPut, Key: assignmentKey, Value: assignmentValue},
			{Type: MutationPut, Key: assignmentIndexKey, Value: assignmentValue},
			{Type: MutationPut, Key: timeoutIndexKey, Value: assignmentValue},
		}
		var writerValue []byte
		if candidate.writerKey != "" {
			writerValue, err = encodeTaskMaterializationWriter(
				taskMaterializationWriter(task, candidate.environmentID),
			)
			if err != nil {
				clear(runningValue)
				clear(assignmentValue)
				return TaskAssignment{}, false, err
			}
			conditions = append(conditions, Condition{Key: candidate.writerKey})
			mutations = append(mutations, Mutation{
				Type: MutationPut, Key: candidate.writerKey, Value: writerValue,
			})
		}
		attachChange, err := repository.prepareAttachTaskClaim(ctx, task, candidate.readRevision)
		if err != nil {
			clear(runningValue)
			clear(assignmentValue)
			clear(writerValue)
			return TaskAssignment{}, false, err
		}
		if attachChange.applies {
			conditions = append(conditions, attachChange.conditions...)
			if attachChange.mutates {
				mutations = append(mutations, attachChange.mutations...)
			}
		}
		transaction, err := repository.store.Transact(ctx, conditions, mutations)
		clear(runningValue)
		clear(assignmentValue)
		clear(writerValue)
		clearAttachTaskChange(attachChange)
		if err != nil {
			return TaskAssignment{}, false, err
		}
		clearKeyValues(transaction.FailureReads)
		if !transaction.Succeeded {
			conflicts++
			if err := repository.retryPolicy.waitAfterConflict(ctx, conflicts); err != nil {
				return TaskAssignment{}, false, err
			}
			continue
		}
		return TaskAssignment{
			Assignment: Versioned[TaskAssignmentRecord]{
				Record: assignment, Revision: transaction.Revision, ReadRevision: transaction.Revision,
			},
			Task: Versioned[TaskRecord]{
				Record: running, Revision: transaction.Revision, ReadRevision: transaction.Revision,
			},
		}, true, nil
	}
}

const taskClaimQueuePageSize = 64

type taskClaimCandidate struct {
	queued        KeyValue
	taskValue     *KeyValue
	task          TaskRecord
	readRevision  int64
	writerKey     string
	environmentID string
}

func (repository *TaskRepository) nextTaskClaimCandidate(
	ctx context.Context,
	executor TaskExecutor,
) (taskClaimCandidate, bool, error) {
	prefix := taskQueueScopePrefix(executor)
	start := ""
	var revision int64
	for {
		page, err := repository.store.Range(ctx, RangeRequest{
			Prefix: prefix, StartExclusive: start, Limit: taskClaimQueuePageSize, Revision: revision,
		})
		if err != nil {
			return taskClaimCandidate{}, false, err
		}
		if revision == 0 {
			revision = page.ReadRevision
		}
		if page.ReadRevision != revision {
			return taskClaimCandidate{}, false, errs.New(errs.KindInternal, "task queue scan changed MVCC revision")
		}
		for _, queued := range page.Values {
			taskID, err := taskIDFromQueueKey(executor, queued.Key)
			if err != nil {
				return taskClaimCandidate{}, false, err
			}
			referencedTaskID, err := decodeTaskReference(queued.Value)
			if err != nil || referencedTaskID != taskID {
				return taskClaimCandidate{}, false, errs.New(
					errs.KindInternal,
					"task queue record does not match its key",
				)
			}
			taskRead, err := repository.store.GetMany(ctx, GetManyRequest{
				Keys: []string{taskKey(taskID)}, Revision: revision,
			})
			if err != nil {
				return taskClaimCandidate{}, false, err
			}
			if len(taskRead.Values) != 1 || taskRead.Values[0] == nil {
				return taskClaimCandidate{}, false, errs.New(errs.KindInternal, "queued Task primary is missing")
			}
			taskValue := taskRead.Values[0]
			task, err := decodeTaskRecord(taskValue.Value)
			if err != nil {
				return taskClaimCandidate{}, false, err
			}
			hierarchyChild := task.Executor == TaskExecutorAgent &&
				task.Params[TaskResourceKindParam] == TaskResourceHierarchyDeletion
			if task.ID != taskID || task.Executor != executor || task.Status != TaskStatusPending ||
				task.idempotencyMarker == nil && !hierarchyChild {
				return taskClaimCandidate{}, false, errs.New(errs.KindInternal, "queued Task is not claimable")
			}
			environmentID, materializes, err := taskEnvironmentWriter(task)
			if err != nil {
				return taskClaimCandidate{}, false, err
			}
			writerKey := ""
			if materializes {
				writerKey = taskMaterializationWriterKey(environmentID)
				writerRead, err := repository.store.GetMany(ctx, GetManyRequest{
					Keys: []string{writerKey}, Revision: revision,
				})
				if err != nil {
					return taskClaimCandidate{}, false, err
				}
				if len(writerRead.Values) != 1 {
					return taskClaimCandidate{}, false, errs.New(
						errs.KindInternal,
						"materialization writer read is incomplete",
					)
				}
				if writerRead.Values[0] != nil {
					writer, err := decodeTaskMaterializationWriter(writerRead.Values[0].Value)
					if err != nil || writer.EnvironmentID != environmentID || writer.TaskID == task.ID {
						return taskClaimCandidate{}, false, corruptTaskMaterializationWriter()
					}
					continue
				}
			}
			return taskClaimCandidate{
				queued: queued, taskValue: taskValue, task: task, readRevision: revision,
				writerKey: writerKey, environmentID: environmentID,
			}, true, nil
		}
		if !page.More {
			return taskClaimCandidate{}, false, nil
		}
		if len(page.Values) == 0 {
			return taskClaimCandidate{}, false, errs.New(
				errs.KindInternal,
				"task queue page is empty before completion",
			)
		}
		start = page.Values[len(page.Values)-1].Key
	}
}

// ListAgentAssignments restores the complete bounded assignment set for one
// exact Agent generation at one MVCC revision. The caller supplies the
// authorized max-concurrency bound from the Agent config.
func (repository *TaskRepository) ListAgentAssignments(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	maximum int32,
) ([]TaskAssignment, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	if validateStableID(ids.KindAgent, agentID) != nil || agentGeneration == 0 || maximum <= 0 {
		return nil, errs.New(errs.KindValidationFailed, "agent assignment query is invalid")
	}
	assignments, err := repository.store.Range(ctx, RangeRequest{
		Prefix: taskAssignmentScopePrefix(agentID),
		Limit:  int64(maximum) + 1,
	})
	if err != nil {
		return nil, err
	}
	if assignments.More || len(assignments.Values) > int(maximum) {
		return nil, errs.New(errs.KindInternal, "agent assignments exceed configured concurrency")
	}
	if len(assignments.Values) == 0 {
		return []TaskAssignment{}, nil
	}

	records := make([]TaskAssignmentRecord, len(assignments.Values))
	companionKeys := make([]string, 0, len(assignments.Values)*3)
	for index, value := range assignments.Values {
		taskID, err := taskIDFromAssignmentKey(agentID, value.Key)
		if err != nil {
			return nil, err
		}
		record, err := decodeTaskAssignment(value.Value)
		if err != nil {
			return nil, err
		}
		if record.TaskID != taskID || record.AgentID != agentID {
			return nil, errs.New(errs.KindInternal, "task assignment record does not match its key")
		}
		if record.AgentGeneration != agentGeneration {
			return nil, errs.New(errs.KindStateConflict, "durable Task assignment belongs to another Agent generation")
		}
		if record.ClaimedTaskRevision >= value.ModRevision {
			return nil, corruptTaskAssignment()
		}
		records[index] = record
		companionKeys = append(
			companionKeys,
			taskKey(taskID),
			taskAssignmentIndexKey(taskID),
			taskTimeoutIndexKey(taskID, record.Deadline),
		)
	}
	companions, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: companionKeys, Revision: assignments.ReadRevision,
	})
	if err != nil {
		return nil, err
	}
	if len(companions.Values) != len(companionKeys) {
		return nil, errs.New(errs.KindInternal, "agent assignment companion read is incomplete")
	}
	result := make([]TaskAssignment, len(records))
	for index, record := range records {
		taskValue := companions.Values[index*3]
		indexValue := companions.Values[index*3+1]
		timeoutValue := companions.Values[index*3+2]
		assignmentValue := assignments.Values[index]
		if taskValue == nil || indexValue == nil || timeoutValue == nil {
			return nil, errs.New(errs.KindInternal, "assigned Task companion is missing")
		}
		if indexValue.ModRevision != assignmentValue.ModRevision ||
			timeoutValue.ModRevision != assignmentValue.ModRevision ||
			!bytes.Equal(indexValue.Value, assignmentValue.Value) ||
			!bytes.Equal(timeoutValue.Value, assignmentValue.Value) {
			return nil, errs.New(errs.KindInternal, "durable Task assignment copies do not match")
		}
		task, err := decodeTaskRecord(taskValue.Value)
		if err != nil {
			return nil, err
		}
		if task.ID != record.TaskID || task.Status != TaskStatusRunning ||
			task.StartedAt == nil || !task.StartedAt.Equal(record.AssignedAt) ||
			!record.Deadline.Equal(record.AssignedAt.Add(time.Duration(task.TimeoutSeconds)*time.Second)) ||
			taskValue.ModRevision < assignmentValue.ModRevision || task.idempotencyMarker == nil {
			return nil, errs.New(errs.KindInternal, "durable Task assignment and Task are inconsistent")
		}
		environmentID, materializes, err := taskMaterializationEnvironment(task)
		if err != nil {
			return nil, err
		}
		if materializes {
			writerRead, err := repository.store.GetMany(ctx, GetManyRequest{
				Keys: []string{taskMaterializationWriterKey(environmentID)}, Revision: assignments.ReadRevision,
			})
			if err != nil {
				return nil, err
			}
			if len(writerRead.Values) != 1 || writerRead.Values[0] == nil {
				return nil, errs.New(errs.KindInternal, "assigned Task materialization writer is missing")
			}
			writer, err := decodeTaskMaterializationWriter(writerRead.Values[0].Value)
			if err != nil || writer != taskMaterializationWriter(task, environmentID) {
				return nil, corruptTaskMaterializationWriter()
			}
		}
		result[index] = TaskAssignment{
			Assignment: Versioned[TaskAssignmentRecord]{
				Record: record, Revision: assignmentValue.ModRevision,
				ReadRevision: assignments.ReadRevision,
			},
			Task: Versioned[TaskRecord]{
				Record: task, Revision: taskValue.ModRevision,
				ReadRevision: assignments.ReadRevision,
			},
		}
	}
	return result, nil
}

// ListControllerTaskClaims restores the single native Controller execution
// claim at one MVCC revision. The MVP runs one Controller worker serially, so
// more than one durable claim is corruption rather than hidden concurrency.
func (repository *TaskRepository) ListControllerTaskClaims(
	ctx context.Context,
) ([]TaskAssignment, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	claims, err := repository.store.Range(ctx, RangeRequest{
		Prefix: controllerTaskClaimPrefix,
		Limit:  2,
	})
	if err != nil {
		return nil, err
	}
	if claims.More || len(claims.Values) > 1 {
		return nil, errs.New(errs.KindInternal, "controller Task claims exceed serial execution")
	}
	if len(claims.Values) == 0 {
		return []TaskAssignment{}, nil
	}
	claimValue := claims.Values[0]
	claim, err := decodeTaskAssignment(claimValue.Value)
	if err != nil {
		return nil, err
	}
	if claim.Executor != TaskExecutorController || claimValue.Key != controllerTaskClaimKey(claim.TaskID) ||
		claim.ClaimedTaskRevision >= claimValue.ModRevision {
		return nil, errs.New(errs.KindInternal, "controller Task claim does not match its key")
	}
	companions, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			taskKey(claim.TaskID),
			taskAssignmentIndexKey(claim.TaskID),
			taskTimeoutIndexKey(claim.TaskID, claim.Deadline),
		},
		Revision: claims.ReadRevision,
	})
	if err != nil {
		return nil, err
	}
	if len(companions.Values) != 3 || companions.Values[0] == nil ||
		companions.Values[1] == nil || companions.Values[2] == nil {
		return nil, errs.New(errs.KindInternal, "claimed Controller Task companion is missing")
	}
	taskValue := companions.Values[0]
	if companions.Values[1].ModRevision != claimValue.ModRevision ||
		companions.Values[2].ModRevision != claimValue.ModRevision ||
		!bytes.Equal(companions.Values[1].Value, claimValue.Value) ||
		!bytes.Equal(companions.Values[2].Value, claimValue.Value) {
		return nil, errs.New(errs.KindInternal, "Controller Task assignment copies do not match")
	}
	task, err := decodeTaskRecord(taskValue.Value)
	if err != nil {
		return nil, err
	}
	if task.ID != claim.TaskID || task.Executor != TaskExecutorController ||
		task.Status != TaskStatusRunning || task.StartedAt == nil ||
		!task.StartedAt.Equal(claim.AssignedAt) ||
		!claim.Deadline.Equal(claim.AssignedAt.Add(time.Duration(task.TimeoutSeconds)*time.Second)) ||
		taskValue.ModRevision < claimValue.ModRevision || task.idempotencyMarker == nil {
		return nil, errs.New(errs.KindInternal, "controller Task claim and Task are inconsistent")
	}
	return []TaskAssignment{{
		Assignment: Versioned[TaskAssignmentRecord]{
			Record: claim, Revision: claimValue.ModRevision, ReadRevision: claims.ReadRevision,
		},
		Task: Versioned[TaskRecord]{
			Record: task, Revision: taskValue.ModRevision, ReadRevision: claims.ReadRevision,
		},
	}}, nil
}

// TimeoutAgentAssignments terminalizes the complete configured assignment set
// for one exact Agent generation. Stale-Agent detection owns when this method
// is called; the repository owns the normal terminal transaction and treats a
// concurrent terminal acknowledgement as an already-resolved assignment.
func (repository *TaskRepository) TimeoutAgentAssignments(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	maximum int32,
	terminalAt time.Time,
) (int, error) {
	if err := validateContext(ctx); err != nil {
		return 0, err
	}
	if err := validateTimestamp("stale Agent task terminal_at", terminalAt); err != nil {
		return 0, err
	}
	assignments, err := repository.ListAgentAssignments(ctx, agentID, agentGeneration, maximum)
	if err != nil {
		return 0, err
	}
	timedOut := 0
	for _, assignment := range assignments {
		_, err := repository.AcknowledgeTask(
			ctx,
			agentID,
			agentGeneration,
			assignment.Task.Record.ID,
			assignment.Assignment.Record.AssignmentID,
			TaskStatusTimedOut,
			TaskResultRecord{
				Kind: TaskResultCompose, Diagnostic: TaskResultDiagnosticNone,
				ReconciliationRequired: true,
			},
			terminalAt,
		)
		if err != nil {
			if errors.Is(err, errs.New(errs.KindStateConflict, "")) {
				continue
			}
			return timedOut, err
		}
		timedOut++
	}
	return timedOut, nil
}

// AcknowledgeTask atomically records the Agent's terminal acknowledgement,
// removes assignment and active-operation state, terminalizes replay evidence,
// and advances any owned Attach lifecycle. Replaying the same terminal
// acknowledgement is idempotent.
func (repository *TaskRepository) AcknowledgeTask(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	taskID string,
	assignmentID string,
	terminalStatus TaskStatus,
	result TaskResultRecord,
	terminalAt time.Time,
) (Versioned[TaskRecord], error) {
	return repository.acknowledgeTask(
		ctx, TaskExecutorAgent, agentID, agentGeneration, taskID, assignmentID,
		terminalStatus, &result, terminalAt, "",
	)
}

// AcknowledgeEnvironmentCreation atomically terminalizes one Agent Task and
// moves its owned Environment from provisioning to ready or failed.
func (repository *TaskRepository) AcknowledgeEnvironmentCreation(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	taskID string,
	assignmentID string,
	environmentID string,
	terminalStatus TaskStatus,
	result TaskResultRecord,
	terminalAt time.Time,
) (Versioned[TaskRecord], error) {
	return repository.acknowledgeTask(
		ctx,
		TaskExecutorAgent,
		agentID,
		agentGeneration,
		taskID,
		assignmentID,
		terminalStatus,
		&result,
		terminalAt,
		environmentID,
	)
}

// AcknowledgeControllerTask terminalizes one Controller claim. Native Tasks
// have no Compose result; their durable event journal carries execution detail.
func (repository *TaskRepository) AcknowledgeControllerTask(
	ctx context.Context,
	taskID string,
	terminalStatus TaskStatus,
	terminalAt time.Time,
) (Versioned[TaskRecord], error) {
	return repository.acknowledgeTask(
		ctx, TaskExecutorController, "", 0, taskID, "", terminalStatus, nil, terminalAt, "",
	)
}

func (repository *TaskRepository) acknowledgeTask(
	ctx context.Context,
	executor TaskExecutor,
	agentID string,
	agentGeneration uint64,
	taskID string,
	assignmentID string,
	terminalStatus TaskStatus,
	result *TaskResultRecord,
	terminalAt time.Time,
	environmentID string,
) (Versioned[TaskRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[TaskRecord]{}, err
	}
	if !validTaskExecutor(executor) || validateStableID(ids.KindTask, taskID) != nil ||
		!isTerminalTaskStatus(terminalStatus) ||
		(executor == TaskExecutorAgent && (validateStableID(ids.KindAgent, agentID) != nil || agentGeneration == 0 ||
			validateStableID(ids.KindAssignment, assignmentID) != nil || result == nil)) ||
		(executor == TaskExecutorController &&
			(agentID != "" || agentGeneration != 0 || assignmentID != "" || result != nil)) {
		return Versioned[TaskRecord]{}, errs.New(errs.KindValidationFailed, "task acknowledgement is invalid")
	}
	if err := validateTimestamp("task terminal_at", terminalAt); err != nil {
		return Versioned[TaskRecord]{}, err
	}
	if environmentID != "" && validateStableID(ids.KindEnvironment, environmentID) != nil {
		return Versioned[TaskRecord]{}, errs.New(
			errs.KindValidationFailed,
			"environment creation acknowledgement is invalid",
		)
	}

	claimKey := taskExecutionClaimKey(executor, agentID, taskID)
	conflicts := 0
	for {
		primaryAndAssignment, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
			taskKey(taskID), claimKey, taskAssignmentIndexKey(taskID),
		}})
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
		if len(primaryAndAssignment.Values) != 3 || primaryAndAssignment.Values[0] == nil {
			return Versioned[TaskRecord]{}, errs.Newf(errs.KindTaskNotFound, "task not found: %s", taskID)
		}
		taskValue := primaryAndAssignment.Values[0]
		task, err := decodeTaskRecord(taskValue.Value)
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
		if task.Executor != executor {
			return Versioned[TaskRecord]{}, errs.New(errs.KindStateConflict, "task execution authority changed")
		}
		if task.Params[TaskResourceKindParam] == TaskResourceHierarchyDeletion {
			if executor == TaskExecutorController {
				return repository.acknowledgeHierarchyDeletionControllerTask(
					ctx, taskID, terminalStatus, terminalAt,
				)
			}
			if result == nil {
				return Versioned[TaskRecord]{}, errs.New(errs.KindStateConflict, "hierarchy deletion Agent Task requires a result")
			}
			return repository.acknowledgeHierarchyDeletionAgentTask(
				ctx, agentID, agentGeneration, taskID, assignmentID,
				terminalStatus, *result, terminalAt,
			)
		}
		if task.Type == TaskBackup || task.Type == TaskBackupPrune {
			if executor != TaskExecutorAgent || result == nil {
				return Versioned[TaskRecord]{}, errs.New(
					errs.KindStateConflict,
					"backup Task requires an Agent acknowledgement",
				)
			}
			return repository.acknowledgeBackupTask(
				ctx,
				agentID,
				agentGeneration,
				taskID,
				assignmentID,
				terminalStatus,
				*result,
				terminalAt,
			)
		}
		assignmentValue := primaryAndAssignment.Values[1]
		assignmentIndexValue := primaryAndAssignment.Values[2]
		environmentCreation := executor == TaskExecutorAgent && task.Type == TaskCreate &&
			validateStableID(ids.KindEnvironment, task.Target) == nil
		environmentRemoval := executor == TaskExecutorAgent && task.Type == TaskRemove &&
			validateStableID(ids.KindEnvironment, task.Target) == nil
		zoneRemoval := executor == TaskExecutorAgent && task.Type == TaskRemove &&
			validateStableID(ids.KindNetwork, task.Target) == nil
		if environmentCreation != (environmentID != "") || (environmentCreation && task.Target != environmentID) {
			return Versioned[TaskRecord]{}, errs.New(
				errs.KindStateConflict,
				"environment creation Task requires its atomic provisioning acknowledgement",
			)
		}
		if result != nil {
			if err := validateTaskResult(*result, task.Steps, terminalStatus); err != nil {
				return Versioned[TaskRecord]{}, err
			}
		}
		if assignmentValue == nil {
			if assignmentIndexValue != nil {
				return Versioned[TaskRecord]{}, errs.New(errs.KindInternal, "task assignment index is orphaned")
			}
			if task.Status == terminalStatus &&
				((result == nil && task.Result == nil) ||
					(result != nil && task.Result != nil && taskResultsEqual(*task.Result, *result))) {
				if executor == TaskExecutorAgent {
					expected := TaskTerminalAssignmentRecord{
						AssignmentID: assignmentID, AgentID: agentID, AgentGeneration: agentGeneration,
					}
					if task.TerminalAssignment == nil || *task.TerminalAssignment != expected {
						return Versioned[TaskRecord]{}, errs.New(
							errs.KindStateConflict,
							"task terminal assignment identity does not match",
						)
					}
				}
				if environmentID != "" {
					if err := repository.validateEnvironmentCreationReplay(
						ctx, task, terminalStatus, primaryAndAssignment.ReadRevision,
					); err != nil {
						return Versioned[TaskRecord]{}, err
					}
				}
				if environmentRemoval {
					if err := repository.validateEnvironmentRemovalReplay(
						ctx, task, terminalStatus, primaryAndAssignment.ReadRevision,
					); err != nil {
						return Versioned[TaskRecord]{}, err
					}
				}
				if zoneRemoval {
					if err := repository.validateZoneRemovalReplay(
						ctx, task, terminalStatus, primaryAndAssignment.ReadRevision,
					); err != nil {
						return Versioned[TaskRecord]{}, err
					}
				}
				if err := repository.validateAttachTaskAcknowledgementReplay(
					ctx, task, terminalStatus, primaryAndAssignment.ReadRevision,
				); err != nil {
					return Versioned[TaskRecord]{}, err
				}
				if err := repository.validateSecretTaskAcknowledgementReplay(
					ctx, task, terminalStatus, primaryAndAssignment.ReadRevision,
				); err != nil {
					return Versioned[TaskRecord]{}, err
				}
				if err := repository.validateConnectorTaskAcknowledgementReplay(
					ctx, task, terminalStatus, primaryAndAssignment.ReadRevision,
				); err != nil {
					return Versioned[TaskRecord]{}, err
				}
				if err := repository.validateRunnerTaskAcknowledgementReplay(
					ctx, task, terminalStatus, primaryAndAssignment.ReadRevision,
				); err != nil {
					return Versioned[TaskRecord]{}, err
				}
				if err := repository.validateScriptTaskAcknowledgementReplay(
					ctx, task, terminalStatus, primaryAndAssignment.ReadRevision,
				); err != nil {
					return Versioned[TaskRecord]{}, err
				}
				if err := repository.validateReleaseGroupTaskAcknowledgementReplay(
					ctx, task, terminalStatus, primaryAndAssignment.ReadRevision,
				); err != nil {
					return Versioned[TaskRecord]{}, err
				}
				if task.Params[TaskReleasePublicationParam] != "" {
					headRead, err := repository.store.GetMany(ctx, GetManyRequest{
						Keys: []string{releaseOperationKey(task.OperationID)}, Revision: primaryAndAssignment.ReadRevision,
					})
					if err != nil || headRead == nil || len(headRead.Values) != 1 || headRead.Values[0] == nil {
						return Versioned[TaskRecord]{}, corruptReleaseRecord()
					}
					head, err := decodeReleaseRecord[ReleaseOperationHead](headRead.Values[0].Value, "release-operation")
					if err != nil || repository.validateReleaseTerminalMembers(
						ctx, task, head, terminalStatus, primaryAndAssignment.ReadRevision,
					) != nil {
						return Versioned[TaskRecord]{}, corruptReleaseRecord()
					}
				}
				if err := repository.validateRemovalTaskAcknowledgementReplay(
					ctx, task, terminalStatus, primaryAndAssignment.ReadRevision,
				); err != nil {
					return Versioned[TaskRecord]{}, err
				}
				if err := repository.validateBackingZoneTaskAcknowledgementReplay(
					ctx, task, terminalStatus, primaryAndAssignment.ReadRevision,
				); err != nil {
					return Versioned[TaskRecord]{}, err
				}
				if err := repository.validateComponentTaskAcknowledgementReplay(
					ctx, task, terminalStatus, primaryAndAssignment.ReadRevision,
				); err != nil {
					return Versioned[TaskRecord]{}, err
				}
				if err := repository.validateBackupKeyRotationTaskAcknowledgementReplay(
					ctx, task, terminalStatus, primaryAndAssignment.ReadRevision,
				); err != nil {
					return Versioned[TaskRecord]{}, err
				}
				if err := repository.validateTaskRetentionReplay(
					ctx, task, primaryAndAssignment.ReadRevision,
				); err != nil {
					return Versioned[TaskRecord]{}, err
				}
				return Versioned[TaskRecord]{
					Record: task, Revision: taskValue.ModRevision,
					ReadRevision: primaryAndAssignment.ReadRevision,
				}, nil
			}
			return Versioned[TaskRecord]{}, errs.New(errs.KindStateConflict, "task has no matching active assignment")
		}
		assignment, err := decodeTaskAssignment(assignmentValue.Value)
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
		if assignmentIndexValue == nil || assignmentIndexValue.ModRevision != assignmentValue.ModRevision ||
			!bytes.Equal(assignmentIndexValue.Value, assignmentValue.Value) {
			return Versioned[TaskRecord]{}, errs.New(
				errs.KindInternal,
				"task assignment index does not match assignment",
			)
		}
		if assignment.TaskID != task.ID || assignment.Executor != executor ||
			(executor == TaskExecutorAgent && assignment.AssignmentID != assignmentID) ||
			assignment.AgentID != agentID ||
			assignment.AgentGeneration != agentGeneration ||
			assignment.ClaimedTaskRevision >= assignmentValue.ModRevision || task.StartedAt == nil ||
			!assignment.AssignedAt.Equal(*task.StartedAt) {
			return Versioned[TaskRecord]{}, errs.New(
				errs.KindStateConflict,
				"task assignment does not match the Agent generation",
			)
		}
		terminalAt, err = nextTaskControllerTimestamp(task.UpdatedAt, terminalAt)
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
		if executor == TaskExecutorAgent && task.Params[TaskReleasePublicationParam] != "" {
			processed, err := repository.finalizeReleaseTaskBatch(
				ctx, task, assignment, terminalStatus, *result, agentID, terminalAt,
				primaryAndAssignment.ReadRevision,
			)
			if err != nil {
				return Versioned[TaskRecord]{}, err
			}
			if processed {
				continue
			}
		}
		if environmentRemoval && terminalStatus == TaskStatusCompleted {
			processed, err := repository.finalizeEnvironmentBlueprintRevisionBatch(ctx, task, terminalAt)
			if err != nil {
				return Versioned[TaskRecord]{}, err
			}
			if processed {
				continue
			}
		}
		if zoneRemoval && terminalStatus == TaskStatusCompleted {
			processed, err := repository.finalizeZoneServiceMembershipBatch(ctx, task, terminalAt)
			if err != nil {
				return Versioned[TaskRecord]{}, err
			}
			if processed {
				continue
			}
		}
		terminal, err := transitionTaskStatus(task, TaskStatusRunning, terminalStatus, terminalAt)
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
		terminal.Result = cloneTaskResult(result)
		if executor == TaskExecutorAgent {
			terminal.TerminalAssignment = &TaskTerminalAssignmentRecord{
				AssignmentID: assignmentID, AgentID: agentID, AgentGeneration: agentGeneration,
			}
		}
		if err := validateTaskRecord(terminal); err != nil {
			return Versioned[TaskRecord]{}, err
		}
		transitionedMarker, markerKey, retentionKey, err := prepareTerminalTaskMarker(
			task,
			terminalStatus,
			terminalAt,
		)
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
		materializationEnvironmentID, materializes, err := taskEnvironmentWriter(task)
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
		companionKeys := []string{
			taskActiveOperationKey(task.OperationID), markerKey, taskQueueKey(task.Executor, task.ID), retentionKey,
			taskTimeoutIndexKey(task.ID, assignment.Deadline),
		}
		writerKey := ""
		if materializes {
			writerKey = taskMaterializationWriterKey(materializationEnvironmentID)
			companionKeys = append(companionKeys, writerKey)
		}
		companions, err := repository.store.GetMany(ctx, GetManyRequest{
			Keys:     companionKeys,
			Revision: primaryAndAssignment.ReadRevision,
		})
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
		if len(companions.Values) != len(companionKeys) || companions.Values[0] == nil || companions.Values[1] == nil ||
			companions.Values[2] != nil || companions.Values[3] != nil || companions.Values[4] == nil ||
			companions.Values[4].ModRevision != assignmentValue.ModRevision ||
			!bytes.Equal(companions.Values[4].Value, assignmentValue.Value) {
			return Versioned[TaskRecord]{}, errs.New(
				errs.KindInternal,
				"running Task lifecycle records are inconsistent",
			)
		}
		if err := validateTaskLifecycleCompanions(task, companions.Values[0], companions.Values[1]); err != nil {
			return Versioned[TaskRecord]{}, err
		}
		if materializes {
			if companions.Values[5] == nil {
				return Versioned[TaskRecord]{}, errs.New(errs.KindInternal, "task materialization writer is missing")
			}
			writer, err := decodeTaskMaterializationWriter(companions.Values[5].Value)
			if err != nil || writer != taskMaterializationWriter(task, materializationEnvironmentID) {
				return Versioned[TaskRecord]{}, corruptTaskMaterializationWriter()
			}
		}
		transitionedMarker, err = hydrateTerminalTaskMarker(transitionedMarker, companions.Values[1].Value)
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
		terminalValue, err := encodeTaskRecord(terminal)
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
		markerValue, err := encodeIdempotencyMarker(transitionedMarker)
		clear(transitionedMarker.Intent.Ciphertext)
		clear(transitionedMarker.Response.Body)
		if err != nil {
			clear(terminalValue)
			return Versioned[TaskRecord]{}, err
		}
		retentionValue, err := json.Marshal(retentionReferenceJSON{Schema: 1, MarkerKey: markerKey})
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			return Versioned[TaskRecord]{}, errs.Wrap(errs.KindInternal, err)
		}
		taskRetentionKey, taskRetentionValue, err := prepareTaskRetentionIndex(terminal)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			return Versioned[TaskRecord]{}, err
		}
		conditions := []Condition{
			{Key: taskKey(task.ID), ModRevision: taskValue.ModRevision},
			{Key: claimKey, ModRevision: assignmentValue.ModRevision},
			{Key: taskAssignmentIndexKey(task.ID), ModRevision: assignmentIndexValue.ModRevision},
			{Key: taskActiveOperationKey(task.OperationID), ModRevision: companions.Values[0].ModRevision},
			{Key: markerKey, ModRevision: companions.Values[1].ModRevision},
			{Key: taskQueueKey(task.Executor, task.ID)},
			{Key: retentionKey},
			{Key: taskRetentionKey},
			{Key: taskTimeoutIndexKey(task.ID, assignment.Deadline), ModRevision: companions.Values[4].ModRevision},
		}
		mutations := []Mutation{
			{Type: MutationPut, Key: taskKey(task.ID), Value: terminalValue},
			{Type: MutationDelete, Key: claimKey},
			{Type: MutationDelete, Key: taskAssignmentIndexKey(task.ID)},
			{Type: MutationDelete, Key: taskActiveOperationKey(task.OperationID)},
			{Type: MutationPut, Key: markerKey, Value: markerValue},
			{Type: MutationPut, Key: retentionKey, Value: retentionValue},
			{Type: MutationPut, Key: taskRetentionKey, Value: taskRetentionValue},
			{Type: MutationDelete, Key: taskTimeoutIndexKey(task.ID, assignment.Deadline)},
		}
		if materializes {
			conditions = append(conditions, Condition{Key: writerKey, ModRevision: companions.Values[5].ModRevision})
			mutations = append(mutations, Mutation{Type: MutationDelete, Key: writerKey})
		}
		var environmentValue []byte
		if environmentID != "" {
			environmentConditions, environmentMutations, value, err := repository.prepareEnvironmentCreationAcknowledgement(
				ctx,
				task,
				terminalStatus,
				primaryAndAssignment.ReadRevision,
			)
			if err != nil {
				clear(terminalValue)
				clear(markerValue)
				clear(retentionValue)
				return Versioned[TaskRecord]{}, err
			}
			environmentValue = value
			conditions = append(conditions, environmentConditions...)
			mutations = append(mutations, environmentMutations...)
		}
		if environmentRemoval {
			environmentConditions, environmentMutations, err := repository.prepareEnvironmentRemovalAcknowledgement(
				ctx, task, terminalStatus, primaryAndAssignment.ReadRevision,
			)
			if err != nil {
				clear(terminalValue)
				clear(markerValue)
				clear(retentionValue)
				clear(environmentValue)
				return Versioned[TaskRecord]{}, err
			}
			conditions = append(conditions, environmentConditions...)
			mutations = append(mutations, environmentMutations...)
		}
		if zoneRemoval {
			zoneConditions, zoneMutations, err := repository.prepareZoneRemovalAcknowledgement(
				ctx, task, terminalStatus, primaryAndAssignment.ReadRevision,
			)
			if err != nil {
				clear(terminalValue)
				clear(markerValue)
				clear(retentionValue)
				clear(environmentValue)
				return Versioned[TaskRecord]{}, err
			}
			defer clearMutationValues(zoneMutations)
			conditions = append(conditions, zoneConditions...)
			mutations = append(mutations, zoneMutations...)
		}
		attachChange, err := repository.prepareAttachTaskAcknowledgement(
			ctx, task, terminalStatus, primaryAndAssignment.ReadRevision,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(environmentValue)
			return Versioned[TaskRecord]{}, err
		}
		if attachChange.applies {
			conditions = append(conditions, attachChange.conditions...)
			mutations = append(mutations, attachChange.mutations...)
		}
		secretChange, err := repository.prepareSecretTaskAcknowledgement(
			ctx, task, terminalStatus, primaryAndAssignment.ReadRevision,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(environmentValue)
			clearAttachTaskChange(attachChange)
			return Versioned[TaskRecord]{}, err
		}
		if secretChange.applies {
			conditions = append(conditions, secretChange.conditions...)
			mutations = append(mutations, secretChange.mutations...)
		}
		scriptChange, err := repository.prepareScriptTaskAcknowledgement(
			ctx, task, terminalStatus, primaryAndAssignment.ReadRevision,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(environmentValue)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			return Versioned[TaskRecord]{}, err
		}
		if scriptChange.applies {
			conditions = append(conditions, scriptChange.conditions...)
			mutations = append(mutations, scriptChange.mutations...)
		}
		releaseGroupChange, err := repository.prepareReleaseGroupTaskAcknowledgement(
			ctx, task, terminalStatus, primaryAndAssignment.ReadRevision,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(environmentValue)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			clearScriptTaskChange(scriptChange)
			return Versioned[TaskRecord]{}, err
		}
		defer clearReleaseGroupTaskChange(releaseGroupChange)
		if releaseGroupChange.applies {
			conditions = append(conditions, releaseGroupChange.conditions...)
			mutations = append(mutations, releaseGroupChange.mutations...)
		}
		routeChange, err := repository.prepareRemovalTaskAcknowledgement(
			ctx, task, terminalStatus, terminalAt, primaryAndAssignment.ReadRevision,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(environmentValue)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			return Versioned[TaskRecord]{}, err
		}
		if routeChange.applies {
			conditions = append(conditions, routeChange.conditions...)
			mutations = append(mutations, routeChange.mutations...)
		}
		serviceChange, err := repository.prepareServiceTaskAcknowledgement(
			ctx, task, primaryAndAssignment.ReadRevision,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(environmentValue)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			clearRouteTaskChange(routeChange)
			return Versioned[TaskRecord]{}, err
		}
		if serviceChange.applies {
			conditions = append(conditions, serviceChange.conditions...)
			mutations = append(mutations, serviceChange.mutations...)
		}
		backingZoneChange, err := repository.prepareBackingZoneTaskAcknowledgement(
			ctx, task, terminalStatus, primaryAndAssignment.ReadRevision,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(environmentValue)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			clearRouteTaskChange(routeChange)
			clearServiceTaskChange(serviceChange)
			return Versioned[TaskRecord]{}, err
		}
		if backingZoneChange.applies {
			conditions = append(conditions, backingZoneChange.conditions...)
			mutations = append(mutations, backingZoneChange.mutations...)
		}
		componentChange, err := repository.prepareComponentTaskAcknowledgement(
			ctx,
			task,
			terminalStatus,
			terminalAt,
			primaryAndAssignment.ReadRevision,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(environmentValue)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			clearRouteTaskChange(routeChange)
			clearServiceTaskChange(serviceChange)
			clearBackingZoneTaskChange(backingZoneChange)
			return Versioned[TaskRecord]{}, err
		}
		if componentChange.applies {
			conditions = append(conditions, componentChange.conditions...)
			mutations = append(mutations, componentChange.mutations...)
		}
		connectorChange, err := repository.prepareConnectorTaskAcknowledgement(
			ctx, task, terminalStatus, primaryAndAssignment.ReadRevision,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(environmentValue)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			clearRouteTaskChange(routeChange)
			clearServiceTaskChange(serviceChange)
			clearBackingZoneTaskChange(backingZoneChange)
			clearComponentTaskChange(componentChange)
			return Versioned[TaskRecord]{}, err
		}
		if connectorChange.applies {
			conditions = append(conditions, connectorChange.conditions...)
			mutations = append(mutations, connectorChange.mutations...)
		}
		runnerChange, err := repository.prepareRunnerTaskAcknowledgement(
			ctx, task, terminalStatus, primaryAndAssignment.ReadRevision,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(environmentValue)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			clearRouteTaskChange(routeChange)
			clearServiceTaskChange(serviceChange)
			clearBackingZoneTaskChange(backingZoneChange)
			clearComponentTaskChange(componentChange)
			clearConnectorTaskChange(connectorChange)
			return Versioned[TaskRecord]{}, err
		}
		if runnerChange.applies {
			conditions = append(conditions, runnerChange.conditions...)
			mutations = append(mutations, runnerChange.mutations...)
		}
		rotationChange, err := repository.prepareBackupKeyRotationTaskAcknowledgement(
			ctx, task, terminalStatus, terminalAt, primaryAndAssignment.ReadRevision,
		)
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
		defer rotationChange.clear()
		conditions = append(conditions, rotationChange.conditions...)
		mutations = append(mutations, rotationChange.mutations...)
		environmentBinding, err := repository.bindOrdinaryTaskEnvironmentMutation(
			ctx,
			task,
			primaryAndAssignment.ReadRevision,
			conditions,
			mutations,
			attachChange.applies,
			routeChange.applies && task.Params[TaskEntryEnvironmentParam] != "",
			serviceChange.applies,
			connectorChange.applies,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(taskRetentionValue)
			clear(environmentValue)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			clearRouteTaskChange(routeChange)
			clearServiceTaskChange(serviceChange)
			clearBackingZoneTaskChange(backingZoneChange)
			clearComponentTaskChange(componentChange)
			clearConnectorTaskChange(connectorChange)
			clearRunnerTaskChange(runnerChange)
			return Versioned[TaskRecord]{}, err
		}
		var environmentEpochValue []byte
		if environmentBinding != nil {
			conditions = environmentBinding.conditions
			mutations = environmentBinding.mutations
			environmentEpochValue = mutations[len(mutations)-1].Value
		}
		transaction, err := repository.store.Transact(ctx, conditions, mutations)
		clear(terminalValue)
		clear(markerValue)
		clear(retentionValue)
		clear(taskRetentionValue)
		clear(environmentValue)
		clearAttachTaskChange(attachChange)
		clearSecretTaskChange(secretChange)
		clearRouteTaskChange(routeChange)
		clearServiceTaskChange(serviceChange)
		clearBackingZoneTaskChange(backingZoneChange)
		clearComponentTaskChange(componentChange)
		clearConnectorTaskChange(connectorChange)
		clearRunnerTaskChange(runnerChange)
		clear(environmentEpochValue)
		environmentBinding.clear()
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
		clearKeyValues(transaction.FailureReads)
		if !transaction.Succeeded {
			conflicts++
			if err := repository.retryPolicy.waitAfterConflict(ctx, conflicts); err != nil {
				return Versioned[TaskRecord]{}, err
			}
			continue
		}
		return Versioned[TaskRecord]{
			Record: terminal, Revision: transaction.Revision, ReadRevision: transaction.Revision,
		}, nil
	}
}

// acknowledgeBackupTask routes the public Agent acknowledgement through the
// Backup domain's terminal plan. Task state, run or prune state, assignment
// cleanup, exclusions, dispatch, and the Environment lock therefore commit at
// one revision rather than exposing a generic Task-only terminal gap.
func (repository *TaskRepository) acknowledgeBackupTask(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	taskID string,
	assignmentID string,
	terminalStatus TaskStatus,
	result TaskResultRecord,
	terminalAt time.Time,
) (Versioned[TaskRecord], error) {
	runtime, err := newBackupRuntimeRepository(repository.store)
	if err != nil {
		return Versioned[TaskRecord]{}, err
	}
	conflicts := 0
	for {
		current, assigned, err := repository.loadBackupTaskAssignment(
			ctx, agentID, agentGeneration, taskID, assignmentID,
		)
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
		if !assigned {
			expectedAssignment := TaskTerminalAssignmentRecord{
				AssignmentID: assignmentID, AgentID: agentID, AgentGeneration: agentGeneration,
			}
			if err := repository.validateBackupTaskTerminalReplay(
				ctx, current.Task, terminalStatus, &result, &expectedAssignment,
			); err != nil {
				return Versioned[TaskRecord]{}, err
			}
			return current.Task, nil
		}

		effectiveAt, err := nextTaskControllerTimestamp(current.Task.Record.UpdatedAt, terminalAt)
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
		var conditions []Condition
		var mutations []Mutation
		var terminal TaskRecord
		switch current.Task.Record.Type {
		case TaskBackup:
			run, getErr := runtime.GetBackupRun(ctx, taskID)
			if getErr != nil {
				return Versioned[TaskRecord]{}, getErr
			}
			if err := validateBackupRunTaskBinding(current.Task.Record, run.Record); err != nil {
				return Versioned[TaskRecord]{}, err
			}
			effectiveAt = backupTerminalTimestamp(effectiveAt, run.Record.UpdatedAt)
			next, transitionErr := backupRunForTaskTerminal(run.Record, terminalStatus, effectiveAt)
			if transitionErr != nil {
				return Versioned[TaskRecord]{}, transitionErr
			}
			taskPlan, prepareErr := repository.prepareBackupTaskTerminal(
				ctx, current, terminalStatus, result, effectiveAt,
			)
			if prepareErr != nil {
				return Versioned[TaskRecord]{}, prepareErr
			}
			runPlan, prepareErr := runtime.prepareBackupRunTerminal(ctx, run, next)
			if prepareErr != nil {
				taskPlan.clear()
				return Versioned[TaskRecord]{}, prepareErr
			}
			receiptPlan, prepareErr := prepareBackupRunTerminalReceipt(
				current.Task, taskPlan.record, runPlan.record,
			)
			if prepareErr != nil {
				taskPlan.clear()
				runPlan.clear()
				return Versioned[TaskRecord]{}, prepareErr
			}
			terminal = taskPlan.record
			conditions, mutations, err = composeBackupRunTerminalTransaction(
				taskPlan, runPlan, receiptPlan,
			)
			taskPlan.clear()
			runPlan.clear()
			receiptPlan.clear()
		case TaskBackupPrune:
			dispatch, prunes, loadErr := repository.loadBackupPruneTerminalAuthority(
				ctx, current.Task.Record,
			)
			if loadErr != nil {
				return Versioned[TaskRecord]{}, loadErr
			}
			effectiveAt = backupTerminalTimestamp(effectiveAt, dispatch.Record.CreatedAt)
			for _, prune := range prunes {
				effectiveAt = backupTerminalTimestamp(effectiveAt, prune.Record.UpdatedAt)
			}
			taskPlan, prepareErr := repository.prepareBackupTaskTerminal(
				ctx, current, terminalStatus, result, effectiveAt,
			)
			if prepareErr != nil {
				return Versioned[TaskRecord]{}, prepareErr
			}
			var prunePlan backupPruneTransactionPlan
			if terminalStatus == TaskStatusCompleted {
				prunePlan, prepareErr = runtime.prepareBackupPruneCompletion(ctx, dispatch, prunes)
			} else {
				prunePlan, prepareErr = runtime.prepareBackupPruneFailure(
					ctx, dispatch, prunes, effectiveAt,
				)
			}
			if prepareErr != nil {
				taskPlan.clear()
				return Versioned[TaskRecord]{}, prepareErr
			}
			receiptPlan, prepareErr := prepareBackupPruneTerminalReceipt(
				current.Task, taskPlan.record, dispatch.Record, prunes,
			)
			if prepareErr != nil {
				taskPlan.clear()
				prunePlan.clear()
				return Versioned[TaskRecord]{}, prepareErr
			}
			terminal = taskPlan.record
			conditions, mutations, err = composeBackupPruneTerminalTransaction(
				taskPlan, prunePlan, receiptPlan,
			)
			taskPlan.clear()
			prunePlan.clear()
			receiptPlan.clear()
		default:
			return Versioned[TaskRecord]{}, errs.New(
				errs.KindInternal,
				"backup Task terminal dispatch received an ordinary Task",
			)
		}
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
		transaction, err := runtime.transact(ctx, conditions, mutations)
		clearBackupRuntimeMutations(mutations)
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
		clearKeyValues(transaction.FailureReads)
		if !transaction.Succeeded {
			conflicts++
			if err := repository.retryPolicy.waitAfterConflict(ctx, conflicts); err != nil {
				return Versioned[TaskRecord]{}, err
			}
			continue
		}
		return Versioned[TaskRecord]{
			Record: terminal, Revision: transaction.Revision, ReadRevision: transaction.Revision,
		}, nil
	}
}

func (repository *TaskRepository) loadBackupTaskAssignment(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	taskID string,
	assignmentID string,
) (TaskAssignment, bool, error) {
	claimKey := taskAssignmentKey(agentID, taskID)
	read, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
		taskKey(taskID), claimKey, taskAssignmentIndexKey(taskID),
	}})
	if err != nil {
		return TaskAssignment{}, false, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != 3 || read.Values[0] == nil {
		return TaskAssignment{}, false, errs.New(errs.KindInternal, "backup Task assignment read is incomplete")
	}
	defer clearKeyValues(read.Values)
	task, err := decodeTaskRecord(read.Values[0].Value)
	if err != nil || task.ID != taskID || task.Executor != TaskExecutorAgent ||
		(task.Type != TaskBackup && task.Type != TaskBackupPrune) {
		return TaskAssignment{}, false, errs.New(errs.KindInternal, "backup Task assignment is corrupt")
	}
	current := TaskAssignment{Task: Versioned[TaskRecord]{
		Record: task, Revision: read.Values[0].ModRevision, ReadRevision: read.ReadRevision,
	}}
	if read.Values[1] == nil {
		if read.Values[2] != nil {
			return TaskAssignment{}, false, errs.New(
				errs.KindStateConflict,
				"backup Task assignment identity changed",
			)
		}
		return current, false, nil
	}
	if read.Values[2] == nil || read.Values[1].ModRevision != read.Values[2].ModRevision ||
		!bytes.Equal(read.Values[1].Value, read.Values[2].Value) {
		return TaskAssignment{}, false, errs.New(errs.KindInternal, "backup Task assignment copies differ")
	}
	assignment, err := decodeTaskAssignment(read.Values[1].Value)
	if err != nil || assignment.TaskID != taskID || assignment.Executor != TaskExecutorAgent ||
		assignment.AssignmentID != assignmentID || assignment.AgentID != agentID ||
		assignment.AgentGeneration != agentGeneration || assignment.ClaimedTaskRevision >= read.Values[1].ModRevision ||
		task.Status != TaskStatusRunning || task.StartedAt == nil ||
		!task.StartedAt.Equal(assignment.AssignedAt) ||
		!assignment.Deadline.Equal(assignment.AssignedAt.Add(time.Duration(task.TimeoutSeconds)*time.Second)) ||
		read.Values[0].ModRevision < read.Values[1].ModRevision || task.idempotencyMarker == nil {
		return TaskAssignment{}, false, errs.New(
			errs.KindStateConflict,
			"backup Task assignment identity changed",
		)
	}
	current.Assignment = Versioned[TaskAssignmentRecord]{
		Record: assignment, Revision: read.Values[1].ModRevision, ReadRevision: read.ReadRevision,
	}
	return current, true, nil
}

func (repository *TaskRepository) loadBackupPruneTerminalAuthority(
	ctx context.Context,
	task TaskRecord,
) (
	Versioned[BackupRecoveryPointPruneDispatchRecord],
	[]Versioned[BackupRecoveryPointPruneRecord],
	error,
) {
	taskID := task.ID
	dispatchRead, err := repository.store.Get(ctx, backupRecoveryPointPruneDispatchKey(taskID))
	if err != nil {
		return Versioned[BackupRecoveryPointPruneDispatchRecord]{}, nil, err
	}
	if dispatchRead == nil || dispatchRead.Entry == nil || dispatchRead.ReadRevision <= 0 {
		return Versioned[BackupRecoveryPointPruneDispatchRecord]{}, nil, errs.New(
			errs.KindInternal,
			"backup prune dispatch is missing for its active Task",
		)
	}
	defer clear(dispatchRead.Entry.Value)
	dispatchRecord, err := decodeBackupRecoveryPointPruneDispatchRecord(dispatchRead.Entry.Value)
	if err != nil || dispatchRecord.TaskID != taskID {
		return Versioned[BackupRecoveryPointPruneDispatchRecord]{}, nil, corruptBackupRuntimeRecord()
	}
	if err := validateBackupPruneTaskBinding(task, dispatchRecord); err != nil {
		return Versioned[BackupRecoveryPointPruneDispatchRecord]{}, nil, err
	}
	dispatch := Versioned[BackupRecoveryPointPruneDispatchRecord]{
		Record: dispatchRecord, Revision: dispatchRead.Entry.ModRevision,
		ReadRevision: dispatchRead.ReadRevision,
	}
	keys := make([]string, len(dispatchRecord.RecoveryPointIDs))
	for index, pointID := range dispatchRecord.RecoveryPointIDs {
		keys[index] = backupRecoveryPointPruneKey(pointID)
	}
	read, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys})
	if err != nil {
		return Versioned[BackupRecoveryPointPruneDispatchRecord]{}, nil, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != len(keys) {
		return Versioned[BackupRecoveryPointPruneDispatchRecord]{}, nil, errs.New(
			errs.KindInternal,
			"backup prune authority read is incomplete",
		)
	}
	defer clearKeyValues(read.Values)
	prunes := make([]Versioned[BackupRecoveryPointPruneRecord], len(keys))
	for index, value := range read.Values {
		if value == nil {
			return Versioned[BackupRecoveryPointPruneDispatchRecord]{}, nil, errs.New(
				errs.KindInternal,
				"backup prune authority is missing for its active Task",
			)
		}
		record, decodeErr := decodeBackupRecoveryPointPruneRecord(value.Value)
		if decodeErr != nil || record.Point.ID != dispatchRecord.RecoveryPointIDs[index] ||
			record.Point.EnvironmentID != dispatchRecord.EnvironmentID ||
			record.OperationID != dispatchRecord.OperationID || record.TaskID != taskID ||
			(record.State != BackupPruneAssigned && record.State != BackupPruneVerifiedAbsent) {
			return Versioned[BackupRecoveryPointPruneDispatchRecord]{}, nil, corruptBackupRuntimeRecord()
		}
		prunes[index] = Versioned[BackupRecoveryPointPruneRecord]{
			Record: record, Revision: value.ModRevision, ReadRevision: read.ReadRevision,
		}
	}
	return dispatch, prunes, nil
}

func backupRunForTaskTerminal(
	current BackupRunRecord,
	terminalStatus TaskStatus,
	terminalAt time.Time,
) (BackupRunRecord, error) {
	next := current
	next.Sources = append([]BackupRunSourceAttemptRecord(nil), current.Sources...)
	next.UpdatedAt = terminalAt
	switch terminalStatus {
	case TaskStatusCompleted:
		next.State = BackupRunCompleted
		return next, nil
	case TaskStatusFailed:
		next.State = BackupRunFailed
	case TaskStatusAborted:
		next.State = BackupRunAborted
	case TaskStatusTimedOut:
		next.State = BackupRunTimedOut
	default:
		return BackupRunRecord{}, errs.New(
			errs.KindValidationFailed,
			"backup Task terminal status is invalid",
		)
	}
	boundary := -1
	for index := range next.Sources {
		if next.Sources[index].State != BackupSourceAttemptSucceeded {
			boundary = index
			break
		}
	}
	if boundary < 0 {
		return BackupRunRecord{}, errs.New(
			errs.KindStateConflict,
			"non-success Backup acknowledgement has no active source",
		)
	}
	source := &next.Sources[boundary]
	if source.State == BackupSourceAttemptStaged &&
		(source.Phase == BackupSourcePhaseUpload ||
			source.Phase == BackupSourcePhaseHeadVerification ||
			source.Phase == BackupSourcePhasePointCommit) {
		source.State = BackupSourceAttemptOrphaned
	} else if source.State != BackupSourceAttemptOrphaned {
		source.State = BackupSourceAttemptFailed
	}
	switch terminalStatus {
	case TaskStatusAborted:
		source.FailureCode = BackupFailureAborted
	case TaskStatusTimedOut:
		source.FailureCode = BackupFailureTimedOut
	default:
		failureCode, err := backupFailureCodeForPhase(source.Phase)
		if err != nil {
			return BackupRunRecord{}, err
		}
		source.FailureCode = failureCode
	}
	for index := boundary + 1; index < len(next.Sources); index++ {
		next.Sources[index].State = BackupSourceAttemptUnstarted
		next.Sources[index].Phase = BackupSourcePhaseCapture
		next.Sources[index].SizeBytes = 0
		next.Sources[index].SHA256 = ""
		next.Sources[index].FailureCode = ""
	}
	return next, nil
}

func validateBackupRunTaskBinding(task TaskRecord, run BackupRunRecord) error {
	if task.Type != TaskBackup || task.ID != run.TaskID || task.OperationID != run.OperationID ||
		task.Owner.EnvironmentID == "" || task.Owner.EnvironmentID != run.EnvironmentID ||
		task.Target != run.EnvironmentID || !task.CreatedAt.Equal(run.CreatedAt) ||
		task.RetryOf != run.RetryOfTaskID {
		return errs.New(errs.KindInternal, "backup task and run identity differ")
	}
	return nil
}

func validateBackupPruneTaskBinding(
	task TaskRecord,
	dispatch BackupRecoveryPointPruneDispatchRecord,
) error {
	if task.Type != TaskBackupPrune || task.ID != dispatch.TaskID ||
		task.OperationID != dispatch.OperationID || task.Owner.EnvironmentID == "" ||
		task.Owner.EnvironmentID != dispatch.EnvironmentID || task.Target != dispatch.EnvironmentID ||
		!task.CreatedAt.Equal(dispatch.CreatedAt) {
		return errs.New(errs.KindInternal, "backup prune Task and dispatch identity differ")
	}
	return nil
}

func backupFailureCodeForPhase(phase BackupSourceAttemptPhase) (BackupFailureCode, error) {
	switch phase {
	case BackupSourcePhaseCapture:
		return BackupFailureCapture, nil
	case BackupSourcePhaseStaging:
		return BackupFailureStaging, nil
	case BackupSourcePhaseUpload:
		return BackupFailureUpload, nil
	case BackupSourcePhaseHeadVerification:
		return BackupFailureHeadVerification, nil
	case BackupSourcePhasePointCommit:
		return BackupFailurePointCommit, nil
	case BackupSourcePhaseCleanup:
		return BackupFailureCleanup, nil
	case BackupSourcePhaseRetention:
		return BackupFailureRetention, nil
	default:
		return "", errs.New(errs.KindInternal, "backup source phase is corrupt")
	}
}

func backupTerminalTimestamp(supplied time.Time, previous time.Time) time.Time {
	if supplied.After(previous) {
		return supplied
	}
	return previous.Add(time.Nanosecond)
}

func (repository *TaskRepository) validateBackupTaskTerminalReplay(
	ctx context.Context,
	task Versioned[TaskRecord],
	terminalStatus TaskStatus,
	result *TaskResultRecord,
	assignment *TaskTerminalAssignmentRecord,
) error {
	if task.Record.Status != terminalStatus ||
		((result == nil) != (task.Record.Result == nil)) ||
		(result != nil && !taskResultsEqual(*result, *task.Record.Result)) ||
		((assignment == nil) != (task.Record.TerminalAssignment == nil)) ||
		(assignment != nil && *assignment != *task.Record.TerminalAssignment) {
		return errs.New(errs.KindStateConflict, "backup Task terminal acknowledgement changed")
	}
	if err := repository.validateTaskRetentionReplay(ctx, task.Record, task.ReadRevision); err != nil {
		return err
	}
	// Rationale: a retained terminal receipt, not compactable MVCC history or
	// perpetual ownership of released domain keys, is the replay authority.
	return repository.validateBackupTerminalReceiptReplay(ctx, task)
}

func backupRunStateForTaskStatus(status TaskStatus) (BackupRunState, error) {
	switch status {
	case TaskStatusCompleted:
		return BackupRunCompleted, nil
	case TaskStatusFailed:
		return BackupRunFailed, nil
	case TaskStatusAborted:
		return BackupRunAborted, nil
	case TaskStatusTimedOut:
		return BackupRunTimedOut, nil
	default:
		return "", errs.New(errs.KindValidationFailed, "backup Task terminal status is invalid")
	}
}

// ExpireTimedOutTasks terminalizes at most 24 overdue assignments per pass.
// Deadline ordering prevents healthy future work from starving older timeouts.
func (repository *TaskRepository) ExpireTimedOutTasks(ctx context.Context, now time.Time) (int, error) {
	if err := validateContext(ctx); err != nil {
		return 0, err
	}
	if err := validateTimestamp("task timeout collector", now); err != nil {
		return 0, err
	}
	page, err := repository.store.Range(ctx, RangeRequest{Prefix: taskTimeoutIndexPrefix, Limit: 24})
	if err != nil {
		return 0, err
	}
	expired := 0
	for _, value := range page.Values {
		taskID, deadline, err := parseTaskTimeoutIndexKey(value.Key)
		if err != nil {
			return expired, err
		}
		assignment, err := decodeTaskAssignment(value.Value)
		if err != nil || assignment.TaskID != taskID || !assignment.Deadline.Equal(deadline) {
			return expired, errs.New(errs.KindInternal, "task timeout index does not match assignment")
		}
		if deadline.After(now) {
			break
		}
		if assignment.Executor == TaskExecutorController {
			_, err = repository.AcknowledgeControllerTask(ctx, taskID, TaskStatusTimedOut, now)
		} else {
			_, err = repository.AcknowledgeTask(
				ctx,
				assignment.AgentID,
				assignment.AgentGeneration,
				taskID,
				assignment.AssignmentID,
				TaskStatusTimedOut,
				TaskResultRecord{
					Kind: TaskResultCompose, Diagnostic: TaskResultDiagnosticNone,
					ReconciliationRequired: true,
				},
				now,
			)
		}
		if err != nil {
			if errors.Is(err, errs.New(errs.KindStateConflict, "")) {
				continue
			}
			return expired, err
		}
		expired++
	}
	return expired, nil
}

func taskResultsEqual(left, right TaskResultRecord) bool {
	if left.Kind != right.Kind || left.ExitCode != right.ExitCode ||
		left.FailedStepID != right.FailedStepID || left.Diagnostic != right.Diagnostic ||
		left.ReconciliationRequired != right.ReconciliationRequired || len(left.Projects) != len(right.Projects) {
		return false
	}
	for index := range left.Projects {
		if left.Projects[index] != right.Projects[index] {
			return false
		}
	}
	return true
}

// AbortPendingTask wins only while the Task is still queued. If assignment
// wins the CAS first, the caller receives a state conflict and must use the
// Agent abort path for the now-running Task.
func (repository *TaskRepository) AbortPendingTask(
	ctx context.Context,
	taskID string,
	terminalAt time.Time,
) (Versioned[TaskRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[TaskRecord]{}, err
	}
	if validateStableID(ids.KindTask, taskID) != nil {
		return Versioned[TaskRecord]{}, errs.New(errs.KindValidationFailed, "task id is invalid")
	}
	if err := validateTimestamp("task terminal_at", terminalAt); err != nil {
		return Versioned[TaskRecord]{}, err
	}

	conflicts := 0
	for {
		current, err := repository.GetTask(ctx, taskID)
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
		if current.Record.Type == TaskBackup || current.Record.Type == TaskBackupPrune {
			return repository.abortPendingBackupTask(ctx, taskID, terminalAt)
		}
		environmentCreation := current.Record.Executor == TaskExecutorAgent && current.Record.Type == TaskCreate &&
			validateStableID(ids.KindEnvironment, current.Record.Target) == nil
		environmentRemoval := current.Record.Executor == TaskExecutorAgent && current.Record.Type == TaskRemove &&
			validateStableID(ids.KindEnvironment, current.Record.Target) == nil
		zoneRemoval := current.Record.Executor == TaskExecutorAgent && current.Record.Type == TaskRemove &&
			validateStableID(ids.KindNetwork, current.Record.Target) == nil
		if current.Record.Status == TaskStatusAborted {
			if environmentCreation {
				if err := repository.validateEnvironmentCreationReplay(
					ctx, current.Record, TaskStatusAborted, current.ReadRevision,
				); err != nil {
					return Versioned[TaskRecord]{}, err
				}
			}
			if environmentRemoval {
				if err := repository.validateEnvironmentRemovalReplay(
					ctx, current.Record, TaskStatusAborted, current.ReadRevision,
				); err != nil {
					return Versioned[TaskRecord]{}, err
				}
			}
			if zoneRemoval {
				if err := repository.validateZoneRemovalReplay(
					ctx, current.Record, TaskStatusAborted, current.ReadRevision,
				); err != nil {
					return Versioned[TaskRecord]{}, err
				}
			}
			if err := repository.validateAttachTaskAcknowledgementReplay(
				ctx, current.Record, TaskStatusAborted, current.ReadRevision,
			); err != nil {
				return Versioned[TaskRecord]{}, err
			}
			if err := repository.validateSecretTaskAcknowledgementReplay(
				ctx, current.Record, TaskStatusAborted, current.ReadRevision,
			); err != nil {
				return Versioned[TaskRecord]{}, err
			}
			if err := repository.validateConnectorTaskAcknowledgementReplay(
				ctx, current.Record, TaskStatusAborted, current.ReadRevision,
			); err != nil {
				return Versioned[TaskRecord]{}, err
			}
			if err := repository.validateRunnerTaskAcknowledgementReplay(
				ctx, current.Record, TaskStatusAborted, current.ReadRevision,
			); err != nil {
				return Versioned[TaskRecord]{}, err
			}
			if err := repository.validateScriptTaskAcknowledgementReplay(
				ctx, current.Record, TaskStatusAborted, current.ReadRevision,
			); err != nil {
				return Versioned[TaskRecord]{}, err
			}
			if err := repository.validateReleaseGroupTaskAcknowledgementReplay(
				ctx, current.Record, TaskStatusAborted, current.ReadRevision,
			); err != nil {
				return Versioned[TaskRecord]{}, err
			}
			if err := repository.validateRemovalTaskAcknowledgementReplay(
				ctx, current.Record, TaskStatusAborted, current.ReadRevision,
			); err != nil {
				return Versioned[TaskRecord]{}, err
			}
			if err := repository.validateBackingZoneTaskAcknowledgementReplay(
				ctx, current.Record, TaskStatusAborted, current.ReadRevision,
			); err != nil {
				return Versioned[TaskRecord]{}, err
			}
			if err := repository.validateComponentTaskAcknowledgementReplay(
				ctx, current.Record, TaskStatusAborted, current.ReadRevision,
			); err != nil {
				return Versioned[TaskRecord]{}, err
			}
			if err := repository.validateBackupKeyRotationTaskAcknowledgementReplay(
				ctx, current.Record, TaskStatusAborted, current.ReadRevision,
			); err != nil {
				return Versioned[TaskRecord]{}, err
			}
			if err := repository.validateTaskRetentionReplay(
				ctx, current.Record, current.ReadRevision,
			); err != nil {
				return Versioned[TaskRecord]{}, err
			}
			return current, nil
		}
		if current.Record.Status != TaskStatusPending {
			return Versioned[TaskRecord]{}, errs.New(
				errs.KindStateConflict,
				"only a pending Task can be aborted before assignment",
			)
		}
		terminal, err := transitionTaskStatus(current.Record, TaskStatusPending, TaskStatusAborted, terminalAt)
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
		terminalAt = *terminal.FinishedAt
		transitionedMarker, markerKey, retentionKey, err := prepareTerminalTaskMarker(
			current.Record,
			TaskStatusAborted,
			terminalAt,
		)
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
		companions, err := repository.store.GetMany(ctx, GetManyRequest{
			Keys: []string{
				taskActiveOperationKey(current.Record.OperationID), markerKey,
				taskQueueKey(current.Record.Executor, taskID), retentionKey,
			},
			Revision: current.ReadRevision,
		})
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
		if len(companions.Values) != 4 || companions.Values[0] == nil || companions.Values[1] == nil ||
			companions.Values[2] == nil || companions.Values[3] != nil {
			return Versioned[TaskRecord]{}, errs.New(
				errs.KindInternal,
				"pending Task lifecycle records are inconsistent",
			)
		}
		queuedTaskID, queueErr := decodeTaskReference(companions.Values[2].Value)
		if queueErr != nil || queuedTaskID != taskID {
			return Versioned[TaskRecord]{}, errs.New(
				errs.KindInternal,
				"pending Task queue record does not match its Task",
			)
		}
		if err := validateTaskLifecycleCompanions(
			current.Record,
			companions.Values[0],
			companions.Values[1],
		); err != nil {
			return Versioned[TaskRecord]{}, err
		}
		transitionedMarker, err = hydrateTerminalTaskMarker(transitionedMarker, companions.Values[1].Value)
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
		terminalValue, err := encodeTaskRecord(terminal)
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
		markerValue, err := encodeIdempotencyMarker(transitionedMarker)
		clear(transitionedMarker.Intent.Ciphertext)
		clear(transitionedMarker.Response.Body)
		if err != nil {
			clear(terminalValue)
			return Versioned[TaskRecord]{}, err
		}
		retentionValue, err := json.Marshal(retentionReferenceJSON{Schema: 1, MarkerKey: markerKey})
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			return Versioned[TaskRecord]{}, errs.Wrap(errs.KindInternal, err)
		}
		taskRetentionKey, taskRetentionValue, err := prepareTaskRetentionIndex(terminal)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			return Versioned[TaskRecord]{}, err
		}
		conditions := []Condition{
			{Key: taskKey(taskID), ModRevision: current.Revision},
			{Key: taskActiveOperationKey(current.Record.OperationID), ModRevision: companions.Values[0].ModRevision},
			{Key: markerKey, ModRevision: companions.Values[1].ModRevision},
			{Key: taskQueueKey(current.Record.Executor, taskID), ModRevision: companions.Values[2].ModRevision},
			{Key: retentionKey},
			{Key: taskRetentionKey},
		}
		mutations := []Mutation{
			{Type: MutationPut, Key: taskKey(taskID), Value: terminalValue},
			{Type: MutationDelete, Key: taskActiveOperationKey(current.Record.OperationID)},
			{Type: MutationDelete, Key: taskQueueKey(current.Record.Executor, taskID)},
			{Type: MutationPut, Key: markerKey, Value: markerValue},
			{Type: MutationPut, Key: retentionKey, Value: retentionValue},
			{Type: MutationPut, Key: taskRetentionKey, Value: taskRetentionValue},
		}
		var environmentValue []byte
		if environmentCreation {
			environmentConditions, environmentMutations, value, prepareErr :=
				repository.prepareEnvironmentCreationAcknowledgement(
					ctx,
					current.Record,
					TaskStatusAborted,
					current.ReadRevision,
				)
			if prepareErr != nil {
				clear(terminalValue)
				clear(markerValue)
				clear(retentionValue)
				clear(taskRetentionValue)
				return Versioned[TaskRecord]{}, prepareErr
			}
			environmentValue = value
			conditions = append(conditions, environmentConditions...)
			mutations = append(mutations, environmentMutations...)
		}
		if environmentRemoval {
			environmentConditions, environmentMutations, prepareErr :=
				repository.prepareEnvironmentRemovalAcknowledgement(
					ctx, current.Record, TaskStatusAborted, current.ReadRevision,
				)
			if prepareErr != nil {
				clear(terminalValue)
				clear(markerValue)
				clear(retentionValue)
				clear(taskRetentionValue)
				clear(environmentValue)
				return Versioned[TaskRecord]{}, prepareErr
			}
			conditions = append(conditions, environmentConditions...)
			mutations = append(mutations, environmentMutations...)
		}
		var zoneMutations []Mutation
		if zoneRemoval {
			zoneConditions, preparedZoneMutations, prepareErr := repository.prepareZoneRemovalAcknowledgement(
				ctx, current.Record, TaskStatusAborted, current.ReadRevision,
			)
			if prepareErr != nil {
				clear(terminalValue)
				clear(markerValue)
				clear(retentionValue)
				clear(taskRetentionValue)
				clear(environmentValue)
				return Versioned[TaskRecord]{}, prepareErr
			}
			zoneMutations = preparedZoneMutations
			conditions = append(conditions, zoneConditions...)
			mutations = append(mutations, zoneMutations...)
		}
		attachChange, err := repository.prepareAttachTaskAcknowledgement(
			ctx, current.Record, TaskStatusAborted, current.ReadRevision,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(taskRetentionValue)
			clear(environmentValue)
			clearMutationValues(zoneMutations)
			return Versioned[TaskRecord]{}, err
		}
		if attachChange.applies {
			conditions = append(conditions, attachChange.conditions...)
			mutations = append(mutations, attachChange.mutations...)
		}
		secretChange, err := repository.prepareSecretTaskAcknowledgement(
			ctx, current.Record, TaskStatusAborted, current.ReadRevision,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(taskRetentionValue)
			clear(environmentValue)
			clearMutationValues(zoneMutations)
			clearAttachTaskChange(attachChange)
			return Versioned[TaskRecord]{}, err
		}
		scriptChange, err := repository.prepareScriptTaskAcknowledgement(
			ctx, current.Record, TaskStatusAborted, current.ReadRevision,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(taskRetentionValue)
			clear(environmentValue)
			clearMutationValues(zoneMutations)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			return Versioned[TaskRecord]{}, err
		}
		releaseGroupChange, err := repository.prepareReleaseGroupTaskAcknowledgement(
			ctx, current.Record, TaskStatusAborted, current.ReadRevision,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(taskRetentionValue)
			clear(environmentValue)
			clearMutationValues(zoneMutations)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			clearScriptTaskChange(scriptChange)
			return Versioned[TaskRecord]{}, err
		}
		defer clearReleaseGroupTaskChange(releaseGroupChange)
		if releaseGroupChange.applies {
			conditions = append(conditions, releaseGroupChange.conditions...)
			mutations = append(mutations, releaseGroupChange.mutations...)
		}
		routeChange, err := repository.prepareRemovalTaskAcknowledgement(
			ctx, current.Record, TaskStatusAborted, terminalAt, current.ReadRevision,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(taskRetentionValue)
			clear(environmentValue)
			clearMutationValues(zoneMutations)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			return Versioned[TaskRecord]{}, err
		}
		serviceChange, err := repository.prepareServiceTaskAcknowledgement(
			ctx, current.Record, current.ReadRevision,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(taskRetentionValue)
			clear(environmentValue)
			clearMutationValues(zoneMutations)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			clearRouteTaskChange(routeChange)
			return Versioned[TaskRecord]{}, err
		}
		backingZoneChange, err := repository.prepareBackingZoneTaskAcknowledgement(
			ctx, current.Record, TaskStatusAborted, current.ReadRevision,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(taskRetentionValue)
			clear(environmentValue)
			clearMutationValues(zoneMutations)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			clearRouteTaskChange(routeChange)
			clearServiceTaskChange(serviceChange)
			return Versioned[TaskRecord]{}, err
		}
		componentChange, err := repository.prepareComponentTaskAcknowledgement(
			ctx, current.Record, TaskStatusAborted, terminalAt, current.ReadRevision,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(taskRetentionValue)
			clear(environmentValue)
			clearMutationValues(zoneMutations)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			clearRouteTaskChange(routeChange)
			clearServiceTaskChange(serviceChange)
			clearBackingZoneTaskChange(backingZoneChange)
			return Versioned[TaskRecord]{}, err
		}
		if secretChange.applies {
			conditions = append(conditions, secretChange.conditions...)
			mutations = append(mutations, secretChange.mutations...)
		}
		if scriptChange.applies {
			conditions = append(conditions, scriptChange.conditions...)
			mutations = append(mutations, scriptChange.mutations...)
		}
		if routeChange.applies {
			conditions = append(conditions, routeChange.conditions...)
			mutations = append(mutations, routeChange.mutations...)
		}
		if serviceChange.applies {
			conditions = append(conditions, serviceChange.conditions...)
			mutations = append(mutations, serviceChange.mutations...)
		}
		if backingZoneChange.applies {
			conditions = append(conditions, backingZoneChange.conditions...)
			mutations = append(mutations, backingZoneChange.mutations...)
		}
		if componentChange.applies {
			conditions = append(conditions, componentChange.conditions...)
			mutations = append(mutations, componentChange.mutations...)
		}
		connectorChange, err := repository.prepareConnectorTaskAcknowledgement(
			ctx, current.Record, TaskStatusAborted, current.ReadRevision,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(taskRetentionValue)
			clear(environmentValue)
			clearMutationValues(zoneMutations)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			clearRouteTaskChange(routeChange)
			clearServiceTaskChange(serviceChange)
			clearBackingZoneTaskChange(backingZoneChange)
			clearComponentTaskChange(componentChange)
			return Versioned[TaskRecord]{}, err
		}
		if connectorChange.applies {
			conditions = append(conditions, connectorChange.conditions...)
			mutations = append(mutations, connectorChange.mutations...)
		}
		runnerChange, err := repository.prepareRunnerTaskAcknowledgement(
			ctx, current.Record, TaskStatusAborted, current.ReadRevision,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(taskRetentionValue)
			clear(environmentValue)
			clearMutationValues(zoneMutations)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			clearRouteTaskChange(routeChange)
			clearServiceTaskChange(serviceChange)
			clearBackingZoneTaskChange(backingZoneChange)
			clearComponentTaskChange(componentChange)
			clearConnectorTaskChange(connectorChange)
			return Versioned[TaskRecord]{}, err
		}
		if runnerChange.applies {
			conditions = append(conditions, runnerChange.conditions...)
			mutations = append(mutations, runnerChange.mutations...)
		}
		rotationChange, err := repository.prepareBackupKeyRotationTaskAcknowledgement(
			ctx, current.Record, TaskStatusAborted, terminalAt, current.ReadRevision,
		)
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
		defer rotationChange.clear()
		conditions = append(conditions, rotationChange.conditions...)
		mutations = append(mutations, rotationChange.mutations...)
		environmentBinding, err := repository.bindOrdinaryTaskEnvironmentMutation(
			ctx,
			current.Record,
			current.ReadRevision,
			conditions,
			mutations,
			attachChange.applies,
			routeChange.applies && current.Record.Params[TaskEntryEnvironmentParam] != "",
			serviceChange.applies,
			connectorChange.applies,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(taskRetentionValue)
			clear(environmentValue)
			clearMutationValues(zoneMutations)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			clearRouteTaskChange(routeChange)
			clearServiceTaskChange(serviceChange)
			clearBackingZoneTaskChange(backingZoneChange)
			clearComponentTaskChange(componentChange)
			clearConnectorTaskChange(connectorChange)
			clearRunnerTaskChange(runnerChange)
			return Versioned[TaskRecord]{}, err
		}
		var environmentEpochValue []byte
		if environmentBinding != nil {
			conditions = environmentBinding.conditions
			mutations = environmentBinding.mutations
			environmentEpochValue = mutations[len(mutations)-1].Value
		}
		transaction, err := repository.store.Transact(ctx, conditions, mutations)
		clear(terminalValue)
		clear(markerValue)
		clear(retentionValue)
		clear(taskRetentionValue)
		clear(environmentValue)
		clearMutationValues(zoneMutations)
		clearAttachTaskChange(attachChange)
		clearSecretTaskChange(secretChange)
		clearRouteTaskChange(routeChange)
		clearServiceTaskChange(serviceChange)
		clearBackingZoneTaskChange(backingZoneChange)
		clearComponentTaskChange(componentChange)
		clearConnectorTaskChange(connectorChange)
		clearRunnerTaskChange(runnerChange)
		clear(environmentEpochValue)
		environmentBinding.clear()
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
		clearKeyValues(transaction.FailureReads)
		if !transaction.Succeeded {
			conflicts++
			if err := repository.retryPolicy.waitAfterConflict(ctx, conflicts); err != nil {
				return Versioned[TaskRecord]{}, err
			}
			continue
		}
		return Versioned[TaskRecord]{
			Record: terminal, Revision: transaction.Revision, ReadRevision: transaction.Revision,
		}, nil
	}
}

func (repository *TaskRepository) abortPendingBackupTask(
	ctx context.Context,
	taskID string,
	terminalAt time.Time,
) (Versioned[TaskRecord], error) {
	runtime, err := newBackupRuntimeRepository(repository.store)
	if err != nil {
		return Versioned[TaskRecord]{}, err
	}
	conflicts := 0
	for {
		current, err := repository.GetTask(ctx, taskID)
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
		if current.Record.Status == TaskStatusAborted {
			if err := repository.validateBackupTaskTerminalReplay(
				ctx, current, TaskStatusAborted, nil, nil,
			); err != nil {
				return Versioned[TaskRecord]{}, err
			}
			return current, nil
		}
		if current.Record.Status != TaskStatusPending ||
			(current.Record.Type != TaskBackup && current.Record.Type != TaskBackupPrune) {
			return Versioned[TaskRecord]{}, errs.New(
				errs.KindStateConflict,
				"only a pending Backup Task can be aborted before assignment",
			)
		}
		effectiveAt, err := nextTaskControllerTimestamp(current.Record.UpdatedAt, terminalAt)
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
		var conditions []Condition
		var mutations []Mutation
		var terminal TaskRecord
		switch current.Record.Type {
		case TaskBackup:
			run, getErr := runtime.GetBackupRun(ctx, taskID)
			if getErr != nil {
				return Versioned[TaskRecord]{}, getErr
			}
			if err := validateBackupRunTaskBinding(current.Record, run.Record); err != nil {
				return Versioned[TaskRecord]{}, err
			}
			effectiveAt = backupTerminalTimestamp(effectiveAt, run.Record.UpdatedAt)
			next, transitionErr := backupRunForTaskTerminal(
				run.Record, TaskStatusAborted, effectiveAt,
			)
			if transitionErr != nil {
				return Versioned[TaskRecord]{}, transitionErr
			}
			taskPlan, prepareErr := repository.preparePendingBackupTaskTerminal(
				ctx, current, effectiveAt,
			)
			if prepareErr != nil {
				return Versioned[TaskRecord]{}, prepareErr
			}
			runPlan, prepareErr := runtime.prepareBackupRunTerminal(ctx, run, next)
			if prepareErr != nil {
				taskPlan.clear()
				return Versioned[TaskRecord]{}, prepareErr
			}
			receiptPlan, prepareErr := prepareBackupRunTerminalReceipt(
				current, taskPlan.record, runPlan.record,
			)
			if prepareErr != nil {
				taskPlan.clear()
				runPlan.clear()
				return Versioned[TaskRecord]{}, prepareErr
			}
			terminal = taskPlan.record
			conditions, mutations, err = composeBackupRunTerminalTransaction(
				taskPlan, runPlan, receiptPlan,
			)
			taskPlan.clear()
			runPlan.clear()
			receiptPlan.clear()
		case TaskBackupPrune:
			dispatch, prunes, loadErr := repository.loadBackupPruneTerminalAuthority(
				ctx, current.Record,
			)
			if loadErr != nil {
				return Versioned[TaskRecord]{}, loadErr
			}
			effectiveAt = backupTerminalTimestamp(effectiveAt, dispatch.Record.CreatedAt)
			for _, prune := range prunes {
				effectiveAt = backupTerminalTimestamp(effectiveAt, prune.Record.UpdatedAt)
			}
			taskPlan, prepareErr := repository.preparePendingBackupTaskTerminal(
				ctx, current, effectiveAt,
			)
			if prepareErr != nil {
				return Versioned[TaskRecord]{}, prepareErr
			}
			prunePlan, prepareErr := runtime.prepareBackupPruneFailure(
				ctx, dispatch, prunes, effectiveAt,
			)
			if prepareErr != nil {
				taskPlan.clear()
				return Versioned[TaskRecord]{}, prepareErr
			}
			receiptPlan, prepareErr := prepareBackupPruneTerminalReceipt(
				current, taskPlan.record, dispatch.Record, prunes,
			)
			if prepareErr != nil {
				taskPlan.clear()
				prunePlan.clear()
				return Versioned[TaskRecord]{}, prepareErr
			}
			terminal = taskPlan.record
			conditions, mutations, err = composeBackupPruneTerminalTransaction(
				taskPlan, prunePlan, receiptPlan,
			)
			taskPlan.clear()
			prunePlan.clear()
			receiptPlan.clear()
		}
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
		transaction, err := runtime.transact(ctx, conditions, mutations)
		clearBackupRuntimeMutations(mutations)
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
		clearKeyValues(transaction.FailureReads)
		if !transaction.Succeeded {
			conflicts++
			if err := repository.retryPolicy.waitAfterConflict(ctx, conflicts); err != nil {
				return Versioned[TaskRecord]{}, err
			}
			continue
		}
		return Versioned[TaskRecord]{
			Record: terminal, Revision: transaction.Revision, ReadRevision: transaction.Revision,
		}, nil
	}
}

func (repository *TaskRepository) preparePendingBackupTaskTerminal(
	ctx context.Context,
	current Versioned[TaskRecord],
	terminalAt time.Time,
) (backupTaskTerminalPlan, error) {
	task := current.Record
	if current.Revision <= 0 || current.ReadRevision <= 0 ||
		(task.Type != TaskBackup && task.Type != TaskBackupPrune) ||
		task.Executor != TaskExecutorAgent || task.Status != TaskStatusPending {
		return backupTaskTerminalPlan{}, errs.New(
			errs.KindValidationFailed,
			"pending Backup Task terminal identity is invalid",
		)
	}
	terminal, err := transitionTaskStatus(task, TaskStatusPending, TaskStatusAborted, terminalAt)
	if err != nil {
		return backupTaskTerminalPlan{}, err
	}
	terminalAt = *terminal.FinishedAt
	transitionedMarker, markerKey, retentionKey, err := prepareTerminalTaskMarker(
		task, TaskStatusAborted, terminalAt,
	)
	if err != nil {
		return backupTaskTerminalPlan{}, err
	}
	taskRetentionKey, taskRetentionValue, err := prepareTaskRetentionIndex(terminal)
	if err != nil {
		return backupTaskTerminalPlan{}, err
	}
	defer clear(taskRetentionValue)
	keys := []string{
		taskActiveOperationKey(task.OperationID), markerKey,
		taskQueueKey(task.Executor, task.ID), retentionKey, taskRetentionKey,
	}
	companions, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: keys, Revision: current.ReadRevision,
	})
	if err != nil {
		return backupTaskTerminalPlan{}, err
	}
	if companions == nil || companions.ReadRevision != current.ReadRevision ||
		len(companions.Values) != len(keys) || companions.Values[0] == nil ||
		companions.Values[1] == nil || companions.Values[2] == nil ||
		companions.Values[3] != nil || companions.Values[4] != nil {
		return backupTaskTerminalPlan{}, errs.New(
			errs.KindInternal,
			"pending Backup Task lifecycle records are inconsistent",
		)
	}
	defer clearKeyValues(companions.Values)
	queuedTaskID, err := decodeTaskReference(companions.Values[2].Value)
	if err != nil || queuedTaskID != task.ID ||
		validateTaskLifecycleCompanions(task, companions.Values[0], companions.Values[1]) != nil {
		return backupTaskTerminalPlan{}, errs.New(
			errs.KindInternal,
			"pending Backup Task companions do not match",
		)
	}
	transitionedMarker, err = hydrateTerminalTaskMarker(
		transitionedMarker, companions.Values[1].Value,
	)
	if err != nil {
		return backupTaskTerminalPlan{}, err
	}
	terminalValue, err := encodeTaskRecord(terminal)
	if err != nil {
		return backupTaskTerminalPlan{}, err
	}
	markerValue, err := encodeIdempotencyMarker(transitionedMarker)
	clear(transitionedMarker.Intent.Ciphertext)
	clear(transitionedMarker.Response.Body)
	if err != nil {
		clear(terminalValue)
		return backupTaskTerminalPlan{}, err
	}
	retentionValue, err := json.Marshal(retentionReferenceJSON{Schema: 1, MarkerKey: markerKey})
	if err != nil {
		clear(terminalValue)
		clear(markerValue)
		return backupTaskTerminalPlan{}, errs.Wrap(errs.KindInternal, err)
	}
	conditions := []Condition{
		{Key: taskKey(task.ID), ModRevision: current.Revision},
		{Key: keys[0], ModRevision: companions.Values[0].ModRevision},
		{Key: keys[1], ModRevision: companions.Values[1].ModRevision},
		{Key: keys[2], ModRevision: companions.Values[2].ModRevision},
		{Key: keys[3]}, {Key: keys[4]},
	}
	mutations := []Mutation{
		{Type: MutationPut, Key: taskKey(task.ID), Value: terminalValue},
		{Type: MutationDelete, Key: keys[0]},
		{Type: MutationPut, Key: keys[1], Value: markerValue},
		{Type: MutationDelete, Key: keys[2]},
		{Type: MutationPut, Key: keys[3], Value: retentionValue},
		{Type: MutationPut, Key: keys[4], Value: append([]byte(nil), taskRetentionValue...)},
	}
	return backupTaskTerminalPlan{conditions: conditions, mutations: mutations, record: terminal}, nil
}

func (repository *TaskRepository) bindOrdinaryTaskEnvironmentMutation(
	ctx context.Context,
	task TaskRecord,
	readRevision int64,
	conditions []Condition,
	mutations []Mutation,
	attachChange bool,
	entryChange bool,
	serviceChange bool,
	connectorChange bool,
) (*ordinaryEnvironmentMutationBinding, error) {
	environmentID, applies, err := ordinaryTaskEnvironmentMutationTarget(
		task,
		attachChange,
		entryChange,
		serviceChange,
		connectorChange,
	)
	if err != nil || !applies {
		return nil, err
	}
	fence, err := loadOrdinaryEnvironmentMutationFence(ctx, repository.store, environmentID, readRevision)
	if err != nil {
		return nil, err
	}
	mutationContext := ordinaryEnvironmentMutationContext{
		environmentID: environmentID,
		readRevision:  readRevision,
		fence:         fence,
	}
	return mutationContext.bind(ctx, repository.store, conditions, mutations, true)
}

func ordinaryTaskEnvironmentMutationTarget(
	task TaskRecord,
	attachChange bool,
	entryChange bool,
	serviceChange bool,
	connectorChange bool,
) (string, bool, error) {
	targets := make([]string, 0, 1)
	if attachChange {
		targets = append(targets, task.Params[TaskMutationEnvironmentParam])
	}
	if entryChange {
		targets = append(targets, task.Params[TaskEntryEnvironmentParam])
	}
	if serviceChange {
		targets = append(targets, task.Params[TaskServiceEnvironmentParam])
	}
	if connectorChange {
		targets = append(targets, task.Params[TaskConnectorEnvironmentParam])
	}
	if len(targets) == 0 {
		return "", false, nil
	}
	if len(targets) != 1 {
		return "", false, errs.New(errs.KindInternal, "task has conflicting environment mutation targets")
	}
	environmentID := targets[0]
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || task.Owner.EnvironmentID != environmentID {
		return "", false, errs.New(errs.KindInternal, "task environment mutation ownership is corrupt")
	}
	return environmentID, true, nil
}

func prepareTerminalTaskMarker(
	task TaskRecord,
	status TaskStatus,
	terminalAt time.Time,
) (IdempotencyMarker, string, string, error) {
	if task.idempotencyMarker == nil {
		return IdempotencyMarker{}, "", "", errs.New(
			errs.KindInternal,
			"task is missing its idempotency marker locator",
		)
	}
	markerKey, err := idempotencyMarkerKey(*task.idempotencyMarker)
	if err != nil {
		return IdempotencyMarker{}, "", "", errs.New(errs.KindInternal, "task idempotency marker locator is corrupt")
	}
	state := IdempotencyMarkerFailed
	if status == TaskStatusCompleted {
		state = IdempotencyMarkerCompleted
	}
	marker := IdempotencyMarker{
		Kind: IdempotencyMarkerTask, State: state, Locator: *task.idempotencyMarker,
		TaskID: task.ID, UpdatedAt: terminalAt, TerminalAt: terminalAt,
		RetainUntil: terminalAt.Add(markerRetention),
	}
	retentionKey, err := idempotencyRetentionKey(markerKey, marker.RetainUntil)
	if err != nil {
		return IdempotencyMarker{}, "", "", err
	}
	return marker, markerKey, retentionKey, nil
}
