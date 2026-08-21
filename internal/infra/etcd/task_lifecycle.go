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
	TaskID              string
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
	Schema              int    `json:"schema"`
	TaskID              string `json:"task_id"`
	AgentID             string `json:"agent_id"`
	AgentGeneration     uint64 `json:"agent_generation"`
	ClaimedTaskRevision int64  `json:"claimed_task_revision"`
	AssignedAt          string `json:"assigned_at"`
	Deadline            string `json:"deadline"`
}

func encodeTaskAssignment(record TaskAssignmentRecord) ([]byte, error) {
	if err := validateTaskAssignment(record); err != nil {
		return nil, err
	}
	return json.Marshal(taskAssignmentJSON{
		Schema: 1, TaskID: record.TaskID, AgentID: record.AgentID,
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
		TaskID: data.TaskID, AgentID: data.AgentID, AgentGeneration: data.AgentGeneration,
		ClaimedTaskRevision: data.ClaimedTaskRevision, AssignedAt: assignedAt, Deadline: deadline,
	}
	if err := validateTaskAssignment(record); err != nil {
		return TaskAssignmentRecord{}, corruptTaskAssignment()
	}
	return record, nil
}

func validateTaskAssignment(record TaskAssignmentRecord) error {
	if validateStableID(ids.KindTask, record.TaskID) != nil ||
		validateStableID(ids.KindAgent, record.AgentID) != nil ||
		record.AgentGeneration == 0 || record.ClaimedTaskRevision <= 0 ||
		validateTimestamp("task assignment assigned_at", record.AssignedAt) != nil ||
		validateTimestamp("task assignment deadline", record.Deadline) != nil ||
		!record.Deadline.After(record.AssignedAt) {
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
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "Task creation marker does not match its Task")
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
		{Key: taskQueueKey(record.ID)},
	}
	mutations := []Mutation{
		{Type: MutationPut, Key: taskKey(record.ID), Value: taskValue},
		{Type: MutationPut, Key: taskOperationIndexKey(record.OperationID, record.ID), Value: reference},
		{Type: MutationPut, Key: taskActiveOperationKey(record.OperationID), Value: reference},
		{Type: MutationPut, Key: taskQueueKey(record.ID), Value: reference},
	}
	plan, err := newTaskIdempotencyMutationPlan(
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
			return errs.New(errs.KindInternal, "Task creation compare evidence is incomplete")
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
				return errs.New(errs.KindInternal, "Task creation collided with durable Task state")
			}
		}
		return errs.New(errs.KindInternal, "Task creation compare failure was not classified")
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
	retry, err := cloneRetryTask(source.Record, retryTaskID, marker.CreatedAt)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if marker.Kind != IdempotencyMarkerTask || marker.State != IdempotencyMarkerPending ||
		marker.TaskID != retry.ID || !marker.CreatedAt.Equal(retry.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Task retry marker does not match its Task",
		)
	}
	if err := validateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	retry.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	if err := validateTaskRecord(retry); err != nil {
		return IdempotencyTransactionResult{}, err
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
		{Key: taskQueueKey(retry.ID)},
	}
	mutations := []Mutation{
		{Type: MutationPut, Key: taskKey(retry.ID), Value: taskValue},
		{Type: MutationPut, Key: taskOperationIndexKey(retry.OperationID, retry.ID), Value: reference},
		{Type: MutationPut, Key: taskActiveOperationKey(retry.OperationID), Value: reference},
		{Type: MutationPut, Key: taskQueueKey(retry.ID), Value: reference},
	}
	plan, err := newTaskIdempotencyMutationPlan(
		conditions,
		mutations,
		classifyTaskRetryConflict(sourceTaskID, retry.OperationID),
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

func classifyTaskRetryConflict(sourceTaskID string, operationID string) idempotencyPlanClassifier {
	return func(_ int64, values []*KeyValue) error {
		if len(values) != 5 {
			return errs.New(errs.KindInternal, "Task retry compare evidence is incomplete")
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
			return errs.New(errs.KindInternal, "Task retry collided with durable Task state")
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
	if err := validateContext(ctx); err != nil {
		return TaskAssignment{}, false, err
	}
	if validateStableID(ids.KindAgent, agentID) != nil || agentGeneration == 0 {
		return TaskAssignment{}, false, errs.New(errs.KindValidationFailed, "Agent assignment identity is invalid")
	}
	if err := validateTimestamp("task assignment assigned_at", assignedAt); err != nil {
		return TaskAssignment{}, false, err
	}

	conflicts := 0
	for {
		queue, err := repository.store.Range(ctx, RangeRequest{Prefix: taskQueuePrefix, Limit: 1})
		if err != nil {
			return TaskAssignment{}, false, err
		}
		if len(queue.Values) == 0 {
			return TaskAssignment{}, false, nil
		}
		queued := queue.Values[0]
		taskID, err := taskIDFromQueueKey(queued.Key)
		if err != nil {
			return TaskAssignment{}, false, err
		}
		referencedTaskID, err := decodeTaskReference(queued.Value)
		if err != nil || referencedTaskID != taskID {
			return TaskAssignment{}, false, errs.New(errs.KindInternal, "task queue record does not match its key")
		}
		taskRead, err := repository.store.GetMany(ctx, GetManyRequest{
			Keys: []string{taskKey(taskID)}, Revision: queue.ReadRevision,
		})
		if err != nil {
			return TaskAssignment{}, false, err
		}
		if len(taskRead.Values) != 1 || taskRead.Values[0] == nil {
			return TaskAssignment{}, false, errs.New(errs.KindInternal, "queued Task primary is missing")
		}
		taskValue := taskRead.Values[0]
		task, err := decodeTaskRecord(taskValue.Value)
		if err != nil {
			return TaskAssignment{}, false, err
		}
		if task.ID != taskID || task.Status != TaskStatusPending || task.idempotencyMarker == nil {
			return TaskAssignment{}, false, errs.New(errs.KindInternal, "queued Task is not a claimable pending Task")
		}
		activeKey := taskActiveOperationKey(task.OperationID)
		assignmentKey := taskAssignmentKey(agentID, task.ID)
		assignmentIndexKey := taskAssignmentIndexKey(task.ID)
		deadline := assignedAt.Add(time.Duration(task.TimeoutSeconds) * time.Second)
		timeoutIndexKey := taskTimeoutIndexKey(task.ID, deadline)
		companions, err := repository.store.GetMany(ctx, GetManyRequest{
			Keys:     []string{activeKey, assignmentKey, assignmentIndexKey, timeoutIndexKey},
			Revision: queue.ReadRevision,
		})
		if err != nil {
			return TaskAssignment{}, false, err
		}
		if len(companions.Values) != 4 || companions.Values[0] == nil || companions.Values[1] != nil ||
			companions.Values[2] != nil || companions.Values[3] != nil {
			return TaskAssignment{}, false, errs.New(errs.KindInternal, "queued Task lifecycle records are inconsistent")
		}
		activeTaskID, err := decodeTaskReference(companions.Values[0].Value)
		if err != nil || activeTaskID != task.ID {
			return TaskAssignment{}, false, errs.New(errs.KindInternal, "active-operation record does not match queued Task")
		}
		running, err := transitionTaskStatus(task, TaskStatusPending, TaskStatusRunning, assignedAt)
		if err != nil {
			return TaskAssignment{}, false, err
		}
		assignment := TaskAssignmentRecord{
			TaskID: task.ID, AgentID: agentID, AgentGeneration: agentGeneration,
			ClaimedTaskRevision: taskValue.ModRevision, AssignedAt: assignedAt, Deadline: deadline,
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
		transaction, err := repository.store.Transact(ctx, []Condition{
			{Key: queued.Key, ModRevision: queued.ModRevision},
			{Key: taskKey(task.ID), ModRevision: taskValue.ModRevision},
			{Key: activeKey, ModRevision: companions.Values[0].ModRevision},
			{Key: assignmentKey},
			{Key: assignmentIndexKey},
			{Key: timeoutIndexKey},
		}, []Mutation{
			{Type: MutationPut, Key: taskKey(task.ID), Value: runningValue},
			{Type: MutationDelete, Key: queued.Key},
			{Type: MutationPut, Key: assignmentKey, Value: assignmentValue},
			{Type: MutationPut, Key: assignmentIndexKey, Value: assignmentValue},
			{Type: MutationPut, Key: timeoutIndexKey, Value: assignmentValue},
		})
		clear(runningValue)
		clear(assignmentValue)
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
		return nil, errs.New(errs.KindValidationFailed, "Agent assignment query is invalid")
	}
	assignments, err := repository.store.Range(ctx, RangeRequest{
		Prefix: taskAssignmentScopePrefix(agentID),
		Limit:  int64(maximum) + 1,
	})
	if err != nil {
		return nil, err
	}
	if assignments.More || len(assignments.Values) > int(maximum) {
		return nil, errs.New(errs.KindInternal, "Agent assignments exceed configured concurrency")
	}
	if len(assignments.Values) == 0 {
		return []TaskAssignment{}, nil
	}

	records := make([]TaskAssignmentRecord, len(assignments.Values))
	taskKeys := make([]string, len(assignments.Values))
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
		taskKeys[index] = taskKey(taskID)
	}
	tasks, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: taskKeys, Revision: assignments.ReadRevision,
	})
	if err != nil {
		return nil, err
	}
	if len(tasks.Values) != len(records) {
		return nil, errs.New(errs.KindInternal, "Agent assignment Task read is incomplete")
	}
	result := make([]TaskAssignment, len(records))
	for index, record := range records {
		taskValue := tasks.Values[index]
		if taskValue == nil {
			return nil, errs.New(errs.KindInternal, "assigned Task primary is missing")
		}
		task, err := decodeTaskRecord(taskValue.Value)
		if err != nil {
			return nil, err
		}
		assignmentValue := assignments.Values[index]
		if task.ID != record.TaskID || task.Status != TaskStatusRunning ||
			task.StartedAt == nil || !task.StartedAt.Equal(record.AssignedAt) ||
			!record.Deadline.Equal(record.AssignedAt.Add(time.Duration(task.TimeoutSeconds)*time.Second)) ||
			taskValue.ModRevision < assignmentValue.ModRevision || task.idempotencyMarker == nil {
			return nil, errs.New(errs.KindInternal, "durable Task assignment and Task are inconsistent")
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
// removes assignment and active-operation state, and terminalizes replay
// evidence. Replaying the same terminal acknowledgement is idempotent.
func (repository *TaskRepository) AcknowledgeTask(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	taskID string,
	terminalStatus TaskStatus,
	result TaskResultRecord,
	terminalAt time.Time,
) (Versioned[TaskRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[TaskRecord]{}, err
	}
	if validateStableID(ids.KindAgent, agentID) != nil || agentGeneration == 0 ||
		validateStableID(ids.KindTask, taskID) != nil || !isTerminalTaskStatus(terminalStatus) {
		return Versioned[TaskRecord]{}, errs.New(errs.KindValidationFailed, "Task acknowledgement is invalid")
	}
	if err := validateTimestamp("task terminal_at", terminalAt); err != nil {
		return Versioned[TaskRecord]{}, err
	}

	conflicts := 0
	for {
		primaryAndAssignment, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
			taskKey(taskID), taskAssignmentKey(agentID, taskID), taskAssignmentIndexKey(taskID),
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
		assignmentValue := primaryAndAssignment.Values[1]
		assignmentIndexValue := primaryAndAssignment.Values[2]
		if err := validateTaskResult(result, task.Steps, terminalStatus); err != nil {
			return Versioned[TaskRecord]{}, err
		}
		if assignmentValue == nil {
			if assignmentIndexValue != nil {
				return Versioned[TaskRecord]{}, errs.New(errs.KindInternal, "Task assignment index is orphaned")
			}
			if task.Status == terminalStatus && task.Result != nil && taskResultsEqual(*task.Result, result) {
				return Versioned[TaskRecord]{
					Record: task, Revision: taskValue.ModRevision,
					ReadRevision: primaryAndAssignment.ReadRevision,
				}, nil
			}
			return Versioned[TaskRecord]{}, errs.New(errs.KindStateConflict, "Task has no matching active assignment")
		}
		assignment, err := decodeTaskAssignment(assignmentValue.Value)
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
		if assignmentIndexValue == nil || !bytes.Equal(assignmentIndexValue.Value, assignmentValue.Value) {
			return Versioned[TaskRecord]{}, errs.New(errs.KindInternal, "Task assignment index does not match assignment")
		}
		if assignment.TaskID != task.ID || assignment.AgentID != agentID ||
			assignment.AgentGeneration != agentGeneration ||
			assignment.ClaimedTaskRevision >= assignmentValue.ModRevision || task.StartedAt == nil ||
			!assignment.AssignedAt.Equal(*task.StartedAt) {
			return Versioned[TaskRecord]{}, errs.New(errs.KindStateConflict, "Task assignment does not match the Agent generation")
		}
		terminal, err := transitionTaskStatus(task, TaskStatusRunning, terminalStatus, terminalAt)
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
		terminal.Result = cloneTaskResult(&result)
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
		companions, err := repository.store.GetMany(ctx, GetManyRequest{
			Keys: []string{
				taskActiveOperationKey(task.OperationID), markerKey, taskQueueKey(task.ID), retentionKey,
				taskTimeoutIndexKey(task.ID, assignment.Deadline),
			},
			Revision: primaryAndAssignment.ReadRevision,
		})
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
		if len(companions.Values) != 5 || companions.Values[0] == nil || companions.Values[1] == nil ||
			companions.Values[2] != nil || companions.Values[3] != nil || companions.Values[4] == nil ||
			!bytes.Equal(companions.Values[4].Value, assignmentValue.Value) {
			return Versioned[TaskRecord]{}, errs.New(errs.KindInternal, "running Task lifecycle records are inconsistent")
		}
		if err := validateTaskLifecycleCompanions(task, companions.Values[0], companions.Values[1]); err != nil {
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
		transaction, err := repository.store.Transact(ctx, []Condition{
			{Key: taskKey(task.ID), ModRevision: taskValue.ModRevision},
			{Key: taskAssignmentKey(agentID, task.ID), ModRevision: assignmentValue.ModRevision},
			{Key: taskAssignmentIndexKey(task.ID), ModRevision: assignmentIndexValue.ModRevision},
			{Key: taskActiveOperationKey(task.OperationID), ModRevision: companions.Values[0].ModRevision},
			{Key: markerKey, ModRevision: companions.Values[1].ModRevision},
			{Key: taskQueueKey(task.ID)},
			{Key: retentionKey},
			{Key: taskTimeoutIndexKey(task.ID, assignment.Deadline), ModRevision: companions.Values[4].ModRevision},
		}, []Mutation{
			{Type: MutationPut, Key: taskKey(task.ID), Value: terminalValue},
			{Type: MutationDelete, Key: taskAssignmentKey(agentID, task.ID)},
			{Type: MutationDelete, Key: taskAssignmentIndexKey(task.ID)},
			{Type: MutationDelete, Key: taskActiveOperationKey(task.OperationID)},
			{Type: MutationPut, Key: markerKey, Value: markerValue},
			{Type: MutationPut, Key: retentionKey, Value: retentionValue},
			{Type: MutationDelete, Key: taskTimeoutIndexKey(task.ID, assignment.Deadline)},
		})
		clear(terminalValue)
		clear(markerValue)
		clear(retentionValue)
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
		_, err = repository.AcknowledgeTask(
			ctx,
			assignment.AgentID,
			assignment.AgentGeneration,
			taskID,
			TaskStatusTimedOut,
			TaskResultRecord{
				Kind: TaskResultCompose, Diagnostic: TaskResultDiagnosticNone,
				ReconciliationRequired: true,
			},
			now,
		)
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
		return Versioned[TaskRecord]{}, errs.New(errs.KindValidationFailed, "Task id is invalid")
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
		if current.Record.Status == TaskStatusAborted {
			return current, nil
		}
		if current.Record.Status != TaskStatusPending {
			return Versioned[TaskRecord]{}, errs.New(errs.KindStateConflict, "only a pending Task can be aborted before assignment")
		}
		terminal, err := transitionTaskStatus(current.Record, TaskStatusPending, TaskStatusAborted, terminalAt)
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
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
				taskQueueKey(taskID), retentionKey,
			},
			Revision: current.ReadRevision,
		})
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
		if len(companions.Values) != 4 || companions.Values[0] == nil || companions.Values[1] == nil ||
			companions.Values[2] == nil || companions.Values[3] != nil {
			return Versioned[TaskRecord]{}, errs.New(errs.KindInternal, "pending Task lifecycle records are inconsistent")
		}
		queuedTaskID, queueErr := decodeTaskReference(companions.Values[2].Value)
		if queueErr != nil || queuedTaskID != taskID {
			return Versioned[TaskRecord]{}, errs.New(errs.KindInternal, "pending Task queue record does not match its Task")
		}
		if err := validateTaskLifecycleCompanions(current.Record, companions.Values[0], companions.Values[1]); err != nil {
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
		transaction, err := repository.store.Transact(ctx, []Condition{
			{Key: taskKey(taskID), ModRevision: current.Revision},
			{Key: taskActiveOperationKey(current.Record.OperationID), ModRevision: companions.Values[0].ModRevision},
			{Key: markerKey, ModRevision: companions.Values[1].ModRevision},
			{Key: taskQueueKey(taskID), ModRevision: companions.Values[2].ModRevision},
			{Key: retentionKey},
		}, []Mutation{
			{Type: MutationPut, Key: taskKey(taskID), Value: terminalValue},
			{Type: MutationDelete, Key: taskActiveOperationKey(current.Record.OperationID)},
			{Type: MutationDelete, Key: taskQueueKey(taskID)},
			{Type: MutationPut, Key: markerKey, Value: markerValue},
			{Type: MutationPut, Key: retentionKey, Value: retentionValue},
		})
		clear(terminalValue)
		clear(markerValue)
		clear(retentionValue)
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

func prepareTerminalTaskMarker(
	task TaskRecord,
	status TaskStatus,
	terminalAt time.Time,
) (IdempotencyMarker, string, string, error) {
	if task.idempotencyMarker == nil {
		return IdempotencyMarker{}, "", "", errs.New(errs.KindInternal, "Task is missing its idempotency marker locator")
	}
	markerKey, err := idempotencyMarkerKey(*task.idempotencyMarker)
	if err != nil {
		return IdempotencyMarker{}, "", "", errs.New(errs.KindInternal, "Task idempotency marker locator is corrupt")
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

func validateTaskLifecycleCompanions(task TaskRecord, activeValue *KeyValue, markerValue *KeyValue) error {
	activeTaskID, err := decodeTaskReference(activeValue.Value)
	if err != nil || activeTaskID != task.ID {
		return errs.New(errs.KindInternal, "active-operation record does not match its Task")
	}
	marker, err := decodeIdempotencyMarker(markerValue.Value, *task.idempotencyMarker)
	if err != nil {
		return err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	if marker.Kind != IdempotencyMarkerTask || marker.State != IdempotencyMarkerPending ||
		marker.TaskID != task.ID {
		return errs.New(errs.KindInternal, "Task idempotency marker is not pending for its Task")
	}
	return nil
}

func hydrateTerminalTaskMarker(
	prepared IdempotencyMarker,
	persisted []byte,
) (IdempotencyMarker, error) {
	existing, err := decodeIdempotencyMarker(persisted, prepared.Locator)
	if err != nil {
		return IdempotencyMarker{}, err
	}
	prepared.Intent = existing.Intent
	prepared.Response = existing.Response
	prepared.CreatedAt = existing.CreatedAt
	return prepared, nil
}
