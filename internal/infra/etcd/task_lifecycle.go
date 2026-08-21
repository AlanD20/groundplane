package etcd

import (
	"bytes"
	"context"
	"encoding/json"
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
}

func encodeTaskAssignment(record TaskAssignmentRecord) ([]byte, error) {
	if err := validateTaskAssignment(record); err != nil {
		return nil, err
	}
	return json.Marshal(taskAssignmentJSON{
		Schema: 1, TaskID: record.TaskID, AgentID: record.AgentID,
		AgentGeneration: record.AgentGeneration, ClaimedTaskRevision: record.ClaimedTaskRevision,
		AssignedAt: record.AssignedAt.Format(time.RFC3339Nano),
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
	record := TaskAssignmentRecord{
		TaskID: data.TaskID, AgentID: data.AgentID, AgentGeneration: data.AgentGeneration,
		ClaimedTaskRevision: data.ClaimedTaskRevision, AssignedAt: assignedAt,
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
		validateTimestamp("task assignment assigned_at", record.AssignedAt) != nil {
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
	record.IdempotencyKey = marker.Locator.Key
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
		companions, err := repository.store.GetMany(ctx, GetManyRequest{
			Keys: []string{activeKey, assignmentKey}, Revision: queue.ReadRevision,
		})
		if err != nil {
			return TaskAssignment{}, false, err
		}
		if len(companions.Values) != 2 || companions.Values[0] == nil || companions.Values[1] != nil {
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
			ClaimedTaskRevision: taskValue.ModRevision, AssignedAt: assignedAt,
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
		}, []Mutation{
			{Type: MutationPut, Key: taskKey(task.ID), Value: runningValue},
			{Type: MutationDelete, Key: queued.Key},
			{Type: MutationPut, Key: assignmentKey, Value: assignmentValue},
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

// AcknowledgeTask atomically records the Agent's terminal acknowledgement,
// removes assignment and active-operation state, and terminalizes replay
// evidence. Replaying the same terminal acknowledgement is idempotent.
func (repository *TaskRepository) AcknowledgeTask(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	taskID string,
	terminalStatus TaskStatus,
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
			taskKey(taskID), taskAssignmentKey(agentID, taskID),
		}})
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
		if len(primaryAndAssignment.Values) != 2 || primaryAndAssignment.Values[0] == nil {
			return Versioned[TaskRecord]{}, errs.Newf(errs.KindTaskNotFound, "task not found: %s", taskID)
		}
		taskValue := primaryAndAssignment.Values[0]
		task, err := decodeTaskRecord(taskValue.Value)
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
		assignmentValue := primaryAndAssignment.Values[1]
		if assignmentValue == nil {
			if task.Status == terminalStatus {
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
			},
			Revision: primaryAndAssignment.ReadRevision,
		})
		if err != nil {
			return Versioned[TaskRecord]{}, err
		}
		if len(companions.Values) != 4 || companions.Values[0] == nil || companions.Values[1] == nil ||
			companions.Values[2] != nil || companions.Values[3] != nil {
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
			{Key: taskActiveOperationKey(task.OperationID), ModRevision: companions.Values[0].ModRevision},
			{Key: markerKey, ModRevision: companions.Values[1].ModRevision},
			{Key: taskQueueKey(task.ID)},
			{Key: retentionKey},
		}, []Mutation{
			{Type: MutationPut, Key: taskKey(task.ID), Value: terminalValue},
			{Type: MutationDelete, Key: taskAssignmentKey(agentID, task.ID)},
			{Type: MutationDelete, Key: taskActiveOperationKey(task.OperationID)},
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
