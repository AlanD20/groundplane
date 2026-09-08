package etcd

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// AppendTaskEvent persists the event, updated Task summary, and deduplication
// record in one CAS transaction. It never converts a transaction error into
// success by inspecting later key state: that requires the separate durable
// idempotency evidence that ADR 0013 still gates.
func (repository *TaskRepository) AppendTaskEvent(
	ctx context.Context,
	input TaskEventInput,
	receivedAt time.Time,
) (TaskEventAppend, error) {
	if err := validateContext(ctx); err != nil {
		return TaskEventAppend{}, err
	}
	if err := validateTaskEventIdentity(input.Identity); err != nil {
		return TaskEventAppend{}, err
	}

	conflicts := 0
	for {
		if err := ctx.Err(); err != nil {
			return TaskEventAppend{}, err
		}
		claimKey := taskAssignmentKey(input.Identity.AgentID, input.Identity.TaskID)
		result, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
			taskKey(input.Identity.TaskID), taskEventDedupKey(input.Identity), claimKey,
			taskAssignmentIndexKey(input.Identity.TaskID), blueprintClosingReportKey(input.Identity.TaskID),
		}})
		if err != nil {
			return TaskEventAppend{}, err
		}
		if len(result.Values) != 5 {
			return TaskEventAppend{}, errs.New(errs.KindInternal, "task event read returned an invalid record count")
		}
		taskValue := result.Values[0]
		if taskValue == nil {
			return TaskEventAppend{}, errs.Newf(
				errs.KindTaskNotFound,
				"task not found: %s",
				input.Identity.TaskID,
			)
		}
		task, err := decodeTaskRecord(taskValue.Value)
		if err != nil {
			return TaskEventAppend{}, err
		}
		if task.ID != input.Identity.TaskID || taskValue.Key != taskKey(task.ID) {
			return TaskEventAppend{}, errs.New(errs.KindInternal, "task event read returned a mismatched task")
		}

		var existing *TaskEventDedupRecord
		if result.Values[1] != nil {
			dedup, err := decodeTaskEventDedupRecord(result.Values[1].Value)
			if err != nil {
				return TaskEventAppend{}, err
			}
			existing = &dedup
		}
		prepared, err := prepareTaskEvent(task, input, existing, receivedAt)
		if err != nil {
			return TaskEventAppend{}, err
		}
		assignmentValue := result.Values[2]
		assignmentIndexValue := result.Values[3]
		if task.Status != TaskStatusRunning || assignmentValue == nil || assignmentIndexValue == nil {
			return TaskEventAppend{}, errs.New(errs.KindStateConflict, "task event has no matching active assignment")
		}
		assignment, err := decodeTaskAssignment(assignmentValue.Value)
		if err != nil {
			return TaskEventAppend{}, err
		}
		if assignmentIndexValue.ModRevision != assignmentValue.ModRevision ||
			!bytes.Equal(assignmentIndexValue.Value, assignmentValue.Value) {
			return TaskEventAppend{}, errs.New(errs.KindInternal, "task event assignment index is inconsistent")
		}
		if assignment.AssignmentID != input.Identity.AssignmentID ||
			assignment.TaskID != input.Identity.TaskID || assignment.Executor != TaskExecutorAgent ||
			assignment.AgentID != input.Identity.AgentID ||
			assignment.AgentGeneration != input.Identity.AgentGeneration ||
			assignment.ExecutionEpoch != input.Identity.Attempt {
			return TaskEventAppend{}, errs.New(errs.KindStateConflict, "task event assignment identity does not match")
		}
		if prepared.Duplicate {
			if err := repository.verifyDuplicateEvent(ctx, result.ReadRevision, task, *existing); err != nil {
				return TaskEventAppend{}, err
			}
			return TaskEventAppend{Sequence: prepared.Sequence, Revision: result.ReadRevision, Duplicate: true}, nil
		}
		if result.Values[4] != nil {
			return TaskEventAppend{}, errs.New(
				errs.KindStateConflict,
				"task event arrived after Blueprint source closure",
			)
		}

		var recoveryValue *KeyValue
		var recoveryMutation []Mutation
		if assignment.ExecutionMode == TaskExecutionModeRecoveryOnly {
			recoveryRead, readErr := repository.store.GetMany(ctx, GetManyRequest{
				Keys: []string{releaseRecoveryKey(task.ID)}, Revision: result.ReadRevision,
			})
			if readErr != nil {
				return TaskEventAppend{}, readErr
			}
			if recoveryRead == nil || recoveryRead.ReadRevision != result.ReadRevision ||
				len(recoveryRead.Values) != 1 ||
				recoveryRead.Values[0] == nil {
				return TaskEventAppend{}, corruptTaskAssignment()
			}
			recoveryValue = recoveryRead.Values[0]
			record, decodeErr := decodeReleaseRecoveryRecord(recoveryValue.Value)
			digest, digestErr := releaseRecoveryRecordSHA256(record)
			if decodeErr != nil || digestErr != nil || digest != assignment.ReleaseRecoveryRecordSHA256 ||
				record.TaskID != task.ID || record.AssignmentID != assignment.AssignmentID ||
				record.OperationID != task.OperationID || record.PlanHash != task.PlanHash ||
				record.RestorationAuthoritySHA256 != assignment.RestorationAuthoritySHA256 ||
				!record.RecoveryDeadline.Equal(assignment.RecoveryDeadline) {
				return TaskEventAppend{}, corruptTaskAssignment()
			}
			next, changed, advanceErr := advanceReleaseRecoveryRecord(record, input, result.ReadRevision)
			if advanceErr != nil {
				return TaskEventAppend{}, advanceErr
			}
			if changed {
				encoded, encodeErr := encodeReleaseRecoveryRecord(next)
				if encodeErr != nil {
					return TaskEventAppend{}, encodeErr
				}
				defer clear(encoded)
				recoveryMutation = []Mutation{{Type: MutationPut, Key: recoveryValue.Key, Value: encoded}}
			}
		} else if assignment.ExecutionMode != TaskExecutionModeForward {
			return TaskEventAppend{}, corruptTaskAssignment()
		}

		eventKey := taskEventKey(task.ID, prepared.Sequence)
		eventAtRevision, err := repository.store.GetMany(ctx, GetManyRequest{
			Keys: []string{eventKey}, Revision: result.ReadRevision,
		})
		if err != nil {
			return TaskEventAppend{}, err
		}
		if len(eventAtRevision.Values) != 1 {
			return TaskEventAppend{}, errs.New(errs.KindInternal, "task event collision read is invalid")
		}
		if eventAtRevision.Values[0] != nil {
			return TaskEventAppend{}, errs.New(errs.KindInternal, "task event sequence is already occupied")
		}

		encodedTask, err := encodeTaskRecord(prepared.Task)
		if err != nil {
			return TaskEventAppend{}, err
		}
		encodedEvent, err := encodeTaskEventRecord(prepared.Event)
		if err != nil {
			return TaskEventAppend{}, err
		}
		encodedDedup, err := encodeTaskEventDedupRecord(prepared.Dedup)
		if err != nil {
			return TaskEventAppend{}, err
		}
		conditions := []Condition{
			{Key: taskKey(task.ID), ModRevision: taskValue.ModRevision},
			{Key: claimKey, ModRevision: assignmentValue.ModRevision},
			{Key: taskAssignmentIndexKey(task.ID), ModRevision: assignmentIndexValue.ModRevision},
			{Key: taskEventDedupKey(input.Identity)},
			{Key: eventKey},
			{Key: blueprintClosingReportKey(task.ID)},
		}
		if recoveryValue != nil {
			conditions = append(conditions, Condition{Key: recoveryValue.Key, ModRevision: recoveryValue.ModRevision})
		}
		mutations := []Mutation{
			{Type: MutationPut, Key: taskKey(task.ID), Value: encodedTask},
			{Type: MutationPut, Key: eventKey, Value: encodedEvent},
			{Type: MutationPut, Key: taskEventDedupKey(input.Identity), Value: encodedDedup},
		}
		mutations = append(mutations, recoveryMutation...)
		transaction, err := repository.store.Transact(
			ctx,
			conditions,
			mutations,
		)
		if err != nil {
			return TaskEventAppend{}, err
		}
		if !transaction.Succeeded {
			conflicts++
			if err := repository.retryPolicy.waitAfterConflict(ctx, conflicts); err != nil {
				return TaskEventAppend{}, err
			}
			continue
		}
		return TaskEventAppend{Sequence: prepared.Sequence, Revision: transaction.Revision}, nil
	}
}
