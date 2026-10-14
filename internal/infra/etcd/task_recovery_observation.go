package etcd

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// TaskRecoverySnapshot is presentation evidence, not permission to execute or
// retry. It is read at the same revision as the Task and its event history.
type TaskRecoverySnapshot struct {
	State                  string
	PrimaryStatus          taskjournal.TaskStatus
	PrimaryResult          taskjournal.TaskResultRecord
	OriginalExecutionEpoch uint32
	StepIDs                []string
	CompletedSteps         *uint32
	Deadline               *time.Time
	AutomaticFailures      uint32
	RetryAfter             *time.Time
	RetryAvailable         bool
	StopReason             string
}

// Read retained recovery records rather than reconstructing the original
// failure from the most recent event, which may describe a recovery attempt.
func (repository *TaskRepository) GetTaskRecovery(
	ctx context.Context, task TaskRecord, revision int64,
) (*TaskRecoverySnapshot, error) {
	if revision <= 0 {
		return nil, errs.New(errs.KindInternal, "Task recovery requires a fixed read revision")
	}
	if task.Executor != taskjournal.TaskExecutorAgent || task.Status == taskjournal.TaskStatusPending {
		return nil, nil
	}
	if taskjournal.IsTerminalTaskStatus(task.Status) {
		// Accepted recovery deletes its execution records in the terminal
		// transaction. The result retains the original failure and recovery
		// digest; do not reconstruct retired progress from current resources.
		if task.Result == nil || task.Result.ReleaseRecoveryRecordSHA256 == "" {
			return nil, nil
		}
		if task.Result.ReconciliationRequired || task.Status == taskjournal.TaskStatusCompleted ||
			task.TerminalAssignment == nil {
			return nil, taskassignments.CorruptTaskAssignment()
		}
		return &TaskRecoverySnapshot{
			State:         "completed",
			PrimaryStatus: task.Status,
			PrimaryResult: *taskjournal.CloneTaskResult(task.Result),
		}, nil
	}
	keys := []string{
		taskassignments.ReleaseRecoveryKey(task.ID),
		taskassignments.ComponentRecoveryKey(task.ID),
		taskassignments.ServiceAbsenceRecoveryKey(task.ID),
		taskassignments.ManagedNetworkRecoveryKey(task.ID),
		taskassignments.AttachRemovalRecoveryKey(task.ID),
		taskjournal.TaskAssignmentIndexKey(task.ID),
		taskjournal.TaskRecoveryProofRequiredKey(task.ID),
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return nil, taskassignments.CorruptTaskAssignment()
	}
	defer keyvalue.ClearValues(read.Values)
	var observation *TaskRecoverySnapshot
	var assignmentID, digest string
	for index, value := range read.Values[:5] {
		if value == nil {
			continue
		}
		if value.Key != keys[index] || observation != nil {
			return nil, taskassignments.CorruptTaskAssignment()
		}
		observation, assignmentID, digest, err = taskRecoveryRecordObservation(task, index, value.Value)
		if err != nil {
			return nil, err
		}
	}
	if observation == nil {
		return nil, nil
	}
	if err := taskjournal.ValidateTaskResult(observation.PrimaryResult, task.Steps, observation.PrimaryStatus); err != nil {
		return nil, taskassignments.CorruptTaskAssignment()
	}
	value := read.Values[5]
	if value == nil || value.Key != keys[5] {
		return nil, taskassignments.CorruptTaskAssignment()
	}
	assignment, err := taskassignments.DecodeTaskAssignment(value.Value)
	if err != nil || assignment.TaskID != task.ID || assignment.AssignmentID != assignmentID ||
		assignment.ExecutionMode != taskassignments.TaskExecutionModeRecoveryOnly ||
		assignment.ReleaseRecoveryRecordSHA256 != digest {
		return nil, taskassignments.CorruptTaskAssignment()
	}
	observation.State = "pending"
	if proof := read.Values[6]; proof != nil {
		proofAssignment, decodeErr := taskassignments.DecodeTaskAssignment(proof.Value)
		if decodeErr != nil || proof.Key != keys[6] || proofAssignment.TaskID != task.ID ||
			proofAssignment.AssignmentID != assignmentID || proofAssignment.ReleaseRecoveryRecordSHA256 != digest ||
			proofAssignment.ExecutionEpoch != assignment.ExecutionEpoch {
			return nil, taskassignments.CorruptTaskAssignment()
		}
		observation.State = "proof_required"
	} else if observation.CompletedSteps != nil && int(*observation.CompletedSteps) == len(observation.StepIDs) {
		observation.State = "awaiting_confirmation"
	}
	control, _, err := repository.readRecoveryRetryControl(ctx, task.ID, revision)
	if err != nil {
		return nil, err
	}
	if control != nil {
		observation.AutomaticFailures = control.Failures
		if control.Stopped {
			observation.State, observation.RetryAvailable = "automatic_retries_stopped", true
			observation.StopReason = control.StopReason
		} else if assignment.ExecutionEpoch == control.ExecutionEpoch {
			observation.State = "retry_wait"
			next := control.NextAttemptAt
			observation.RetryAfter = &next
		}
	} else if observation.State == "proof_required" && !assignment.RecoveryExecutionDeadline.After(time.Now().UTC()) {
		observation.RetryAvailable = true
	}
	return observation, nil
}

func taskRecoveryRecordObservation(
	task TaskRecord,
	kind int,
	value []byte,
) (*TaskRecoverySnapshot, string, string, error) {
	observation := &TaskRecoverySnapshot{}
	var taskID, operationID, planHash, assignmentID, digest string
	var err error
	switch kind {
	case 0, 1, 2:
		record, decodeErr := taskassignments.DecodeReleaseRecoveryRecord(value)
		if decodeErr != nil {
			return nil, "", "", decodeErr
		}
		digest, err = taskassignments.ReleaseRecoveryRecordSHA256(record)
		taskID, operationID, planHash, assignmentID = record.TaskID, record.OperationID, record.PlanHash, record.AssignmentID
		observation.PrimaryStatus, observation.PrimaryResult = record.PrimaryStatus, record.PrimaryResult
		observation.StepIDs, observation.Deadline = slices.Clone(record.RecoveryStepIDs), &record.RecoveryDeadline
		observation.CompletedSteps = &record.Cursor
	case 3:
		record, decodeErr := taskassignments.DecodeManagedNetworkRecoveryRecord(value)
		if decodeErr != nil {
			return nil, "", "", decodeErr
		}
		digest, err = taskassignments.ManagedNetworkRecoveryRecordSHA256(record)
		taskID, operationID, planHash, assignmentID = record.TaskID, record.OperationID, record.PlanHash, record.AssignmentID
		primary, decodeErr := taskjournal.TaskResultFromData(&record.PrimaryResult)
		if decodeErr != nil || primary == nil {
			return nil, "", "", taskassignments.CorruptTaskAssignment()
		}
		observation.PrimaryStatus, observation.PrimaryResult = record.PrimaryStatus, *primary
		observation.StepIDs, observation.Deadline = slices.Clone(record.SettlementStepIDs), &record.RecoveryDeadline
		observation.OriginalExecutionEpoch = record.OriginalExecutionEpoch
	case 4:
		record, decodeErr := taskassignments.DecodeAttachRemovalRecoveryRecord(value)
		if decodeErr != nil {
			return nil, "", "", decodeErr
		}
		digest, err = taskassignments.AttachRemovalRecoveryRecordSHA256(record)
		taskID, operationID, planHash, assignmentID = record.Authority.TaskID, record.Authority.OperationID, record.Authority.PlanHash, record.AssignmentID
		primary, decodeErr := taskjournal.TaskResultFromData(&record.PrimaryResult)
		if decodeErr != nil || primary == nil {
			return nil, "", "", taskassignments.CorruptTaskAssignment()
		}
		observation.PrimaryStatus, observation.PrimaryResult = record.PrimaryStatus, *primary
		observation.StepIDs, observation.Deadline = slices.Clone(record.Authority.StepIDs), &record.RecoveryDeadline
		observation.CompletedSteps, observation.OriginalExecutionEpoch = &record.Cursor, record.OriginalExecutionEpoch
	default:
		return nil, "", "", taskassignments.CorruptTaskAssignment()
	}
	if err != nil || taskID != task.ID || operationID != task.OperationID || planHash != task.PlanHash {
		return nil, "", "", taskassignments.CorruptTaskAssignment()
	}
	return observation, assignmentID, digest, nil
}
