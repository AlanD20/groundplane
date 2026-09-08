package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// blueprintClosingReport is temporary continuation authority, not a terminal
// receipt. It is created with source closure and deleted with Task completion.
// Result is the original Agent report, before recovery normalization.
type blueprintClosingReport struct {
	TaskID               string           `json:"task_id"`
	OperationID          string           `json:"operation_id"`
	PlanHash             string           `json:"plan_hash"`
	AssignmentID         string           `json:"assignment_id"`
	AgentID              string           `json:"agent_id"`
	AgentGeneration      uint64           `json:"agent_generation"`
	ExecutionEpoch       uint32           `json:"execution_epoch"`
	RecoveryRecordSHA256 string           `json:"recovery_record_sha256,omitempty"`
	TaskRevision         int64            `json:"task_revision"`
	AssignmentRevision   int64            `json:"assignment_revision"`
	Status               TaskStatus       `json:"status"`
	Result               TaskResultRecord `json:"result"`
	ObservedAt           time.Time        `json:"observed_at"`
}

func blueprintClosingReportKey(taskID string) string {
	return "/v1/records/blueprint-closing-reports/" + taskID
}

func (report blueprintClosingReport) matches(status TaskStatus, result TaskResultRecord) bool {
	return report.Status == status && taskResultsEqual(report.Result, result) &&
		report.ExecutionEpoch == result.ExecutionEpoch && report.RecoveryRecordSHA256 == result.ReleaseRecoveryRecordSHA256
}

func (report blueprintClosingReport) validate(current TaskAssignment) error {
	task, assignment := current.Task.Record, current.Assignment.Record
	if task.Type != TaskUpdate || task.Status != TaskStatusRunning || task.Executor != TaskExecutorAgent ||
		task.Params[TaskReleasePublicationParam] == "" || report.TaskID != task.ID ||
		report.OperationID != task.OperationID || report.PlanHash != task.PlanHash ||
		report.AssignmentID != assignment.AssignmentID || report.AgentID != assignment.AgentID ||
		report.AgentGeneration != assignment.AgentGeneration || report.ExecutionEpoch != assignment.ExecutionEpoch ||
		report.TaskRevision <= 0 || report.TaskRevision != current.Task.Revision ||
		report.AssignmentRevision <= 0 || report.AssignmentRevision != current.Assignment.Revision ||
		report.Result.ExecutionEpoch != assignment.ExecutionEpoch || report.Result.ReconciliationRequired ||
		report.RecoveryRecordSHA256 != assignment.ReleaseRecoveryRecordSHA256 ||
		report.Result.ReleaseRecoveryRecordSHA256 != report.RecoveryRecordSHA256 ||
		validateTaskResult(report.Result, task.Steps, report.Status) != nil ||
		!isTerminalTaskStatus(
			report.Status,
		) || validateTimestamp("Blueprint closing report observed_at", report.ObservedAt) != nil ||
		report.ObservedAt.Before(task.UpdatedAt) {
		return errs.New(errs.KindInternal, "Blueprint closing report authority is corrupt")
	}
	return nil
}

func (repository *TaskRepository) readBlueprintClosingReport(
	ctx context.Context, current TaskAssignment,
) (blueprintClosingReport, *KeyValue, error) {
	read, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{blueprintClosingReportKey(current.Task.Record.ID)}, Revision: current.Task.ReadRevision,
	})
	if err != nil {
		return blueprintClosingReport{}, nil, err
	}
	if read == nil || read.ReadRevision != current.Task.ReadRevision || len(read.Values) != 1 {
		return blueprintClosingReport{}, nil, corruptTaskAssignment()
	}
	value := read.Values[0]
	if value == nil {
		return blueprintClosingReport{}, nil, nil
	}
	report, err := decodeEnvelope[blueprintClosingReport](value.Value, "blueprint-closing-report")
	if err != nil {
		return blueprintClosingReport{}, nil, err
	}
	report.Result.ExecutionEpoch = report.ExecutionEpoch
	report.Result.ReleaseRecoveryRecordSHA256 = report.RecoveryRecordSHA256
	if err := report.validate(current); err != nil {
		return blueprintClosingReport{}, nil, err
	}
	return report, value, nil
}

func (repository *TaskRepository) prepareBlueprintClosingReport(
	ctx context.Context, current TaskAssignment, status TaskStatus, result TaskResultRecord,
	observedAt time.Time, starting bool,
) (blueprintClosingReport, Condition, Mutation, error) {
	report, value, err := repository.readBlueprintClosingReport(ctx, current)
	if err != nil {
		return blueprintClosingReport{}, Condition{}, Mutation{}, err
	}
	key := blueprintClosingReportKey(current.Task.Record.ID)
	if !starting {
		if value == nil || !report.matches(status, result) {
			return blueprintClosingReport{}, Condition{}, Mutation{}, errs.New(
				errs.KindStateConflict, "Blueprint closing report changed",
			)
		}
		return report, Condition{
				Key:         key,
				ModRevision: value.ModRevision,
			}, Mutation{
				Type: MutationDelete,
				Key:  key,
			}, nil
	}
	if value != nil {
		return blueprintClosingReport{}, Condition{}, Mutation{}, corruptTaskAssignment()
	}
	task, assignment := current.Task.Record, current.Assignment.Record
	report = blueprintClosingReport{
		TaskID: task.ID, OperationID: task.OperationID, PlanHash: task.PlanHash,
		AssignmentID: assignment.AssignmentID, AgentID: assignment.AgentID,
		AgentGeneration: assignment.AgentGeneration, ExecutionEpoch: assignment.ExecutionEpoch,
		RecoveryRecordSHA256: result.ReleaseRecoveryRecordSHA256,
		TaskRevision:         current.Task.Revision, AssignmentRevision: current.Assignment.Revision,
		Status: status, Result: result, ObservedAt: observedAt,
	}
	if err := report.validate(current); err != nil {
		return blueprintClosingReport{}, Condition{}, Mutation{}, err
	}
	encoded, err := encodeEnvelope("blueprint-closing-report", report)
	return report, Condition{Key: key}, Mutation{Type: MutationPut, Key: key, Value: encoded}, err
}

func (repository *TaskRepository) resumeBlueprintClosingReport(
	ctx context.Context, current TaskAssignment,
) (TaskAssignment, bool, error) {
	if current.Task.Record.Type != TaskUpdate {
		return current, false, nil
	}
	report, value, err := repository.readBlueprintClosingReport(ctx, current)
	if err != nil || value == nil {
		return current, false, err
	}
	terminal, err := repository.AcknowledgeTask(ctx, report.AgentID, report.AgentGeneration,
		report.TaskID, report.AssignmentID, report.Status, report.Result, report.ObservedAt)
	if err != nil {
		return TaskAssignment{}, true, err
	}
	if !isTerminalTaskStatus(terminal.Record.Status) {
		return TaskAssignment{}, true, corruptTaskAssignment()
	}
	current.Task = terminal
	return current, true, nil
}
