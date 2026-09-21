package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// scriptClosingReport is temporary continuation authority, not a terminal
// receipt. It is created with source closure and deleted with Task completion.
// Result is the original Agent report, before recovery normalization.
type scriptClosingReport struct {
	TaskID               string                       `json:"task_id"`
	OperationID          string                       `json:"operation_id"`
	PlanHash             string                       `json:"plan_hash"`
	AssignmentID         string                       `json:"assignment_id"`
	AgentID              string                       `json:"agent_id"`
	AgentGeneration      uint64                       `json:"agent_generation"`
	ExecutionEpoch       uint32                       `json:"execution_epoch"`
	RecoveryRecordSHA256 string                       `json:"recovery_record_sha256,omitempty"`
	TaskRevision         int64                        `json:"task_revision"`
	AssignmentRevision   int64                        `json:"assignment_revision"`
	Status               taskjournal.TaskStatus       `json:"status"`
	Result               taskjournal.TaskResultRecord `json:"result"`
	ObservedAt           time.Time                    `json:"observed_at"`
}

func blueprintClosingReportKey(taskID string) string {
	return "/v1/records/blueprint-closing-reports/" + taskID
}

func manualScriptClosingReportKey(taskID string) string {
	return "/v1/records/manual-script-closing-reports/" + taskID
}

func scriptClosingReportKey(task TaskRecord) string {
	if task.Type == taskjournal.TaskScript {
		return manualScriptClosingReportKey(task.ID)
	}
	return blueprintClosingReportKey(task.ID)
}

func scriptClosingReportEnvelope(task TaskRecord) string {
	if task.Type == taskjournal.TaskScript {
		return "manual-script-closing-report"
	}
	return "blueprint-closing-report"
}

func taskHasScriptClosingReport(task TaskRecord) bool {
	return task.Type == taskjournal.TaskScript || (task.Type == taskjournal.TaskUpdate && task.Params[TaskReleasePublicationParam] != "")
}

func (report scriptClosingReport) matches(status taskjournal.TaskStatus, result taskjournal.TaskResultRecord) bool {
	return report.Status == status && taskResultsEqual(report.Result, result) &&
		report.ExecutionEpoch == result.ExecutionEpoch && report.RecoveryRecordSHA256 == result.ReleaseRecoveryRecordSHA256
}

func (report scriptClosingReport) validate(current TaskAssignment) error {
	task, assignment := current.Task.Record, current.Assignment.Record
	if !taskHasScriptClosingReport(task) || task.Status != taskjournal.TaskStatusRunning || task.Executor != taskjournal.TaskExecutorAgent ||
		report.TaskID != task.ID ||
		report.OperationID != task.OperationID || report.PlanHash != task.PlanHash ||
		report.AssignmentID != assignment.AssignmentID || report.AgentID != assignment.AgentID ||
		report.AgentGeneration != assignment.AgentGeneration || report.ExecutionEpoch != assignment.ExecutionEpoch ||
		report.TaskRevision <= 0 || report.TaskRevision != current.Task.Revision ||
		report.AssignmentRevision <= 0 || report.AssignmentRevision != current.Assignment.Revision ||
		report.Result.ExecutionEpoch != assignment.ExecutionEpoch || report.Result.ReconciliationRequired ||
		report.RecoveryRecordSHA256 != assignment.ReleaseRecoveryRecordSHA256 ||
		report.Result.ReleaseRecoveryRecordSHA256 != report.RecoveryRecordSHA256 ||
		taskjournal.ValidateTaskResult(report.Result, task.Steps, report.Status) != nil ||
		!isTerminalTaskStatus(
			report.Status,
		) || recordcodec.ValidateTimestamp("Blueprint closing report observed_at", report.ObservedAt) != nil ||
		report.ObservedAt.Before(task.UpdatedAt) {
		return errs.New(errs.KindInternal, "Blueprint closing report authority is corrupt")
	}
	return nil
}

func (repository *TaskRepository) readScriptClosingReport(
	ctx context.Context, current TaskAssignment,
) (scriptClosingReport, *etcdstore.KeyValue, error) {
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{scriptClosingReportKey(current.Task.Record)}, Revision: current.Task.ReadRevision,
	})
	if err != nil {
		return scriptClosingReport{}, nil, err
	}
	if read == nil || read.ReadRevision != current.Task.ReadRevision || len(read.Values) != 1 {
		return scriptClosingReport{}, nil, corruptTaskAssignment()
	}
	value := read.Values[0]
	if value == nil {
		return scriptClosingReport{}, nil, nil
	}
	report, err := recordcodec.Decode[scriptClosingReport](value.Value, scriptClosingReportEnvelope(current.Task.Record))
	if err != nil {
		return scriptClosingReport{}, nil, err
	}
	report.Result.ExecutionEpoch = report.ExecutionEpoch
	report.Result.ReleaseRecoveryRecordSHA256 = report.RecoveryRecordSHA256
	if err := report.validate(current); err != nil {
		return scriptClosingReport{}, nil, err
	}
	return report, value, nil
}

func (repository *TaskRepository) prepareScriptClosingReport(
	ctx context.Context, current TaskAssignment, status taskjournal.TaskStatus, result taskjournal.TaskResultRecord,
	observedAt time.Time, starting bool,
) (scriptClosingReport, etcdstore.Condition, etcdstore.Mutation, error) {
	report, value, err := repository.readScriptClosingReport(ctx, current)
	if err != nil {
		return scriptClosingReport{}, etcdstore.Condition{}, etcdstore.Mutation{}, err
	}
	key := scriptClosingReportKey(current.Task.Record)
	if !starting {
		if value == nil || !report.matches(status, result) {
			return scriptClosingReport{}, etcdstore.Condition{}, etcdstore.Mutation{}, errs.New(
				errs.KindStateConflict, "Blueprint closing report changed",
			)
		}
		return report, etcdstore.Condition{
			Key:         key,
			ModRevision: value.ModRevision,
		}, etcdstore.Mutation{
			Type: etcdstore.MutationDelete,
			Key:  key,
		}, nil
	}
	if value != nil {
		return scriptClosingReport{}, etcdstore.Condition{}, etcdstore.Mutation{}, corruptTaskAssignment()
	}
	task, assignment := current.Task.Record, current.Assignment.Record
	report = scriptClosingReport{
		TaskID: task.ID, OperationID: task.OperationID, PlanHash: task.PlanHash,
		AssignmentID: assignment.AssignmentID, AgentID: assignment.AgentID,
		AgentGeneration: assignment.AgentGeneration, ExecutionEpoch: assignment.ExecutionEpoch,
		RecoveryRecordSHA256: result.ReleaseRecoveryRecordSHA256,
		TaskRevision:         current.Task.Revision, AssignmentRevision: current.Assignment.Revision,
		Status: status, Result: result, ObservedAt: observedAt,
	}
	if err := report.validate(current); err != nil {
		return scriptClosingReport{}, etcdstore.Condition{}, etcdstore.Mutation{}, err
	}
	encoded, err := recordcodec.Encode(scriptClosingReportEnvelope(task), report)
	return report, etcdstore.Condition{Key: key}, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: key, Value: encoded}, err
}

func (repository *TaskRepository) resumeScriptClosingReport(
	ctx context.Context, current TaskAssignment,
) (TaskAssignment, bool, error) {
	if !taskHasScriptClosingReport(current.Task.Record) {
		return current, false, nil
	}
	report, value, err := repository.readScriptClosingReport(ctx, current)
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
