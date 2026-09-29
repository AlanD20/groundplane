package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// taskClosingReport is temporary continuation authority, not a terminal
// receipt. It precedes partial terminal writes and is deleted with Task completion.
// Result is the original Agent report, before recovery normalization.
type taskClosingReport struct {
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

func manualTaskClosingReportKey(taskID string) string {
	return "/v1/records/manual-script-closing-reports/" + taskID
}

func releaseClosingReportKey(taskID string) string {
	return "/v1/records/release-closing-reports/" + taskID
}

func taskClosingReportKey(task TaskRecord) string {
	if task.Type == taskjournal.TaskScript {
		return manualTaskClosingReportKey(task.ID)
	}
	if task.Type != taskjournal.TaskUpdate {
		return releaseClosingReportKey(task.ID)
	}
	return blueprintClosingReportKey(task.ID)
}

func taskClosingReportEnvelope(task TaskRecord) string {
	if task.Type == taskjournal.TaskScript {
		return "manual-script-closing-report"
	}
	if task.Type != taskjournal.TaskUpdate {
		return "release-closing-report"
	}
	return "blueprint-closing-report"
}

func taskHasClosingReport(task TaskRecord) bool {
	return task.Type == taskjournal.TaskScript ||
		task.Params[releaserender.TaskReleasePublicationParam] != ""
}

func (report taskClosingReport) matches(status taskjournal.TaskStatus, result taskjournal.TaskResultRecord) bool {
	return report.Status == status &&
		taskjournal.TaskResultsEqual(report.Result, result) &&
		report.ExecutionEpoch == result.ExecutionEpoch &&
		report.RecoveryRecordSHA256 == result.ReleaseRecoveryRecordSHA256
}

func (report taskClosingReport) validate(current TaskAssignment) error {
	task, assignment := current.Task.Record, current.Assignment.Record
	if !taskHasClosingReport(task) ||
		task.Status != taskjournal.TaskStatusRunning ||
		task.Executor != taskjournal.TaskExecutorAgent ||
		report.TaskID != task.ID ||
		report.OperationID != task.OperationID ||
		report.PlanHash != task.PlanHash ||
		report.AssignmentID != assignment.AssignmentID ||
		report.AgentID != assignment.AgentID ||
		report.AgentGeneration != assignment.AgentGeneration ||
		report.ExecutionEpoch != assignment.ExecutionEpoch ||
		report.TaskRevision <= 0 ||
		report.TaskRevision != current.Task.Revision ||
		report.AssignmentRevision <= 0 ||
		report.AssignmentRevision != current.Assignment.Revision ||
		report.Result.ExecutionEpoch != assignment.ExecutionEpoch ||
		report.Result.ReconciliationRequired ||
		report.RecoveryRecordSHA256 != assignment.ReleaseRecoveryRecordSHA256 ||
		report.Result.ReleaseRecoveryRecordSHA256 != report.RecoveryRecordSHA256 ||
		taskjournal.ValidateTaskResult(report.Result, task.Steps, report.Status) != nil ||
		!taskjournal.IsTerminalTaskStatus(
			report.Status,
		) ||
		recordcodec.ValidateTimestamp("Blueprint closing report observed_at", report.ObservedAt) != nil ||
		report.ObservedAt.Before(task.UpdatedAt) {
		return errs.New(errs.KindInternal, "Blueprint closing report authority is corrupt")
	}
	return nil
}

func (repository *TaskRepository) readTaskClosingReport(
	ctx context.Context, current TaskAssignment,
) (taskClosingReport, *etcdstore.KeyValue, error) {
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{taskClosingReportKey(current.Task.Record)}, Revision: current.Task.ReadRevision,
	})
	if err != nil {
		return taskClosingReport{}, nil, err
	}
	if read == nil ||
		read.ReadRevision != current.Task.ReadRevision ||
		len(read.Values) != 1 {
		return taskClosingReport{}, nil, taskassignments.CorruptTaskAssignment()
	}
	value := read.Values[0]
	if value == nil {
		return taskClosingReport{}, nil, nil
	}
	report, err := recordcodec.Decode[taskClosingReport](
		value.Value,
		taskClosingReportEnvelope(current.Task.Record),
	)
	if err != nil {
		return taskClosingReport{}, nil, err
	}
	report.Result.ExecutionEpoch = report.ExecutionEpoch
	report.Result.ReleaseRecoveryRecordSHA256 = report.RecoveryRecordSHA256
	if err := report.validate(current); err != nil {
		return taskClosingReport{}, nil, err
	}
	return report, value, nil
}

func (repository *TaskRepository) prepareTaskClosingReport(
	ctx context.Context, current TaskAssignment, status taskjournal.TaskStatus, result taskjournal.TaskResultRecord,
	observedAt time.Time, starting bool,
) (taskClosingReport, etcdstore.Condition, etcdstore.Mutation, error) {
	report, value, err := repository.readTaskClosingReport(ctx, current)
	if err != nil {
		return taskClosingReport{}, etcdstore.Condition{}, etcdstore.Mutation{}, err
	}
	key := taskClosingReportKey(current.Task.Record)
	if !starting {
		if value == nil ||
			!report.matches(status, result) {
			return taskClosingReport{}, etcdstore.Condition{}, etcdstore.Mutation{}, errs.New(
				errs.KindStateConflict, "Blueprint closing report changed",
			)
		}
		condition := etcdstore.Condition{
			Key:         key,
			ModRevision: value.ModRevision,
		}
		mutation := etcdstore.Mutation{
			Type: etcdstore.MutationDelete,
			Key:  key,
		}
		return report, condition, mutation, nil
	}
	if value != nil {
		return taskClosingReport{}, etcdstore.Condition{}, etcdstore.Mutation{}, taskassignments.CorruptTaskAssignment()
	}
	task, assignment := current.Task.Record, current.Assignment.Record
	report = taskClosingReport{
		TaskID: task.ID, OperationID: task.OperationID, PlanHash: task.PlanHash,
		AssignmentID: assignment.AssignmentID, AgentID: assignment.AgentID,
		AgentGeneration: assignment.AgentGeneration, ExecutionEpoch: assignment.ExecutionEpoch,
		RecoveryRecordSHA256: result.ReleaseRecoveryRecordSHA256,
		TaskRevision:         current.Task.Revision, AssignmentRevision: current.Assignment.Revision,
		Status: status, Result: result, ObservedAt: observedAt,
	}
	if err := report.validate(current); err != nil {
		return taskClosingReport{}, etcdstore.Condition{}, etcdstore.Mutation{}, err
	}
	encoded, err := recordcodec.Encode(taskClosingReportEnvelope(task), report)
	condition := etcdstore.Condition{
		Key: key,
	}
	mutation := etcdstore.Mutation{
		Type:  etcdstore.MutationPut,
		Key:   key,
		Value: encoded,
	}
	return report, condition, mutation, err
}

func (repository *TaskRepository) resumeTaskClosingReport(
	ctx context.Context, current TaskAssignment,
) (TaskAssignment, bool, error) {
	if !taskHasClosingReport(current.Task.Record) {
		return current, false, nil
	}
	report, value, err := repository.readTaskClosingReport(ctx, current)
	if err != nil ||
		value == nil {
		return current, false, err
	}
	terminal, err := repository.AcknowledgeTask(ctx, report.AgentID, report.AgentGeneration,
		report.TaskID, report.AssignmentID, report.Status, report.Result, report.ObservedAt)
	if err != nil {
		return TaskAssignment{}, true, err
	}
	if !taskjournal.IsTerminalTaskStatus(terminal.Record.Status) {
		return TaskAssignment{}, true, taskassignments.CorruptTaskAssignment()
	}
	current.Task = terminal
	return current, true, nil
}
