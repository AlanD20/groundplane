package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// prepareReleaseTerminalReport fences recovery entry against source closure.
// Once closing, the original report is immutable and effect classification is
// no longer permitted; in particular cleanup itself cannot trigger recovery.
func (repository *TaskRepository) prepareReleaseTerminalReport(
	ctx context.Context, current TaskAssignment, status taskjournal.TaskStatus, result *taskjournal.TaskResultRecord,
) ([]etcdstore.Condition, error) {
	task, assignment := current.Task.Record, current.Assignment.Record
	if task.Executor != taskjournal.TaskExecutorAgent || result == nil ||
		(task.Type != taskjournal.TaskScript && task.Params[TaskReleasePublicationParam] == "") {
		return nil, nil
	}
	if task.Params[TaskReleasePublicationParam] != "" && assignment.ExecutionMode == TaskExecutionModeForward &&
		(result.ExecutionEpoch != assignment.ExecutionEpoch || result.ReleaseRecoveryRecordSHA256 != "") {
		return nil, errs.New(errs.KindStateConflict, "release terminal execution epoch changed")
	}
	var conditions []etcdstore.Condition
	if taskHasScriptClosingReport(task) {
		report, value, err := repository.readScriptClosingReport(ctx, current)
		if err != nil {
			return nil, err
		}
		if value != nil {
			if !report.matches(status, *result) {
				return nil, errs.New(errs.KindStateConflict, "Blueprint closing report changed")
			}
			return nil, nil
		}
	}
	if task.Type == taskjournal.TaskScript {
		return nil, nil
	}
	if assignment.ExecutionMode != TaskExecutionModeForward ||
		result.Diagnostic != taskjournal.TaskResultDiagnosticTimeoutBeforeEffect || result.ReconciliationRequired {
		if task.Type == taskjournal.TaskUpdate && result.ReconciliationRequired {
			conditions = append(conditions, etcdstore.Condition{Key: blueprintClosingReportKey(task.ID)})
		}
		return conditions, nil
	}
	_, procedure, err := repository.candidateReleaseDescriptorAtRevision(ctx, task, current.Task.ReadRevision)
	if err != nil || validateAssignmentRestorationDescriptor(task, assignment, procedure) != nil {
		return nil, corruptTaskAssignment()
	}
	effect, evidence, err := repository.releaseEffectEvidenceAtRevision(
		ctx,
		task,
		assignment,
		procedure,
		current.Task.ReadRevision,
	)
	if err != nil {
		return nil, err
	}
	if effect {
		result.Diagnostic = taskjournal.TaskResultDiagnosticNone
		result.ReconciliationRequired = true
		if task.Type == taskjournal.TaskUpdate {
			conditions = append(conditions, etcdstore.Condition{Key: blueprintClosingReportKey(task.ID)})
		}
	}
	if err := taskjournal.ValidateTaskResult(*result, task.Steps, status); err != nil {
		return nil, err
	}
	return append(conditions, evidence...), nil
}
