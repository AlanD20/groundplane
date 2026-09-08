package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// prepareReleaseTerminalReport fences recovery entry against source closure.
// Once closing, the original report is immutable and effect classification is
// no longer permitted; in particular cleanup itself cannot trigger recovery.
func (repository *TaskRepository) prepareReleaseTerminalReport(
	ctx context.Context, current TaskAssignment, status TaskStatus, result *TaskResultRecord,
) ([]Condition, error) {
	task, assignment := current.Task.Record, current.Assignment.Record
	if task.Executor != TaskExecutorAgent || result == nil ||
		(task.Type != TaskScript && task.Params[TaskReleasePublicationParam] == "") {
		return nil, nil
	}
	var conditions []Condition
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
	if task.Type == TaskScript {
		return nil, nil
	}
	if assignment.ExecutionMode != TaskExecutionModeForward ||
		result.Diagnostic != TaskResultDiagnosticTimeoutBeforeEffect || result.ReconciliationRequired {
		if task.Type == TaskUpdate && result.ReconciliationRequired {
			conditions = append(conditions, Condition{Key: blueprintClosingReportKey(task.ID)})
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
		result.Diagnostic = TaskResultDiagnosticNone
		result.ReconciliationRequired = true
		if task.Type == TaskUpdate {
			conditions = append(conditions, Condition{Key: blueprintClosingReportKey(task.ID)})
		}
	}
	if err := validateTaskResult(*result, task.Steps, status); err != nil {
		return nil, err
	}
	return append(conditions, evidence...), nil
}
