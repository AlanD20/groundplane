package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

func (repository *TaskRepository) transitionAcknowledgementToRecovery(
	ctx context.Context,
	task TaskRecord,
	taskValue *keyvalue.KeyValue,
	assignment taskassignments.TaskAssignmentRecord,
	assignmentValue, assignmentIndexValue *keyvalue.KeyValue,
	status taskjournal.TaskStatus,
	result *taskjournal.TaskResultRecord,
	revision int64,
	timeoutEvidenceConditions []keyvalue.Condition,
) (keyvalue.Versioned[TaskRecord], bool, bool, error) {
	if assignment.Executor != taskjournal.TaskExecutorAgent || result == nil || !result.ReconciliationRequired ||
		assignment.ExecutionMode != taskassignments.TaskExecutionModeForward {
		return keyvalue.Versioned[TaskRecord]{}, false, false, nil
	}
	if task.Params[taskjournal.TaskBlueprintComponentUnitParam] != "" {
		transitioned, processed, err := repository.transitionComponentAcknowledgementToRecovery(
			ctx, task, taskValue, assignment, assignmentValue, assignmentIndexValue, status, *result, revision,
		)
		return transitioned, true, processed, err
	}
	if assignment.ServiceAbsenceProcedureSHA256 != "" {
		required, err := repository.serviceAbsenceRecoveryRequiredAtRevision(ctx, task, assignment, revision)
		if err != nil {
			return keyvalue.Versioned[TaskRecord]{}, false, false, err
		}
		if !required {
			// The Agent waits for the durable running event before invoking the
			// helper. Its absence is exact proof that this attempt had no host
			// effect, so the original failure can close without a recovery retry.
			result.ReconciliationRequired = false
			return keyvalue.Versioned[TaskRecord]{}, false, false, nil
		}
		transitioned, processed, err := repository.transitionServiceAbsenceAcknowledgementToRecovery(
			ctx, task, taskValue, assignment, assignmentValue, assignmentIndexValue, status, *result, revision,
		)
		return transitioned, true, processed, err
	}
	if releaseRecoveryTask(task) {
		transitioned, processed, err := repository.transitionReleaseAcknowledgementToRecovery(
			ctx, task, taskValue, assignment, assignmentValue, assignmentIndexValue,
			status, *result, revision, timeoutEvidenceConditions...,
		)
		return transitioned, true, processed, err
	}
	return keyvalue.Versioned[TaskRecord]{}, false, false, nil
}
