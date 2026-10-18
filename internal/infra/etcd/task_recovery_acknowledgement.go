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
	if assignment.AttachRemovalAuthoritySHA256 != "" {
		authority, _, found, err := repository.attachRemovalRecoveryAuthorityAtRevision(ctx, task, revision)
		if err != nil {
			return keyvalue.Versioned[TaskRecord]{}, false, false, err
		}
		if !found {
			return keyvalue.Versioned[TaskRecord]{}, false, false, taskassignments.CorruptTaskAssignment()
		}
		evidence, err := repository.attachRemovalMutationEvidenceAtRevision(ctx, task, assignment, authority, revision)
		if err != nil {
			return keyvalue.Versioned[TaskRecord]{}, false, false, err
		}
		if len(evidence) == 0 {
			// Native Detach effects cannot run before their durable start.
			result.ReconciliationRequired = false
			return keyvalue.Versioned[TaskRecord]{}, false, false, nil
		}
		transitioned, processed, err := repository.transitionAttachRemovalAcknowledgementToRecovery(
			ctx, task, taskValue, assignment, assignmentValue, assignmentIndexValue, status, *result, revision,
		)
		return transitioned, true, processed, err
	}
	if assignment.ManagedNetworkProcedureSHA256 != "" {
		required, err := repository.managedNetworkRecoveryRequiredAtRevision(ctx, task, assignment, revision)
		if err != nil {
			return keyvalue.Versioned[TaskRecord]{}, false, false, err
		}
		if !required {
			// The Agent must durably publish running before invoking the helper.
			// Without that event this assignment cannot have affected the bridge.
			result.ReconciliationRequired = false
			return keyvalue.Versioned[TaskRecord]{}, false, false, nil
		}
		transitioned, processed, err := repository.transitionManagedNetworkAcknowledgementToRecovery(
			ctx, task, taskValue, assignment, assignmentValue, assignmentIndexValue,
			status, *result, revision,
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
