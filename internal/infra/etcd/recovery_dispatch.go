package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

func (repository *TaskRepository) recoveryAcknowledgementAtRevision(
	ctx context.Context,
	task TaskRecord,
	assignment taskassignments.TaskAssignmentRecord,
	status taskjournal.TaskStatus,
	result taskjournal.TaskResultRecord,
	revision int64,
) (releaseRecoveryAcknowledgement, error) {
	if assignment.AttachRemovalAuthoritySHA256 != "" {
		return repository.attachRemovalRecoveryAcknowledgementAtRevision(
			ctx,
			task,
			assignment,
			status,
			result,
			revision,
		)
	}
	if assignment.ManagedNetworkProcedureSHA256 != "" {
		return repository.managedNetworkRecoveryAcknowledgementAtRevision(
			ctx, task, assignment, status, result, revision,
		)
	}
	if assignment.ServiceAbsenceProcedureSHA256 != "" {
		return repository.serviceAbsenceRecoveryAcknowledgementAtRevision(
			ctx,
			task,
			assignment,
			status,
			result,
			revision,
		)
	}
	return repository.releaseRecoveryAcknowledgementAtRevision(ctx, task, assignment, status, result, revision)
}

func (repository *TaskRepository) normalizeRecoveryTerminalReplay(
	ctx context.Context,
	task TaskRecord,
	status taskjournal.TaskStatus,
	result taskjournal.TaskResultRecord,
	agentID string,
	agentGeneration uint64,
	assignmentID string,
	revision int64,
) (taskjournal.TaskStatus, *taskjournal.TaskResultRecord, bool, error) {
	if authoredBlueprintAttachRemovalTask(task) {
		return repository.normalizeAttachRemovalRecoveryTerminalReplay(
			ctx,
			task,
			status,
			result,
			agentID,
			agentGeneration,
			assignmentID,
			revision,
		)
	}
	if authoredBlueprintZoneRemovalTask(task) {
		return repository.normalizeManagedNetworkRecoveryTerminalReplay(
			ctx, task, status, result, agentID, agentGeneration, assignmentID, revision,
		)
	}
	if result.ServiceAbsenceEvidence != nil {
		return repository.normalizeServiceAbsenceTerminalReplay(
			ctx, task, status, result, agentID, agentGeneration, assignmentID, revision,
		)
	}
	return repository.normalizeReleaseRecoveryTerminalReplay(
		ctx, task, status, result, agentID, agentGeneration, assignmentID, revision,
	)
}
