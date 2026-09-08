package etcd

import (
	"context"
	"errors"
	"math"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReconnectAgentAssignment fences the previous execution epoch before a
// recovered assignment is returned to an Agent. Only fixed-revision durable
// event and Script checkpoint evidence can force recovery.
func (repository *TaskRepository) ReconnectAgentAssignment(
	ctx context.Context,
	expected TaskAssignment,
) (TaskAssignment, error) {
	for conflicts := 0; ; conflicts++ {
		current, err := repository.GetTaskAssignment(ctx, expected.Task.Record.ID)
		if err != nil {
			return TaskAssignment{}, err
		}
		assignment := current.Assignment.Record
		task := current.Task.Record
		if assignment.AssignmentID != expected.Assignment.Record.AssignmentID ||
			assignment.AgentID != expected.Assignment.Record.AgentID ||
			assignment.AgentGeneration != expected.Assignment.Record.AgentGeneration {
			return TaskAssignment{}, errs.New(errs.KindStateConflict, "Agent reconnect assignment authority changed")
		}
		if taskHasScriptClosingReport(task) {
			if terminal, closing, err := repository.resumeScriptClosingReport(ctx, current); err != nil || closing {
				return terminal, err
			}
		}
		if assignment.ExecutionEpoch == math.MaxUint32 {
			return TaskAssignment{}, errs.New(errs.KindStateConflict, "Agent reconnect assignment authority changed")
		}
		if task.Params[TaskReleasePublicationParam] == "" {
			return current, nil
		}
		_, procedure, err := repository.candidateReleaseDescriptorAtRevision(ctx, task, current.Task.ReadRevision)
		if err != nil || validateAssignmentRestorationDescriptor(task, assignment, procedure) != nil {
			return TaskAssignment{}, corruptTaskAssignment()
		}
		if assignment.ExecutionMode == TaskExecutionModeForward {
			effect, evidenceConditions, classifyErr := repository.releaseEffectEvidenceAtRevision(
				ctx, task, assignment, procedure, current.Task.ReadRevision,
			)
			if classifyErr != nil {
				return TaskAssignment{}, classifyErr
			}
			if effect {
				result := TaskResultRecord{
					Kind: TaskResultCompose, Diagnostic: TaskResultDiagnosticNone,
					ReconciliationRequired: true, ExecutionEpoch: assignment.ExecutionEpoch,
				}
				taskBytes, _ := encodeTaskRecord(task)
				assignmentBytes, _ := encodeTaskAssignment(assignment)
				transitioned, processed, transitionErr := repository.transitionReleaseAcknowledgementToRecovery(
					ctx,
					task,
					&KeyValue{Key: taskKey(task.ID), Value: taskBytes, ModRevision: current.Task.Revision},
					assignment,
					&KeyValue{
						Key:         taskExecutionClaimKey(assignment.Executor, assignment.AgentID, task.ID),
						Value:       assignmentBytes,
						ModRevision: current.Assignment.Revision,
					},
					&KeyValue{
						Key:         taskAssignmentIndexKey(task.ID),
						Value:       assignmentBytes,
						ModRevision: current.Assignment.Revision,
					},
					TaskStatusFailed,
					result,
					current.Task.ReadRevision,
					evidenceConditions...,
				)
				clear(taskBytes)
				clear(assignmentBytes)
				if transitionErr != nil {
					return TaskAssignment{}, transitionErr
				}
				if !processed || transitioned.ReadRevision == 0 {
					if err := repository.retryPolicy.waitAfterConflict(ctx, conflicts+1); err != nil {
						return TaskAssignment{}, err
					}
					continue
				}
				return repository.GetTaskAssignment(ctx, task.ID)
			}
			if err := repository.incrementAssignmentEpoch(ctx, current, evidenceConditions); err != nil {
				if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
					return TaskAssignment{}, err
				}
				if err := repository.retryPolicy.waitAfterConflict(ctx, conflicts+1); err != nil {
					return TaskAssignment{}, err
				}
				continue
			}
			return repository.GetTaskAssignment(ctx, task.ID)
		}
		if assignment.ExecutionMode != TaskExecutionModeRecoveryOnly || current.ReleaseRecovery == nil {
			return TaskAssignment{}, corruptTaskAssignment()
		}
		if err := repository.incrementAssignmentEpoch(ctx, current, nil); err != nil {
			if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
				return TaskAssignment{}, err
			}
			if err := repository.retryPolicy.waitAfterConflict(ctx, conflicts+1); err != nil {
				return TaskAssignment{}, err
			}
			continue
		}
		return repository.GetTaskAssignment(ctx, task.ID)
	}
}

type releaseRecoveryAcknowledgement struct {
	conditions []Condition
	record     releaseRecoveryRecord
	value      *KeyValue
	final      bool
	status     TaskStatus
	result     TaskResultRecord
}
