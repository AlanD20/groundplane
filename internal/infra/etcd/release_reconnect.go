package etcd

import (
	"context"
	"errors"
	"math"

	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

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
		if taskHasClosingReport(task) {
			if terminal, closing, err := repository.resumeTaskClosingReport(ctx, current); err != nil || closing {
				return terminal, err
			}
		}
		if assignment.ExecutionEpoch == math.MaxUint32 {
			return TaskAssignment{}, errs.New(errs.KindStateConflict, "Agent reconnect assignment authority changed")
		}
		if assignment.AttachRemovalAuthoritySHA256 != "" {
			reconnected, reconnectErr := repository.reconnectAttachRemovalAssignment(ctx, current)
			if reconnectErr != nil && errors.Is(reconnectErr, errs.New(errs.KindStateConflict, "")) {
				if err := repository.retryPolicy.waitAfterConflict(ctx, conflicts+1); err != nil {
					return TaskAssignment{}, err
				}
				continue
			}
			return reconnected, reconnectErr
		}
		if assignment.ManagedNetworkProcedureSHA256 != "" {
			procedure, _, _, found, procedureErr := repository.managedNetworkRecoveryProcedureAtRevision(
				ctx, task, current.Task.ReadRevision,
			)
			digest, digestErr := managedNetworkRecoveryAuthorityDigest(procedure)
			if procedureErr != nil || !found || digestErr != nil || digest != assignment.ManagedNetworkProcedureSHA256 {
				return TaskAssignment{}, taskassignments.CorruptTaskAssignment()
			}
			if assignment.ExecutionMode == taskassignments.TaskExecutionModeForward {
				evidence, evidenceErr := repository.managedNetworkMutationEvidenceAtRevision(
					ctx, task, assignment, procedure, current.Task.ReadRevision,
				)
				if evidenceErr != nil {
					return TaskAssignment{}, evidenceErr
				}
				if len(evidence) != 0 {
					result := taskjournal.TaskResultRecord{Kind: taskjournal.TaskResultCompose,
						Diagnostic:             taskjournal.TaskResultDiagnosticNone,
						ReconciliationRequired: true, ExecutionEpoch: assignment.ExecutionEpoch}
					taskBytes, _ := EncodeTaskRecord(task)
					assignmentBytes, _ := taskassignments.EncodeTaskAssignment(assignment)
					transitioned, processed, transitionErr := repository.transitionManagedNetworkAcknowledgementToRecovery(
						ctx,
						task,
						&etcdstore.KeyValue{Key: taskjournal.TaskStorageKey(task.ID), Value: taskBytes,
							ModRevision: current.Task.Revision},
						assignment,
						&etcdstore.KeyValue{Key: taskjournal.TaskExecutionClaimKey(
							assignment.Executor, assignment.AgentID, task.ID,
						), Value: assignmentBytes, ModRevision: current.Assignment.Revision},
						&etcdstore.KeyValue{Key: taskjournal.TaskAssignmentIndexKey(task.ID), Value: assignmentBytes,
							ModRevision: current.Assignment.Revision},
						taskjournal.TaskStatusFailed,
						result,
						current.Task.ReadRevision,
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
				return TaskAssignment{}, errs.New(
					errs.KindStateConflict,
					"managed network assignment has no durable mutation effect to recover",
				)
			}
			if assignment.ExecutionMode != taskassignments.TaskExecutionModeRecoveryOnly ||
				current.ManagedNetworkRecovery == nil {
				return TaskAssignment{}, taskassignments.CorruptTaskAssignment()
			}
			if err := repository.incrementAssignmentEpoch(ctx, current, nil); err != nil {
				if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
					return TaskAssignment{}, err
				}
				continue
			}
			return repository.GetTaskAssignment(ctx, task.ID)
		}
		if assignment.ServiceAbsenceProcedureSHA256 != "" {
			procedure, _, _, found, procedureErr := repository.serviceAbsenceProcedureAtRevision(
				ctx, task, current.Task.ReadRevision,
			)
			digest, digestErr := serviceAbsenceAuthorityDigest(procedure)
			if procedureErr != nil || !found || digestErr != nil || digest != assignment.ServiceAbsenceProcedureSHA256 {
				return TaskAssignment{}, taskassignments.CorruptTaskAssignment()
			}
			if assignment.ExecutionMode == taskassignments.TaskExecutionModeForward {
				evidence, evidenceErr := repository.serviceAbsenceMutationEvidenceAtRevision(
					ctx, task, assignment, procedure, current.Task.ReadRevision,
				)
				if evidenceErr != nil {
					return TaskAssignment{}, evidenceErr
				}
				if len(evidence) != 0 {
					result := taskjournal.TaskResultRecord{Kind: taskjournal.TaskResultCompose,
						ReconciliationRequired: true, ExecutionEpoch: assignment.ExecutionEpoch}
					taskBytes, _ := EncodeTaskRecord(task)
					assignmentBytes, _ := taskassignments.EncodeTaskAssignment(assignment)
					transitioned, processed, transitionErr := repository.transitionServiceAbsenceAcknowledgementToRecovery(
						ctx,
						task,
						&etcdstore.KeyValue{
							Key:         taskjournal.TaskStorageKey(task.ID),
							Value:       taskBytes,
							ModRevision: current.Task.Revision,
						},
						assignment,
						&etcdstore.KeyValue{
							Key: taskjournal.TaskExecutionClaimKey(
								assignment.Executor,
								assignment.AgentID,
								task.ID,
							),
							Value:       assignmentBytes,
							ModRevision: current.Assignment.Revision,
						},
						&etcdstore.KeyValue{
							Key:         taskjournal.TaskAssignmentIndexKey(task.ID),
							Value:       assignmentBytes,
							ModRevision: current.Assignment.Revision,
						},
						taskjournal.TaskStatusFailed,
						result,
						current.Task.ReadRevision,
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
				return TaskAssignment{}, errs.New(
					errs.KindStateConflict,
					"Service absence assignment has no durable mutation effect to recover",
				)
			}
			if assignment.ExecutionMode != taskassignments.TaskExecutionModeRecoveryOnly ||
				current.ServiceAbsenceRecovery == nil {
				return TaskAssignment{}, taskassignments.CorruptTaskAssignment()
			}
			if err := repository.incrementAssignmentEpoch(ctx, current, nil); err != nil {
				if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
					return TaskAssignment{}, err
				}
				continue
			}
			return repository.GetTaskAssignment(ctx, task.ID)
		}
		if componentRecoveryTask(task) {
			_, procedure, _, procedureErr := repository.componentRecoveryProcedureAtRevision(
				ctx,
				task,
				current.Task.ReadRevision,
			)
			digest, digestErr := componentRecoveryAuthorityDigest(procedure)
			if procedureErr != nil || digestErr != nil || digest != assignment.ComponentRecoveryProcedureSHA256 {
				return TaskAssignment{}, taskassignments.CorruptTaskAssignment()
			}
			if assignment.ExecutionMode == taskassignments.TaskExecutionModeForward {
				result := taskjournal.TaskResultRecord{Kind: taskjournal.TaskResultCompose,
					ReconciliationRequired: true, ExecutionEpoch: assignment.ExecutionEpoch}
				taskBytes, _ := EncodeTaskRecord(task)
				assignmentBytes, _ := taskassignments.EncodeTaskAssignment(assignment)
				transitioned, processed, transitionErr := repository.transitionComponentAcknowledgementToRecovery(
					ctx,
					task,
					&etcdstore.KeyValue{
						Key:         taskjournal.TaskStorageKey(task.ID),
						Value:       taskBytes,
						ModRevision: current.Task.Revision,
					},
					assignment,
					&etcdstore.KeyValue{
						Key: taskjournal.TaskExecutionClaimKey(
							assignment.Executor,
							assignment.AgentID,
							task.ID,
						),
						Value:       assignmentBytes,
						ModRevision: current.Assignment.Revision,
					},
					&etcdstore.KeyValue{
						Key:         taskjournal.TaskAssignmentIndexKey(task.ID),
						Value:       assignmentBytes,
						ModRevision: current.Assignment.Revision,
					},
					taskjournal.TaskStatusFailed,
					result,
					current.Task.ReadRevision,
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
			if assignment.ExecutionMode != taskassignments.TaskExecutionModeRecoveryOnly ||
				current.ComponentRecovery == nil {
				return TaskAssignment{}, taskassignments.CorruptTaskAssignment()
			}
			if err := repository.incrementAssignmentEpoch(ctx, current, nil); err != nil {
				if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
					return TaskAssignment{}, err
				}
				continue
			}
			return repository.GetTaskAssignment(ctx, task.ID)
		}
		if !releaseRecoveryTask(task) {
			return current, nil
		}
		procedure, sourceConditions, err := repository.releaseRecoveryProcedureAtRevision(
			ctx,
			task,
			current.Task.ReadRevision,
		)
		if err != nil || validateAssignmentRestorationDescriptor(task, assignment, procedure) != nil {
			return TaskAssignment{}, taskassignments.CorruptTaskAssignment()
		}
		if assignment.ExecutionMode == taskassignments.TaskExecutionModeForward {
			effect, evidenceConditions, classifyErr := repository.releaseEffectEvidenceAtRevision(
				ctx, task, assignment, procedure, current.Task.ReadRevision,
			)
			if classifyErr != nil {
				return TaskAssignment{}, classifyErr
			}
			if effect {
				result := taskjournal.TaskResultRecord{
					Kind: taskjournal.TaskResultCompose, Diagnostic: taskjournal.TaskResultDiagnosticNone,
					ReconciliationRequired: true, ExecutionEpoch: assignment.ExecutionEpoch,
				}
				taskBytes, _ := EncodeTaskRecord(task)
				assignmentBytes, _ := taskassignments.EncodeTaskAssignment(assignment)
				transitioned, processed, transitionErr := repository.transitionReleaseAcknowledgementToRecovery(
					ctx,
					task,
					&etcdstore.KeyValue{
						Key:         taskjournal.TaskStorageKey(task.ID),
						Value:       taskBytes,
						ModRevision: current.Task.Revision,
					},
					assignment,
					&etcdstore.KeyValue{
						Key: taskjournal.TaskExecutionClaimKey(
							assignment.Executor,
							assignment.AgentID,
							task.ID,
						),
						Value:       assignmentBytes,
						ModRevision: current.Assignment.Revision,
					},
					&etcdstore.KeyValue{
						Key:         taskjournal.TaskAssignmentIndexKey(task.ID),
						Value:       assignmentBytes,
						ModRevision: current.Assignment.Revision,
					},
					taskjournal.TaskStatusFailed,
					result,
					current.Task.ReadRevision,
					append(sourceConditions, evidenceConditions...)...,
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
			if err := repository.incrementAssignmentEpoch(ctx, current, append(sourceConditions, evidenceConditions...)); err != nil {
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
		if assignment.ExecutionMode != taskassignments.TaskExecutionModeRecoveryOnly || current.ReleaseRecovery == nil {
			return TaskAssignment{}, taskassignments.CorruptTaskAssignment()
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
	conditions []etcdstore.Condition
	record     taskassignments.ReleaseRecoveryRecord
	value      *etcdstore.KeyValue
	final      bool
	status     taskjournal.TaskStatus
	result     taskjournal.TaskResultRecord
}
