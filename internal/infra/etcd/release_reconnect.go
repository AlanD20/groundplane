package etcd

import (
	"context"
	"errors"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
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
		if task.Params[releaserender.TaskReleasePublicationParam] == "" {
			return current, nil
		}
		_, procedure, err := repository.candidateReleaseDescriptorAtRevision(ctx, task, current.Task.ReadRevision)
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
					&etcdstore.KeyValue{Key: taskjournal.TaskStorageKey(task.ID), Value: taskBytes, ModRevision: current.Task.Revision},
					assignment,
					&etcdstore.KeyValue{
						Key:         taskjournal.TaskExecutionClaimKey(assignment.Executor, assignment.AgentID, task.ID),
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
