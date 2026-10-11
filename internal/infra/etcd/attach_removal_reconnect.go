package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) reconnectAttachRemovalAssignment(
	ctx context.Context, current TaskAssignment,
) (TaskAssignment, error) {
	task, assignment := current.Task.Record, current.Assignment.Record
	authority, sourceConditions, found, err := repository.attachRemovalRecoveryAuthorityAtRevision(
		ctx,
		task,
		current.Task.ReadRevision,
	)
	digest, digestErr := taskassignments.AttachRemovalRecoveryAuthoritySHA256(authority)
	if err != nil {
		return TaskAssignment{}, err
	}
	if !found || digestErr != nil || digest != assignment.AttachRemovalAuthoritySHA256 {
		return TaskAssignment{}, taskassignments.CorruptTaskAssignment()
	}
	if assignment.ExecutionMode == taskassignments.TaskExecutionModeRecoveryOnly {
		if current.AttachRemovalRecovery == nil {
			return TaskAssignment{}, taskassignments.CorruptTaskAssignment()
		}
		if err := repository.incrementAssignmentEpoch(ctx, current, sourceConditions); err != nil {
			return TaskAssignment{}, err
		}
		return repository.GetTaskAssignment(ctx, task.ID)
	}
	evidence, err := repository.attachRemovalMutationEvidenceAtRevision(
		ctx,
		task,
		assignment,
		authority,
		current.Task.ReadRevision,
	)
	if err != nil {
		return TaskAssignment{}, err
	}
	if len(evidence) == 0 {
		// Fencing the Task revision also fences journal publication: a start
		// racing this exact no-effect view invalidates the epoch transaction.
		if err := repository.incrementAssignmentEpoch(ctx, current, sourceConditions); err != nil {
			return TaskAssignment{}, err
		}
		return repository.GetTaskAssignment(ctx, task.ID)
	}
	taskBytes, err := EncodeTaskRecord(task)
	if err != nil {
		return TaskAssignment{}, err
	}
	defer clear(taskBytes)
	assignmentBytes, err := taskassignments.EncodeTaskAssignment(assignment)
	if err != nil {
		return TaskAssignment{}, err
	}
	defer clear(assignmentBytes)
	_, processed, err := repository.transitionAttachRemovalAcknowledgementToRecovery(
		ctx,
		task,
		&keyvalue.KeyValue{
			Key:         taskjournal.TaskStorageKey(task.ID),
			Value:       taskBytes,
			ModRevision: current.Task.Revision,
		},
		assignment,
		&keyvalue.KeyValue{Key: taskjournal.TaskExecutionClaimKey(assignment.Executor, assignment.AgentID, task.ID),
			Value: assignmentBytes, ModRevision: current.Assignment.Revision},
		&keyvalue.KeyValue{
			Key:         taskjournal.TaskAssignmentIndexKey(task.ID),
			Value:       assignmentBytes,
			ModRevision: current.Assignment.Revision,
		},
		taskjournal.TaskStatusFailed,
		taskjournal.TaskResultRecord{
			Kind: taskjournal.TaskResultCompose, Diagnostic: taskjournal.TaskResultDiagnosticNone,
			ReconciliationRequired: true, ExecutionEpoch: assignment.ExecutionEpoch,
		},
		current.Task.ReadRevision,
	)
	if err != nil {
		return TaskAssignment{}, err
	}
	if !processed {
		return TaskAssignment{}, errs.New(errs.KindStateConflict, "Detach reconnect raced execution evidence")
	}
	return repository.GetTaskAssignment(ctx, task.ID)
}
