package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// A retry observation is not terminal proof. Keep the original Task bytes and
// resource ownership unchanged while durably accounting for a fresh failure.
func (repository *TaskRepository) acknowledgeUnsettledRecovery(
	ctx context.Context, task TaskRecord, taskValue, assignmentValue *keyvalue.KeyValue,
	assignment taskassignments.TaskAssignmentRecord, status taskjournal.TaskStatus,
	result taskjournal.TaskResultRecord, revision int64, now time.Time,
) (keyvalue.Versioned[TaskRecord], error) {
	if status != taskjournal.TaskStatusCompleted && result.ExecutionEpoch == assignment.ExecutionEpoch {
		if err := repository.recordRecoveryFailure(ctx, task, taskValue, assignmentValue,
			assignment, result.Failure, revision, now); err != nil {
			return keyvalue.Versioned[TaskRecord]{}, err
		}
	}
	return keyvalue.Versioned[TaskRecord]{Record: task, Revision: taskValue.ModRevision, ReadRevision: revision}, nil
}

func (repository *TaskRepository) readRecoveryRetryControl(
	ctx context.Context, taskID string, revision int64,
) (*taskassignments.RecoveryRetryControl, int64, error) {
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{taskassignments.RecoveryRetryControlKey(taskID)}, Revision: revision,
	})
	if err != nil {
		return nil, 0, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return nil, 0, taskassignments.CorruptTaskAssignment()
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return nil, 0, nil
	}
	control, err := taskassignments.DecodeRecoveryRetryControl(read.Values[0].Value)
	if err != nil || control.TaskID != taskID {
		return nil, 0, taskassignments.CorruptTaskAssignment()
	}
	return &control, read.Values[0].ModRevision, nil
}

func (repository *TaskRepository) recordRecoveryFailure(
	ctx context.Context, task TaskRecord, taskValue, assignmentValue *keyvalue.KeyValue,
	assignment taskassignments.TaskAssignmentRecord, failure *taskprogress.Failure, revision int64, now time.Time,
) error {
	previous, controlRevision, err := repository.readRecoveryRetryControl(ctx, task.ID, revision)
	if err != nil {
		return err
	}
	// Native Detach may include an operator hook with uncertain effects. Its
	// captured one-use Retry permit, not a transient error, authorizes replay.
	safeToReplay := assignment.ExecutionMode == taskassignments.TaskExecutionModeRecoveryOnly &&
		assignment.AttachRemovalAuthoritySHA256 == "" && assignmentRecoveryAuthorityDigest(assignment) != ""
	control, changed, err := taskassignments.ObserveRecoveryFailure(previous, assignment, failure, now, safeToReplay)
	if err != nil || !changed {
		return err
	}
	value, err := taskassignments.EncodeRecoveryRetryControl(control)
	if err != nil {
		return err
	}
	defer clear(value)
	result, err := repository.store.Transact(ctx, []keyvalue.Condition{
		{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: taskValue.ModRevision},
		{
			Key:         taskjournal.TaskExecutionClaimKey(assignment.Executor, assignment.AgentID, task.ID),
			ModRevision: assignmentValue.ModRevision,
		},
		{Key: taskassignments.RecoveryRetryControlKey(task.ID), ModRevision: controlRevision},
	}, []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: taskassignments.RecoveryRetryControlKey(task.ID), Value: value}})
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "recovery retry control changed during acknowledgement")
	}
	return nil
}
