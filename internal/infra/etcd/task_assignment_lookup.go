package etcd

import (
	"bytes"
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// GetTaskAssignment resolves the one durable execution claim through the
// Task-indexed copy and verifies the executor-owned copy at the same revision.
func (repository *TaskRepository) GetTaskAssignment(
	ctx context.Context,
	taskID string,
) (TaskAssignment, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return TaskAssignment{}, err
	}
	if recordcodec.ValidateID(ids.KindTask, taskID) != nil {
		return TaskAssignment{}, errs.New(errs.KindValidationFailed, "Task assignment id is invalid")
	}
	indexed, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		taskjournal.TaskStorageKey(taskID), taskjournal.TaskAssignmentIndexKey(taskID),
	}})
	if err != nil {
		return TaskAssignment{}, err
	}
	if len(indexed.Values) != 2 || indexed.Values[0] == nil {
		etcdstore.ClearValues(indexed.Values)
		return TaskAssignment{}, errs.Newf(errs.KindTaskNotFound, "task not found: %s", taskID)
	}
	taskValue := indexed.Values[0]
	indexValue := indexed.Values[1]
	defer clear(taskValue.Value)
	if indexValue == nil {
		return TaskAssignment{}, errs.New(errs.KindStateConflict, "Task has no active assignment")
	}
	defer clear(indexValue.Value)
	task, err := DecodeTaskRecord(taskValue.Value)
	if err != nil {
		return TaskAssignment{}, err
	}
	assignment, err := taskassignments.DecodeTaskAssignment(indexValue.Value)
	if err != nil {
		return TaskAssignment{}, err
	}
	if task.ID != taskID || task.Status != taskjournal.TaskStatusRunning || task.Executor != assignment.Executor ||
		assignment.TaskID != taskID {
		return TaskAssignment{}, errs.New(errs.KindStateConflict, "Task is not actively assigned")
	}
	claimKey := taskjournal.TaskExecutionClaimKey(assignment.Executor, assignment.AgentID, taskID)
	claim, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys:     []string{claimKey},
		Revision: indexed.ReadRevision,
	})
	if err != nil {
		return TaskAssignment{}, err
	}
	if len(claim.Values) != 1 || claim.Values[0] == nil {
		etcdstore.ClearValues(claim.Values)
		return TaskAssignment{}, errs.New(errs.KindStateConflict, "Task execution claim changed")
	}
	claimValue := claim.Values[0]
	defer clear(claimValue.Value)
	if claimValue.ModRevision != indexValue.ModRevision || !bytes.Equal(claimValue.Value, indexValue.Value) {
		return TaskAssignment{}, errs.New(errs.KindInternal, "Task assignment index does not match its claim")
	}
	_, _, proofRequired, err := repository.assignmentLifecycleIndexAtRevision(
		ctx, assignment, indexValue, indexed.ReadRevision,
	)
	if err != nil {
		return TaskAssignment{}, err
	}
	var recovery *taskassignments.ReleaseRecoveryDirective
	if task.Params[releaserender.TaskReleasePublicationParam] != "" {
		_, procedure, descriptorErr := repository.candidateReleaseDescriptorAtRevision(ctx, task, indexed.ReadRevision)
		if descriptorErr != nil || validateAssignmentRestorationDescriptor(task, assignment, procedure) != nil {
			return TaskAssignment{}, taskassignments.CorruptTaskAssignment()
		}
		if assignment.ExecutionMode == taskassignments.TaskExecutionModeRecoveryOnly {
			recovery, err = repository.releaseRecoveryDirectiveAtRevision(
				ctx, task, assignment, procedure, indexed.ReadRevision,
			)
			if err != nil {
				return TaskAssignment{}, err
			}
		}
	}
	return TaskAssignment{
		Assignment: etcdstore.Versioned[taskassignments.TaskAssignmentRecord]{
			Record: assignment, Revision: claimValue.ModRevision, ReadRevision: indexed.ReadRevision,
		},
		Task: etcdstore.Versioned[TaskRecord]{
			Record: task, Revision: taskValue.ModRevision, ReadRevision: indexed.ReadRevision,
		},
		ReleaseRecovery:       recovery,
		RecoveryProofRequired: proofRequired,
	}, nil
}
