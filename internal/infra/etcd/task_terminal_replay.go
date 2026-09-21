package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) acknowledgeTerminalTaskReplay(
	ctx context.Context,
	task TaskRecord,
	taskValue, assignmentIndexValue *etcdstore.KeyValue,
	readRevision int64,
	executor taskjournal.TaskExecutor,
	agentID string, agentGeneration uint64, assignmentID string,
	terminalStatus taskjournal.TaskStatus, result *taskjournal.TaskResultRecord,
	environmentID string, environmentRemoval, zoneRemoval bool,
) (etcdstore.Versioned[TaskRecord], error) {
	if assignmentIndexValue != nil {
		return etcdstore.Versioned[TaskRecord]{}, errs.New(errs.KindInternal, "task assignment index is orphaned")
	}
	if executor == taskjournal.TaskExecutorAgent && result != nil &&
		task.Params[releaserender.TaskReleasePublicationParam] != "" {
		normalizedStatus, normalizedResult, handled, normalizeErr := repository.normalizeReleaseRecoveryTerminalReplay(
			ctx,
			task,
			terminalStatus,
			*result,
			agentID,
			agentGeneration,
			assignmentID,
			readRevision,
		)
		if normalizeErr != nil {
			return etcdstore.Versioned[TaskRecord]{}, normalizeErr
		}
		if handled {
			terminalStatus, result = normalizedStatus, normalizedResult
		}
	}
	if task.Status == terminalStatus &&
		((result == nil && task.Result == nil) ||
			(result != nil && task.Result != nil && taskjournal.TaskResultsEqual(*task.Result, *result))) {
		if executor == taskjournal.TaskExecutorAgent {
			expected := taskjournal.TaskTerminalAssignmentRecord{
				AssignmentID: assignmentID, AgentID: agentID, AgentGeneration: agentGeneration,
			}
			if task.TerminalAssignment == nil || *task.TerminalAssignment != expected {
				return etcdstore.Versioned[TaskRecord]{}, errs.New(
					errs.KindStateConflict,
					"task terminal assignment identity does not match",
				)
			}
		}
		if err := repository.validateTaskAcknowledgementReplay(
			ctx, task, terminalStatus, readRevision,
			environmentID, environmentRemoval, zoneRemoval,
		); err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		return etcdstore.Versioned[TaskRecord]{
			Record: task, Revision: taskValue.ModRevision,
			ReadRevision: readRevision,
		}, nil
	}
	return etcdstore.Versioned[TaskRecord]{}, errs.New(errs.KindStateConflict, "task has no matching active assignment")
}
