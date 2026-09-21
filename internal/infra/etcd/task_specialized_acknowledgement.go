package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

func (repository *TaskRepository) acknowledgeSpecializedTask(
	ctx context.Context, task TaskRecord, executor taskjournal.TaskExecutor,
	agentID string, agentGeneration uint64, taskID, assignmentID string,
	terminalStatus taskjournal.TaskStatus, result *taskjournal.TaskResultRecord, terminalAt time.Time,
) (etcdstore.Versioned[TaskRecord], bool, error) {
	if task.Params[taskjournal.TaskResourceKindParam] == taskjournal.TaskResourceHierarchyDeletion {
		if executor == taskjournal.TaskExecutorController {
			acknowledged, err := repository.acknowledgeHierarchyDeletionControllerTask(
				ctx, taskID, terminalStatus, terminalAt,
			)
			return acknowledged, true, err
		}
		if result == nil {
			return etcdstore.Versioned[TaskRecord]{}, true, errs.New(
				errs.KindStateConflict,
				"hierarchy deletion Agent Task requires a result",
			)
		}
		acknowledged, err := repository.acknowledgeHierarchyDeletionAgentTask(
			ctx, agentID, agentGeneration, taskID, assignmentID,
			terminalStatus, *result, terminalAt,
		)
		return acknowledged, true, err
	}
	if task.Type == taskjournal.TaskBackup || task.Type == taskjournal.TaskBackupPrune {
		if executor != taskjournal.TaskExecutorAgent || result == nil {
			return etcdstore.Versioned[TaskRecord]{}, true, errs.New(
				errs.KindStateConflict,
				"backup Task requires an Agent acknowledgement",
			)
		}
		acknowledged, err := repository.acknowledgeBackupTask(
			ctx,
			agentID,
			agentGeneration,
			taskID,
			assignmentID,
			terminalStatus,
			*result,
			terminalAt,
		)
		return acknowledged, true, err
	}
	return etcdstore.Versioned[TaskRecord]{}, false, nil
}
