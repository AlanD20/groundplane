package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type taskAcknowledgementSource struct {
	read  *etcdstore.GetManyResult
	value *etcdstore.KeyValue
	task  TaskRecord
}

// readTaskAcknowledgementSource captures the journal and assignment together and
// checks execution authority before any terminal preparation.
func (repository *TaskRepository) readTaskAcknowledgementSource(
	ctx context.Context,
	executor taskjournal.TaskExecutor,
	taskID, claimKey string,
) (taskAcknowledgementSource, error) {
	primaryAndAssignment, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		taskjournal.TaskStorageKey(taskID), claimKey, taskjournal.TaskAssignmentIndexKey(taskID),
	}})
	if err != nil {
		return taskAcknowledgementSource{}, err
	}
	if len(primaryAndAssignment.Values) != 3 || primaryAndAssignment.Values[0] == nil {
		return taskAcknowledgementSource{}, errs.Newf(errs.KindTaskNotFound, "task not found: %s", taskID)
	}
	taskValue := primaryAndAssignment.Values[0]
	task, err := DecodeTaskRecord(taskValue.Value)
	if err != nil {
		return taskAcknowledgementSource{}, err
	}
	if task.Executor != executor {
		return taskAcknowledgementSource{}, errs.New(
			errs.KindStateConflict,
			"task execution authority changed",
		)
	}
	return taskAcknowledgementSource{read: primaryAndAssignment, value: taskValue, task: task}, nil
}
