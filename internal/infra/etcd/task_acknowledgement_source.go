package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type taskAcknowledgementSource struct {
	read  *etcdstore.GetManyResult
	value *etcdstore.KeyValue
	task  TaskRecord
}

// taskAcknowledgementRemovalScope classifies the remaining generic removals.
// Environment provisioning has only its native work-item report path.
func taskAcknowledgementRemovalScope(
	task TaskRecord,
	executor taskjournal.TaskExecutor,
) (bool, bool, error) {
	environmentTask := executor == taskjournal.TaskExecutorAgent &&
		recordcodec.ValidateID(ids.KindEnvironment, task.Target) == nil
	if environmentTask && task.Type == taskjournal.TaskCreate {
		return false, false, errs.New(errs.KindStateConflict, "Environment provisioning requires native execution reports")
	}
	environmentRemoval := environmentTask && task.Type == taskjournal.TaskRemove
	zoneRemoval := executor == taskjournal.TaskExecutorAgent && task.Type == taskjournal.TaskRemove &&
		recordcodec.ValidateID(ids.KindNetwork, task.Target) == nil &&
		task.Params[taskjournal.TaskZoneRemovalOperationParam] != ""
	return environmentRemoval, zoneRemoval, nil
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
