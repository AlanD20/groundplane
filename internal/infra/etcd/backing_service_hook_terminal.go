package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) prepareBackingServiceCreationHookAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	assignment taskassignments.TaskAssignmentRecord,
	readRevision int64,
) (taskMaterializationProjectionChange, error) {
	serviceID, hooked := task.Params[TaskBackingServiceAfterStartParam]
	if !hooked || task.Status != taskjournal.TaskStatusCompleted {
		return taskMaterializationProjectionChange{}, nil
	}
	if serviceID == "" || task.Params[taskjournal.TaskBackingServiceCreationParam] != serviceID || len(task.Steps) == 0 {
		return taskMaterializationProjectionChange{}, errs.New(
			errs.KindInternal,
			"Backing-service after-start terminal Task is incomplete",
		)
	}
	checkpoint, condition, err := repository.requireBackingHookResultCheckpoint(
		ctx,
		task,
		assignment,
		task.Steps[len(task.Steps)-1].ID,
		"",
		backinghook.AfterStart,
		readRevision,
	)
	if err != nil {
		return taskMaterializationProjectionChange{}, err
	}
	if checkpoint.Facts != nil {
		clear(checkpoint.Facts.Ciphertext)
		return taskMaterializationProjectionChange{}, errs.New(
			errs.KindInternal,
			"Backing-service after-start checkpoint contains facts",
		)
	}
	return taskMaterializationProjectionChange{
		applies:    true,
		conditions: []etcdstore.Condition{condition},
	}, nil
}
