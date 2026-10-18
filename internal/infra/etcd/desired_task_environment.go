package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

func desiredRevisionTaskEnvironment(task TaskRecord) (string, bool, error) {
	if task.Executor == taskjournal.TaskExecutorBlueprint && task.Type == taskjournal.TaskUpdate &&
		ids.Validate(ids.KindEnvironment, task.Owner.EnvironmentID) == nil && task.Target == task.Owner.EnvironmentID &&
		task.Params[blueprints.EnvironmentDesiredRevisionParam] == task.ID {
		return task.Owner.EnvironmentID, true, nil
	}
	return taskMaterializationEnvironment(task)
}
