package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

func authoredBlueprintServiceRemovalTask(task TaskRecord) bool {
	return task.Actor == taskjournal.TaskActorSystem && task.Executor == taskjournal.TaskExecutorAgent &&
		task.Type == taskjournal.TaskRemove && ids.Validate(ids.KindService, task.Target) == nil &&
		len(task.Params) == 6 && task.Params[taskjournal.TaskResourceKindParam] == taskjournal.TaskResourceService &&
		task.Params[taskjournal.TaskBlueprintServiceUnitParam] == task.Target &&
		ids.Validate(ids.KindTask, task.Params[taskjournal.TaskBlueprintParentParam]) == nil &&
		task.Params[taskjournal.TaskBlueprintParentParam] == task.Params[blueprints.EnvironmentDesiredRevisionParam] &&
		task.Params[taskjournal.TaskServiceEnvironmentParam] == task.Owner.EnvironmentID &&
		ids.Validate(ids.KindConfig, task.Params[taskjournal.TaskComposeArtifactParam]) == nil
}

// IsAuthoredBlueprintServiceRemovalTask identifies the private protected
// Service cleanup shape executed as Blueprint release recovery.
func IsAuthoredBlueprintServiceRemovalTask(task TaskRecord) bool {
	return authoredBlueprintServiceRemovalTask(task)
}
