package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

func authoredBlueprintEntryRemovalTask(task TaskRecord) bool {
	return task.Actor == taskjournal.TaskActorSystem &&
		task.Executor == taskjournal.TaskExecutorAgent &&
		task.Type == taskjournal.TaskRemove &&
		ids.Validate(ids.KindEnvEntry, task.Target) == nil &&
		len(task.Params) == 10 &&
		task.Params[taskjournal.TaskBlueprintEntryUnitParam] == task.Target &&
		ids.Validate(ids.KindTask, task.Params[taskjournal.TaskBlueprintParentParam]) == nil &&
		task.Params[taskjournal.TaskBlueprintParentParam] == task.Params[blueprints.EnvironmentDesiredRevisionParam] &&
		task.Params[taskjournal.TaskEntryEnvironmentParam] == task.Owner.EnvironmentID &&
		task.Params[taskjournal.TaskMaterializationEnvironmentParam] == task.Owner.EnvironmentID
}

// IsAuthoredBlueprintEntryRemovalTask identifies the one private Entry cleanup
// shape whose file-only restoration runs through Blueprint release execution.
func IsAuthoredBlueprintEntryRemovalTask(task TaskRecord) bool {
	return authoredBlueprintEntryRemovalTask(task)
}

func releaseRecoveryTask(task TaskRecord) bool {
	return task.Params[releaserender.TaskReleasePublicationParam] != "" || authoredBlueprintEntryRemovalTask(task)
}
