package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// blueprintNetworkChildTaskMatches closes the durable shape used by one
// authored Zone ensure. The exact artifact and step are reconstructed from the
// immutable desired revision by taskplanning before Agent dispatch.
func blueprintNetworkChildTaskMatches(task TaskRecord, unit blueprintunits.Unit) bool {
	parentID := task.Params[taskjournal.TaskBlueprintParentParam]
	return unit.Target.Kind == ids.KindNetwork && !unit.Removal && unit.Target.ID == task.Target &&
		task.Type == taskjournal.TaskUpdate && task.Actor == taskjournal.TaskActorSystem &&
		task.Executor == taskjournal.TaskExecutorAgent &&
		task.Params[taskjournal.TaskBlueprintNetworkUnitParam] == task.Target &&
		task.Params[blueprints.EnvironmentDesiredRevisionParam] == parentID &&
		ids.Validate(ids.KindTask, parentID) == nil && parentID != task.ID &&
		len(task.Params) == 3 && len(task.Steps) == 1 &&
		task.Steps[0].Kind == taskjournal.TaskStepOperation &&
		len(task.Materializations) == 0 && task.EntryRuntime == nil && task.Configuration == nil &&
		len(task.ComponentActionStepIDs) == 0 && len(task.ManagedComponentTeardownSources) == 0
}
