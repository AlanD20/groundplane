package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// Component children address their Environment while their unit identifies the
// singleton. Publication, dispatch and settlement share this identity check;
// the stored execution recipe still proves the exact Component and plan.
func blueprintComponentChildTaskMatches(task TaskRecord, unit blueprintunits.Unit) bool {
	parentID := task.Params[taskjournal.TaskBlueprintParentParam]
	return unit.Target.Kind == ids.KindComponent && ids.Validate(ids.KindComponent, unit.Target.ID) == nil &&
		task.Params[taskjournal.TaskBlueprintComponentUnitParam] == unit.Target.ID &&
		task.Type == taskjournal.TaskUpdate && task.Actor == taskjournal.TaskActorSystem &&
		task.Executor == taskjournal.TaskExecutorAgent && task.Target == task.Owner.EnvironmentID &&
		ids.Validate(ids.KindEnvironment, task.Target) == nil &&
		task.Params[taskjournal.TaskMaterializationEnvironmentParam] == task.Owner.EnvironmentID &&
		task.Params[blueprints.EnvironmentDesiredRevisionParam] == parentID &&
		ids.Validate(ids.KindTask, parentID) == nil && parentID != task.ID && len(task.Params) == 4
}
