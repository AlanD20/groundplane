package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	attachinputs "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

func authoredBlueprintAttachRemovalTask(task TaskRecord) bool {
	parentID := task.Params[taskjournal.TaskBlueprintParentParam]
	attachID := task.Params[attachinputs.TaskAttachIDParam]
	return task.Type == taskjournal.TaskDetach && task.Target == attachID &&
		task.Actor == taskjournal.TaskActorSystem && task.Executor == taskjournal.TaskExecutorAgent &&
		task.Owner.EnvironmentID == task.Params[taskjournal.TaskMutationEnvironmentParam] &&
		task.Params[blueprints.EnvironmentDesiredRevisionParam] == parentID &&
		ids.Validate(ids.KindTask, parentID) == nil && parentID != task.ID &&
		ids.Validate(ids.KindAttach, attachID) == nil && len(task.Params) == 4
}
