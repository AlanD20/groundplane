package etcd

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func validateAuthoredAttachRemovalTaskOwner(
	task TaskRecord,
	intent environmentchanges.AttachRemovalIntent,
) error {
	if environmentchanges.ValidateAttachRemovalIntent(intent) != nil || task.ID != intent.TaskID ||
		task.Owner.EnvironmentID != intent.EnvironmentID || task.Type != taskjournal.TaskDetach ||
		task.Target != intent.AttachID || task.Params[taskjournal.TaskBlueprintParentParam] != intent.ParentTaskID ||
		task.Params[blueprints.EnvironmentDesiredRevisionParam] != intent.ParentTaskID ||
		task.Params[taskjournal.TaskMutationEnvironmentParam] != intent.EnvironmentID {
		return errs.New(errs.KindValidationFailed, "Blueprint Attach removal Task ownership changed")
	}
	return nil
}
