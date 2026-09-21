package etcd

import (
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func validateServiceRemovalTaskOwner(task TaskRecord, intent environmentchanges.ServiceRemovalIntent) error {
	if task.ID != intent.TaskID || task.Executor != taskjournal.TaskExecutorAgent || task.Type != taskjournal.TaskRemove ||
		task.Target != intent.ServiceID || !task.CreatedAt.Equal(intent.CreatedAt) ||
		len(task.Params) != 4 ||
		task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceService ||
		task.Params[taskjournal.TaskServiceEnvironmentParam] != intent.EnvironmentID ||
		task.Params[blueprints.EnvironmentDesiredRevisionParam] != intent.Claim.RevisionID ||
		task.Params[taskjournal.TaskComposeArtifactParam] == "" {
		return errs.New(errs.KindStateConflict, "Service removal intent does not belong to its Task")
	}
	return nil
}
