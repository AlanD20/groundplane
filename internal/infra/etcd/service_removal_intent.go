package etcd

import (
	"context"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *ServiceRepository) GetServiceRemovalIntent(
	ctx context.Context,
	taskID string,
) (etcdstore.Versioned[environmentchanges.ServiceRemovalIntent], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[environmentchanges.ServiceRemovalIntent]{}, false, err
	}
	if ids.Validate(ids.KindTask, taskID) != nil {
		return etcdstore.Versioned[environmentchanges.ServiceRemovalIntent]{}, false, errs.New(
			errs.KindValidationFailed,
			"Service removal Task id is invalid",
		)
	}
	result, err := repository.store.Get(ctx, environmentchanges.ServiceRemovalIntentKey(taskID))
	if err != nil {
		return etcdstore.Versioned[environmentchanges.ServiceRemovalIntent]{}, false, err
	}
	if result == nil {
		return etcdstore.Versioned[environmentchanges.ServiceRemovalIntent]{}, false, errs.New(
			errs.KindInternal,
			"Service removal intent read is empty",
		)
	}
	if result.Entry == nil {
		return etcdstore.Versioned[environmentchanges.ServiceRemovalIntent]{ReadRevision: result.ReadRevision}, false, nil
	}
	intent, err := environmentchanges.DecodeServiceRemovalIntent(result.Entry.Value)
	if err != nil || intent.TaskID != taskID {
		return etcdstore.Versioned[environmentchanges.ServiceRemovalIntent]{}, false, environmentchanges.CorruptServiceRemovalIntent()
	}
	return etcdstore.Versioned[environmentchanges.ServiceRemovalIntent]{
		Record:       intent,
		Revision:     result.Entry.ModRevision,
		ReadRevision: result.ReadRevision,
	}, true, nil
}

func validateServiceRemovalTaskOwner(task TaskRecord, intent environmentchanges.ServiceRemovalIntent) error {
	if task.ID != intent.TaskID || task.Executor != taskjournal.TaskExecutorAgent || task.Type != taskjournal.TaskRemove ||
		task.Target != intent.ServiceID || !task.CreatedAt.Equal(intent.CreatedAt) || len(task.Params) != 4 ||
		task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceService ||
		task.Params[taskjournal.TaskServiceEnvironmentParam] != intent.EnvironmentID ||
		task.Params[blueprints.EnvironmentDesiredRevisionParam] != intent.Claim.RevisionID ||
		task.Params[taskjournal.TaskComposeArtifactParam] == "" {
		return errs.New(errs.KindStateConflict, "Service removal intent does not belong to its Task")
	}
	return nil
}
