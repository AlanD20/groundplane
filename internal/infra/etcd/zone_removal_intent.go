package etcd

import (
	"context"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyRepository) GetZoneRemovalIntent(
	ctx context.Context,
	operationID string,
) (etcdstore.Versioned[environmentchanges.ZoneRemovalIntent], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[environmentchanges.ZoneRemovalIntent]{}, false, err
	}
	if ids.Validate(ids.KindOperation, operationID) != nil {
		return etcdstore.Versioned[environmentchanges.ZoneRemovalIntent]{}, false, errs.New(
			errs.KindValidationFailed,
			"Zone removal operation id is invalid",
		)
	}
	result, err := repository.store.Get(ctx, environmentchanges.ZoneRemovalIntentKey(operationID))
	if err != nil {
		return etcdstore.Versioned[environmentchanges.ZoneRemovalIntent]{}, false, err
	}
	if result == nil {
		return etcdstore.Versioned[environmentchanges.ZoneRemovalIntent]{}, false, errs.New(errs.KindInternal, "Zone removal intent read is empty")
	}
	if result.Entry == nil {
		return etcdstore.Versioned[environmentchanges.ZoneRemovalIntent]{ReadRevision: result.ReadRevision}, false, nil
	}
	intent, err := environmentchanges.DecodeZoneRemovalIntent(result.Entry.Value)
	if err != nil || intent.OperationID != operationID {
		return etcdstore.Versioned[environmentchanges.ZoneRemovalIntent]{}, false, environmentchanges.CorruptZoneRemovalIntent()
	}
	return etcdstore.Versioned[environmentchanges.ZoneRemovalIntent]{
		Record:       intent,
		Revision:     result.Entry.ModRevision,
		ReadRevision: result.ReadRevision,
	}, true, nil
}

func validateZoneRemovalTaskOwner(task TaskRecord, intent environmentchanges.ZoneRemovalIntent) error {
	if task.ID != intent.ActiveTaskID || task.Target != intent.ZoneID || task.Type != taskjournal.TaskRemove ||
		!task.CreatedAt.Equal(
			intent.ActiveTaskCreatedAt,
		) || task.Params[taskjournal.TaskZoneRemovalOperationParam] != intent.OperationID ||
		task.Params[taskjournal.TaskZoneEnvironmentParam] != intent.EnvironmentID ||
		task.Params[blueprints.EnvironmentDesiredRevisionParam] != intent.Claim.RevisionID {
		return errs.New(errs.KindStateConflict, "Zone removal intent does not belong to its Task")
	}
	if task.Executor == taskjournal.TaskExecutorAgent {
		if len(task.Params) != 4 || ids.Validate(ids.KindConfig, task.Params[taskjournal.TaskComposeArtifactParam]) != nil {
			return errs.New(errs.KindStateConflict, "Zone removal Agent Task input changed")
		}
		return nil
	}
	if task.Executor != taskjournal.TaskExecutorController || len(task.Params) != 5 ||
		task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceBackingZone || !recordcodec.ValidSHA256(task.Params[taskjournal.TaskZoneImpactTokenParam]) {
		return errs.New(errs.KindStateConflict, "Zone removal Controller Task input changed")
	}
	return nil
}
