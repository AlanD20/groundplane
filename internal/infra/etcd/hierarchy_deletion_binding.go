package etcd

import (
	"context"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	hierarchydeletionplanning "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionplanning"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyDeletionRepository) BindActions(
	ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionOperation,
	planned []hierarchydeletionplanning.HierarchyDeletionPlannedAction,
) ([]hierarchydeletion.HierarchyDeletionAction, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return nil, err
	}
	current, err := repository.OperationByTask(ctx, operation.Tombstone.CurrentTaskID)
	if err != nil {
		return nil, err
	}
	if current.Tombstone.Phase != hierarchydeletion.HierarchyDeletionPlanning || current.Tombstone.PlanCount != nil ||
		current.Tombstone.PlanDigest != nil || current.Tombstone.OperationID != operation.Tombstone.OperationID {
		return nil, errs.New(errs.KindStateConflict, "hierarchy deletion plan is not bindable")
	}
	bound := make([]hierarchydeletion.HierarchyDeletionAction, len(planned))
	for index := range planned {
		bound[index], err = hierarchydeletionplanning.BindHierarchyDeletionAction(current, planned[index])
		if err != nil {
			return nil, err
		}
	}
	return bound, nil
}
