package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"

	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionServiceFinalizer(
	ctx context.Context,
	action HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	primary, err := repository.readHierarchyDeletionPrimary(ctx, servicerecord.ServiceRuntimeKey(action.TargetID), action)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	defer clear(primary.Value)
	record, err := servicerecord.DecodeServiceRuntimeRecord(primary.Value)
	if err != nil || record.ServiceID != action.TargetID {
		return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
	}
	activeKey := serviceLifecycleActiveKey(record.ServiceID)
	active, err := repository.store.Get(ctx, activeKey)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	if active == nil {
		return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
	}
	if active.Entry != nil {
		clear(active.Entry.Value)
		return hierarchyDeletionControllerEffects{}, errs.New(
			errs.KindStateConflict,
			"Service lifecycle is still active during hierarchy metadata finalization",
		)
	}
	scriptConditions, err := prepareServiceScriptAbsence(ctx, repository.store, action.TargetID, 0)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	effects, err := repository.prepareHierarchyDeletionIndexedDelete(ctx, action, primary, nil)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	effects.conditions = append(effects.conditions, etcdstore.Condition{Key: activeKey})
	effects.conditions = append(effects.conditions, scriptConditions...)
	effects.mutations = append(
		effects.mutations,
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: serviceruntimerecord.Key(record.ServiceID)},
	)
	return effects, nil
}
