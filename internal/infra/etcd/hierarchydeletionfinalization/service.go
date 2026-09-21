package hierarchydeletionfinalization

import (
	"context"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	scriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"

	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Preparer) prepareHierarchyDeletionServiceFinalizer(
	ctx context.Context,
	action hierarchydeletion.HierarchyDeletionAction,
) (Effects, error) {
	primary, err := repository.readHierarchyDeletionPrimary(ctx, servicerecord.ServiceRuntimeKey(action.TargetID), action)
	if err != nil {
		return Effects{}, err
	}
	defer clear(primary.Value)
	record, err := servicerecord.DecodeServiceRuntimeRecord(primary.Value)
	if err != nil || record.ServiceID != action.TargetID {
		return Effects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	activeKey := servicerecord.ServiceLifecycleActiveKey(record.ServiceID)
	active, err := repository.store.Get(ctx, activeKey)
	if err != nil {
		return Effects{}, err
	}
	if active == nil {
		return Effects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	if active.Entry != nil {
		clear(active.Entry.Value)
		return Effects{}, errs.New(
			errs.KindStateConflict,
			"Service lifecycle is still active during hierarchy metadata finalization",
		)
	}
	scriptConditions, err := scriptsourceevidence.PrepareServiceScriptAbsence(ctx, repository.store, action.TargetID, 0)
	if err != nil {
		return Effects{}, err
	}
	effects, err := repository.prepareHierarchyDeletionIndexedDelete(ctx, action, primary, nil)
	if err != nil {
		return Effects{}, err
	}
	effects.conditions = append(effects.conditions, etcdstore.Condition{Key: activeKey})
	effects.conditions = append(effects.conditions, scriptConditions...)
	effects.mutations = append(
		effects.mutations,
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: serviceruntimerecord.Key(record.ServiceID)},
	)
	return effects, nil
}
