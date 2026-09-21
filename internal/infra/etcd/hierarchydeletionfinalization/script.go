package hierarchydeletionfinalization

import (
	"context"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Preparer) prepareHierarchyDeletionScriptFinalizer(
	ctx context.Context,
	action hierarchydeletion.HierarchyDeletionAction,
) (Effects, error) {
	if action.ControllerProcedure == nil {
		return Effects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	storage, err := scriptrecord.ReadActiveScriptStorage(
		ctx,
		repository.store,
		action.TargetID,
		action.ControllerProcedure.FixedInputRevision,
	)
	if err != nil {
		return Effects{}, err
	}
	record := storage.Script.Record
	if storage.Script.Revision != action.TargetRevision || record.Desired.ID != action.TargetID {
		return Effects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	if record.ActiveReferences != 0 {
		return Effects{}, errs.New(
			errs.KindResourceInUse, "active Script executions fence hierarchy deletion",
		)
	}
	value, err := scriptrecord.EncodeRecord(record)
	if err != nil {
		return Effects{}, err
	}
	defer clear(value)
	primary := etcdstore.KeyValue{
		Key:   scriptrecord.ScriptSetScriptKey(record.EnvironmentID, record.ScriptSetGeneration, action.TargetID),
		Value: value, ModRevision: storage.Script.Revision,
	}
	keys := []string{
		scriptrecord.ScriptSetOwnerKey(record.EnvironmentID, record.ScriptSetGeneration, action.TargetID),
		scriptrecord.ScriptSetSlugKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.Slug),
	}
	effects, err := repository.prepareHierarchyDeletionIndexedDelete(ctx, action, &primary, keys)
	if err != nil {
		return Effects{}, err
	}
	activeValue, err := scriptrecord.EncodeScriptSetGeneration(storage.Active.Record)
	if err != nil {
		return Effects{}, err
	}
	effects.values = append(effects.values, activeValue)
	effects.conditions = append(
		effects.conditions,
		etcdstore.Condition{
			Key:         scriptrecord.ScriptSetActiveKey(record.EnvironmentID),
			ModRevision: storage.Active.Revision,
		},
		etcdstore.Condition{Key: scriptrecord.ScriptLocatorKey(action.TargetID), ModRevision: storage.Locator.Revision},
		etcdstore.Condition{
			Key:         scriptrecord.ScriptEnvironmentLocatorKey(record.EnvironmentID, action.TargetID),
			ModRevision: storage.EnvironmentLocator.Revision,
		},
	)
	effects.mutations = append(
		effects.mutations,
		etcdstore.Mutation{
			Type: etcdstore.MutationDelete,
			Key: scriptrecord.ScriptSetBodyGenerationPrefix(
				record.EnvironmentID,
				record.ScriptSetGeneration,
				action.TargetID,
			),
			Prefix: true,
		},
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: scriptrecord.ScriptLocatorKey(action.TargetID)},
		etcdstore.Mutation{
			Type: etcdstore.MutationDelete,
			Key:  scriptrecord.ScriptEnvironmentLocatorKey(record.EnvironmentID, action.TargetID),
		},
		etcdstore.Mutation{
			Type:  etcdstore.MutationPut,
			Key:   scriptrecord.ScriptSetActiveKey(record.EnvironmentID),
			Value: activeValue,
		},
	)
	return effects, nil
}
