package etcd

import (
	"context"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	entryvalues "github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionEntryFinalizer(
	ctx context.Context,
	action hierarchydeletion.HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	primary, err := repository.readHierarchyDeletionPrimary(ctx, entryrecord.RecordKey(action.TargetID), action)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	defer clear(primary.Value)
	record, err := entryrecord.DecodeRecord(primary.Value)
	if err != nil || record.Entry.ID != action.TargetID {
		return hierarchyDeletionControllerEffects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	fences, err := prepareEntryScriptAbsence(ctx, repository.store, action.TargetID, 0)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	effects, err := repository.prepareHierarchyDeletionIndexedDelete(ctx, action, primary, []string{
		entryOwnerKey(record.EnvironmentID, record.Entry.ID),
	})
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	effects.conditions = append(effects.conditions, fences...)
	effects.mutations = append(effects.mutations,
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: entryvalues.PlainPrefix + record.Entry.ID + "/", Prefix: true},
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: entryvalues.SecretPrefix + record.Entry.ID + "/", Prefix: true},
	)
	return effects, nil
}
