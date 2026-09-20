package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionEntryFinalizer(
	ctx context.Context,
	action HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	primary, err := repository.readHierarchyDeletionPrimary(ctx, entryRecordKey(action.TargetID), action)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	defer clear(primary.Value)
	record, err := decodeEntryRecord(primary.Value)
	if err != nil || record.Entry.ID != action.TargetID {
		return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
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
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: entryPlainValueGenerationPrefix + record.Entry.ID + "/", Prefix: true},
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: entrySecretValueGenerationPrefix + record.Entry.ID + "/", Prefix: true},
	)
	return effects, nil
}
