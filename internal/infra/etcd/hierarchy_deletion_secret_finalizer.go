package etcd

import "context"

// Secret ciphertext remains protected during parent deletion by the same
// exact Script count and forward-membership absence proof as direct removal.
func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionSecretFinalizer(
	ctx context.Context,
	action HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	primary, err := repository.readHierarchyDeletionPrimary(ctx, secretRecordKey(action.TargetID), action)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	defer clear(primary.Value)
	record, err := decodeSecretRecord(primary.Value)
	if err != nil || record.Secret.ID != action.TargetID {
		return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
	}
	fences, err := prepareSecretScriptAbsence(ctx, repository.store, action.TargetID)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	keys := []string{secretOwnerKey(record.Secret), secretScopedKey(record.Secret), secretValueKey(action.TargetID)}
	effects, err := repository.prepareHierarchyDeletionIndexedDelete(ctx, action, primary, keys)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	effects.conditions = append(effects.conditions, fences...)
	return effects, nil
}
