package etcd

import (
	"context"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
)

// Secret ciphertext remains protected during parent deletion by the same
// exact Script count and forward-membership absence proof as direct removal.
func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionSecretFinalizer(
	ctx context.Context,
	action hierarchydeletion.HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	primary, err := repository.readHierarchyDeletionPrimary(ctx, secretrecord.RecordKey(action.TargetID), action)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	defer clear(primary.Value)
	record, err := secretrecord.DecodeRecord(primary.Value)
	if err != nil || record.Secret.ID != action.TargetID {
		return hierarchyDeletionControllerEffects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	fences, err := prepareSecretScriptAbsence(ctx, repository.store, action.TargetID)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	keys := []string{secretrecord.SecretOwnerKey(record.Secret), secretrecord.SecretScopedKey(record.Secret), secretrecord.ValueKey(action.TargetID)}
	effects, err := repository.prepareHierarchyDeletionIndexedDelete(ctx, action, primary, keys)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	effects.conditions = append(effects.conditions, fences...)
	return effects, nil
}
