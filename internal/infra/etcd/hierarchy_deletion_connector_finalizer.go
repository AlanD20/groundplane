package etcd

import (
	"context"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionConnectorFinalizer(
	ctx context.Context,
	action hierarchydeletion.HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	primary, err := repository.readHierarchyDeletionPrimary(ctx, connectorrecord.RecordKey(action.TargetID), action)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	defer clear(primary.Value)
	record, err := connectorrecord.DecodeRecord(primary.Value)
	if err != nil || record.Connector.ID != action.TargetID {
		return hierarchyDeletionControllerEffects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	keys := []string{
		connectorEnvironmentKey(record.Connector.EnvironmentID, action.TargetID),
		connectorNameKey(record.Connector.EnvironmentID, record.Connector.Name),
		connectorrecord.CredentialValueKey(action.TargetID),
	}
	effects, err := repository.prepareHierarchyDeletionIndexedDelete(ctx, action, primary, keys)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	prefixes := hierarchyDeletionConnectorReferencePrefixes(action.TargetID)
	_, err = repository.requireHierarchyDeletionPrefixesEmpty(ctx, prefixes)
	if err != nil {
		clearByteSlices(effects.values)
		return hierarchyDeletionControllerEffects{}, err
	}
	for _, prefix := range prefixes {
		effects.conditions = append(effects.conditions, etcdstore.Condition{Key: prefix, Prefix: true})
	}
	return effects, nil
}
