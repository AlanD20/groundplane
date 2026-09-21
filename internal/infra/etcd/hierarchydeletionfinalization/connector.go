package hierarchydeletionfinalization

import (
	"context"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func (repository *Preparer) prepareHierarchyDeletionConnectorFinalizer(
	ctx context.Context,
	action hierarchydeletion.HierarchyDeletionAction,
) (Effects, error) {
	primary, err := repository.readHierarchyDeletionPrimary(ctx, connectorrecord.RecordKey(action.TargetID), action)
	if err != nil {
		return Effects{}, err
	}
	defer clear(primary.Value)
	record, err := connectorrecord.DecodeRecord(primary.Value)
	if err != nil || record.Connector.ID != action.TargetID {
		return Effects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	keys := []string{
		connectorrecord.ConnectorEnvironmentKey(record.Connector.EnvironmentID, action.TargetID),
		connectorrecord.ConnectorNameKey(record.Connector.EnvironmentID, record.Connector.Name),
		connectorrecord.CredentialValueKey(action.TargetID),
	}
	effects, err := repository.prepareHierarchyDeletionIndexedDelete(ctx, action, primary, keys)
	if err != nil {
		return Effects{}, err
	}
	prefixes := hierarchyDeletionConnectorReferencePrefixes(action.TargetID)
	_, err = repository.requireHierarchyDeletionPrefixesEmpty(ctx, prefixes)
	if err != nil {
		etcdstore.ClearByteSlices(effects.values)
		return Effects{}, err
	}
	for _, prefix := range prefixes {
		effects.conditions = append(effects.conditions, etcdstore.Condition{Key: prefix, Prefix: true})
	}
	return effects, nil
}
