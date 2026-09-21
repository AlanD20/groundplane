package connectors

import (
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

func ConnectorEnvironmentPrefix(environmentID string) string {
	return connectorEnvironmentIndexPrefix + environmentID + "/"
}

func ConnectorEnvironmentKey(environmentID string, connectorID string) string {
	return ConnectorEnvironmentPrefix(environmentID) + connectorID
}

func ConnectorNameKey(environmentID string, name string) string {
	return connectorNameIndexPrefix + environmentID + "/" + recordcodec.EncodeKeySegment(name)
}

func ConnectorCreateConditions(
	record Record,
) []etcdstore.Condition {
	connector := record.Connector
	conditions := []etcdstore.Condition{
		{Key: RecordKey(connector.ID)},
		{Key: ConnectorNameKey(connector.EnvironmentID, connector.Name)},
		{Key: ConnectorEnvironmentKey(connector.EnvironmentID, connector.ID)},
		{Key: CredentialValueKey(connector.ID)},
		{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetConnector), connector.ID)},
	}
	return conditions
}
