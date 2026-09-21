package etcd

import (
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

func connectorEnvironmentPrefix(environmentID string) string {
	return connectorEnvironmentIndexPrefix + environmentID + "/"
}

func connectorEnvironmentKey(environmentID string, connectorID string) string {
	return connectorEnvironmentPrefix(environmentID) + connectorID
}

func connectorNameKey(environmentID string, name string) string {
	return connectorNameIndexPrefix + environmentID + "/" + recordcodec.EncodeKeySegment(name)
}

func connectorCreateConditions(
	record connectorrecord.Record,
) []etcdstore.Condition {
	connector := record.Connector
	conditions := []etcdstore.Condition{
		{Key: connectorrecord.RecordKey(connector.ID)},
		{Key: connectorNameKey(connector.EnvironmentID, connector.Name)},
		{Key: connectorEnvironmentKey(connector.EnvironmentID, connector.ID)},
		{Key: connectorrecord.CredentialValueKey(connector.ID)},
		{Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetConnector), connector.ID)},
	}
	return conditions
}
