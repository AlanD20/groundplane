package etcd

import (
	"context"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *BackupPolicyRepository) loadBackupPolicyConnectorEvidence(
	ctx context.Context,
	environmentID string,
	connectorID string,
	revision int64,
) (*etcdstore.Versioned[connectorrecord.Record], *etcdstore.KeyValue, error) {
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		connectorrecord.RecordKey(connectorID),
		connectorEnvironmentKey(environmentID, connectorID),
	}, Revision: revision})
	if err != nil {
		return nil, nil, err
	}
	if result == nil || result.ReadRevision != revision || len(result.Values) != 2 {
		return nil, nil, errs.New(errs.KindInternal, "backup policy connector read is incomplete")
	}
	if result.Values[0] == nil {
		return nil, nil, errs.New(errs.KindConnectorNotFound, "connector was not found")
	}
	record, err := connectorrecord.DecodeRecord(result.Values[0].Value)
	if err != nil || record.Connector.ID != connectorID {
		return nil, nil, connectorrecord.CorruptRecord()
	}
	if record.Connector.EnvironmentID != environmentID {
		return nil, nil, errs.New(errs.KindScopeUnauthorized, "connector belongs to another environment")
	}
	if result.Values[1] == nil || string(result.Values[1].Value) != connectorID {
		return nil, nil, connectorrecord.CorruptRecord()
	}
	return &etcdstore.Versioned[connectorrecord.Record]{
		Record: record, Revision: result.Values[0].ModRevision, ReadRevision: result.ReadRevision,
	}, cloneBackupPolicyEvidenceKeyValue(result.Values[1]), nil
}

func (repository *BackupPolicyRepository) loadBackupPolicyConnectorReferences(
	ctx context.Context,
	candidate backupPolicyReplacementCandidate,
	revision int64,
) ([]backupPolicyConnectorReferenceEvidence, error) {
	oldConnectorID := ""
	if candidate.Current != nil && candidate.Current.Record.Enabled {
		oldConnectorID = candidate.Current.Record.ConnectorID
	}
	newConnectorID := ""
	if candidate.Replacement.Enabled {
		newConnectorID = candidate.Replacement.ConnectorID
	}
	connectorIDs := make([]string, 0, 2)
	if oldConnectorID != "" {
		connectorIDs = append(connectorIDs, oldConnectorID)
	}
	if newConnectorID != "" && newConnectorID != oldConnectorID {
		connectorIDs = append(connectorIDs, newConnectorID)
	}
	evidence := make([]backupPolicyConnectorReferenceEvidence, len(connectorIDs))
	for index, connectorID := range connectorIDs {
		result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{
				backuppolicy.BackupPolicyConnectorReferenceKey(connectorID, candidate.Replacement.EnvironmentID),
			},
			Revision: revision,
		})
		if err != nil {
			return nil, err
		}
		if result == nil || result.ReadRevision != revision || len(result.Values) != 1 {
			return nil, errs.New(errs.KindInternal, "backup policy connector reference read is empty")
		}
		evidence[index] = backupPolicyConnectorReferenceEvidence{
			ConnectorID: connectorID,
			Entry:       cloneBackupPolicyEvidenceKeyValue(result.Values[0]),
		}
	}
	return evidence, nil
}
