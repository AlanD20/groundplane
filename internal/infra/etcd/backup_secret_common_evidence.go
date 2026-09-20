package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (reader *BackupSecretResolutionReader) decodeCommonDynamicEvidence(
	result *etcdstore.GetManyResult,
	dynamic *backupSecretDynamicRead,
	evidence *BackupSecretResolutionEvidence,
	environmentID string,
	connectorIDs map[string]int,
	selectedConnectorID string,
) error {
	environmentValue := result.Values[dynamic.environment]
	projectKeyID := ""
	if environmentValue == nil {
		return errs.New(errs.KindStateConflict, "backup environment evidence is unavailable")
	}
	environment, err := hierarchyrecord.DecodeEnvironment(environmentValue.Value)
	if err != nil || environment.ID != environmentID {
		return errs.New(errs.KindInternal, "backup environment evidence is corrupt")
	}
	evidence.Environment = environment
	evidence.EnvironmentRevision = environmentValue.ModRevision
	projectKeyID = environment.ProjectID
	dynamic.project = dynamic.add(hierarchyrecord.ProjectKey(projectKeyID))
	dynamic.projectFence = dynamic.add(
		deletionTombstoneKey(string(DeletionTargetProject), projectKeyID),
	)
	if err := requireNoDeletionFence(
		result.Values[dynamic.environmentFence], DeletionTargetEnvironment, environmentID,
	); err != nil {
		return err
	}
	for connectorID, position := range connectorIDs {
		value := result.Values[position]
		if value == nil {
			return errs.New(errs.KindStateConflict, "backup connector evidence is unavailable")
		}
		connector, decodeErr := connectorrecord.DecodeRecord(value.Value)
		if decodeErr != nil || connector.Connector.ID != connectorID ||
			connector.Connector.EnvironmentID != environmentID {
			return errs.New(errs.KindInternal, "backup connector evidence is corrupt")
		}
		if err := requireNoDeletionFence(
			result.Values[dynamic.connectorFences[connectorID]], DeletionTargetConnector, connectorID,
		); err != nil {
			return err
		}
		if connectorID == selectedConnectorID {
			if evidence.Run != nil && value.ModRevision != evidence.Run.ConnectorRevision {
				return errs.New(errs.KindStateConflict, "backup connector snapshot changed")
			}
			evidence.Connector = connector
			evidence.ConnectorRevision = value.ModRevision
		}
	}
	if evidence.Connector.Connector.ID != selectedConnectorID {
		return errs.New(errs.KindStateConflict, "backup connector evidence is unavailable")
	}
	for _, name := range []backupsecret.CredentialName{
		backupsecret.CredentialAccessKey, backupsecret.CredentialSecretKey,
	} {
		credential := evidence.Connector.Connector.Credentials[name]
		if credential.Kind == backupsecret.CredentialSourceSecretRef {
			hierarchyrecord.ProjectKey := secretKeyIndexKey(backupsecret.SecretScopeProject, projectKeyID, credential.SecretRef)
			platformKey := secretKeyIndexKey(backupsecret.SecretScopePlatform, "", credential.SecretRef)
			dynamic.secretIndexes = append(dynamic.secretIndexes, backupSecretIndexRead{
				name: name, reference: credential.SecretRef,
				projectIndex: dynamic.add(hierarchyrecord.ProjectKey), platformIndex: dynamic.add(platformKey),
			})
		}
	}
	return nil
}

func requireNoDeletionFence(value *etcdstore.KeyValue, targetKind DeletionTargetKind, stableID string) error {
	if value == nil {
		return nil
	}
	tombstone, err := decodeDeletionTombstone(value.Value)
	if err != nil || tombstone.TargetKind != targetKind || tombstone.TargetID != stableID {
		return errs.New(errs.KindInternal, "deletion fence evidence is corrupt")
	}
	return errs.New(errs.KindStateConflict, "backup resource deletion is in progress")
}
