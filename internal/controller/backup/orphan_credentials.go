package backup

import (
	"context"

	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"github.com/AlanD20/groundplane/internal/infra/s3compatible"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BackupOrphanCredentialStore opens existing Connector and Secret encrypted
// owners only for one exact remote operation. It keeps no credential cache.
type BackupOrphanCredentialStore struct {
	connectors   *connectorrecord.Reader
	environments *hierarchyrecord.Reader
	secrets      *secretrecord.Reader
	protector    *secretvalue.Protector
}

func NewBackupOrphanCredentialStore(connectors *connectorrecord.Reader,
	environments *hierarchyrecord.Reader, secrets *secretrecord.Reader,
	protector *secretvalue.Protector,
) (*BackupOrphanCredentialStore, error) {
	if connectors == nil || environments == nil || secrets == nil || protector == nil {
		return nil, errs.New(errs.KindInternal, "backup orphan credential owners are required")
	}
	return &BackupOrphanCredentialStore{connectors: connectors, environments: environments,
		secrets: secrets, protector: protector}, nil
}

func (owner *BackupOrphanCredentialStore) WithBackupOrphanStore(ctx context.Context,
	target backupruntime.BackupRecoveryPointTargetSnapshot,
	use func(BackupOrphanExactStore) error,
) error {
	if owner == nil || owner.connectors == nil || owner.environments == nil ||
		owner.secrets == nil || owner.protector == nil || use == nil {
		return errs.New(errs.KindInternal, "backup orphan credential use is not configured")
	}
	connector, err := owner.connectors.GetConnector(ctx, target.ConnectorID)
	if err != nil {
		return err
	}
	metadata := connector.Record.Connector
	if metadata.EnvironmentID != target.EnvironmentID || metadata.Endpoint != target.ConnectorEndpoint ||
		metadata.Bucket != target.ConnectorBucket || metadata.Prefix != target.ConnectorPrefix ||
		metadata.Region != target.ConnectorRegion || metadata.PathStyle != target.ConnectorPathStyle {
		return errs.New(errs.KindStateConflict, "backup orphan Connector authority changed")
	}
	environment, err := owner.environments.GetEnvironment(ctx, target.EnvironmentID)
	if err != nil {
		return err
	}
	if environment.Record.ID != metadata.EnvironmentID {
		return errs.New(errs.KindStateConflict, "backup orphan Environment authority changed")
	}
	expected, err := expectedBackupCredentialSources(connector.Record)
	if err != nil {
		return err
	}
	values := make(map[core.ConnectorCredentialName][]byte, 2)
	defer clearBackupSecretMap(values)
	resolver := &BackupSecretResolver{protector: owner.protector}
	if connectorrecord.HasDirectCredentials(connector.Record) {
		encrypted, err := owner.connectors.GetConnectorCredentials(ctx, connector)
		if err != nil {
			return err
		}
		defer clear(encrypted.Ciphertext)
		direct, err := resolver.openDirectCredentials(ctx, &encrypted, expected)
		if err != nil {
			return err
		}
		for name, value := range direct {
			values[name] = value
			delete(direct, name)
		}
	}
	for _, name := range []core.ConnectorCredentialName{
		core.ConnectorCredentialAccessKey, core.ConnectorCredentialSecretKey,
	} {
		if expected[name] != core.ConnectorCredentialSecretRef {
			continue
		}
		reference := metadata.Credentials[name].SecretRef
		secret, err := owner.secrets.ResolveSecret(ctx, environment.Record.ProjectID, reference)
		if err != nil {
			return err
		}
		if secret.Record.Secret.Kind != core.SecretKindEnvVar ||
			(secret.Record.Secret.Key != reference && secret.Record.Secret.ID != reference) {
			return errs.New(errs.KindStateConflict, "backup orphan Secret reference changed")
		}
		encrypted, err := owner.secrets.GetSecretValue(ctx, secret)
		if err != nil {
			return err
		}
		value, err := resolver.openSecretValue(ctx, &encrypted)
		clear(encrypted.Ciphertext)
		if err != nil {
			return err
		}
		if len(value) == 0 {
			clear(value)
			return errs.New(errs.KindStateConflict, "backup orphan Secret credential is empty")
		}
		values[name] = value
	}
	if len(values[core.ConnectorCredentialAccessKey]) == 0 || len(values[core.ConnectorCredentialSecretKey]) == 0 {
		return errs.New(errs.KindStateConflict, "backup orphan Connector credentials are incomplete")
	}
	store, err := s3compatible.New(s3compatible.Config{Endpoint: target.ConnectorEndpoint,
		Bucket: target.ConnectorBucket, Prefix: target.ConnectorPrefix,
		Region: target.ConnectorRegion, PathStyle: target.ConnectorPathStyle,
		AccessKey: string(values[core.ConnectorCredentialAccessKey]),
		SecretKey: string(values[core.ConnectorCredentialSecretKey])})
	if err != nil {
		return err
	}
	return use(store)
}
