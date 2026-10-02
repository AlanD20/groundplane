// Package backuppruneevidence resolves the immutable credential authority used
// by a Backup prune execution plan.
package backuppruneevidence

import (
	"context"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// FixedReader reads exact keys from one caller-selected store revision.
type FixedReader interface {
	ReadFixedKeys(context.Context, []string, int64) (*etcdstore.GetManyResult, error)
}

// LoadConnectorAuthority follows the same project-first/platform-fallback
// credential authority as Backup capture. Only encrypted record digests leave
// this read.
func LoadConnectorAuthority(
	ctx context.Context,
	reader FixedReader,
	connector connectors.Record,
	connectorValue, direct *etcdstore.KeyValue,
	projectID string,
	readRevision int64,
) (*agentpb.BackupConnectorAuthority, error) {
	projectRead, err := reader.ReadFixedKeys(ctx, []string{
		hierarchy.ProjectKey(projectID),
		deletions.TombstoneKey(string(deletions.DeletionTargetProject), projectID),
	}, readRevision)
	if err != nil {
		return nil, err
	}
	defer etcdstore.ClearValues(projectRead.Values)
	if len(projectRead.Values) != 2 || projectRead.Values[0] == nil {
		return nil, errs.New(errs.KindStateConflict, "backup prune credential Project is unavailable")
	}
	project, err := hierarchy.DecodeProject(projectRead.Values[0].Value)
	if err != nil || project.ID != projectID {
		return nil, errs.New(errs.KindStateConflict, "backup prune credential Project changed")
	}
	if err := ValidateNoDeletion(
		projectRead.Values[1],
		deletions.DeletionTargetProject,
		projectID,
	); err != nil {
		return nil, err
	}
	if len(connector.Connector.Credentials) != 2 {
		return nil, errs.New(errs.KindStateConflict, "backup prune Connector credentials are incomplete")
	}
	if connectors.HasDirectCredentials(connector) {
		if direct == nil || direct.ModRevision != connectorValue.ModRevision {
			return nil, errs.New(
				errs.KindStateConflict,
				"backup prune direct credential snapshot is unavailable or changed",
			)
		}
		encrypted, err := connectors.DecodeEncryptedCredentials(direct.Value)
		clear(encrypted.Ciphertext)
		if err != nil || encrypted.ConnectorID != connector.Connector.ID {
			return nil, errs.New(errs.KindInternal, "backup prune direct credential snapshot is corrupt")
		}
	} else if direct != nil {
		return nil, errs.New(errs.KindStateConflict, "backup prune direct credential mode changed")
	}
	selected := make([]*agentpb.RevisionDigest, 0, 2)
	credentialNames := []backupsecret.CredentialName{
		backupsecret.CredentialAccessKey,
		backupsecret.CredentialSecretKey,
	}
	for _, name := range credentialNames {
		credential, exists := connector.Connector.Credentials[name]
		if !exists {
			return nil, errs.New(errs.KindStateConflict, "backup prune credential selection is incomplete")
		}
		switch credential.Kind {
		case backupsecret.CredentialSourceDirect:
			selected = append(selected, RevisionDigest(direct))
		case backupsecret.CredentialSourceSecretRef:
			value, err := loadSecretCredential(ctx, reader, projectID, credential.SecretRef, readRevision)
			if err != nil {
				return nil, err
			}
			selected = append(selected, value)
		default:
			return nil, errs.New(errs.KindStateConflict, "backup prune credential selection is unsupported")
		}
	}
	pathStyle := connector.Connector.PathStyle
	return &agentpb.BackupConnectorAuthority{
		ConnectorId:          connector.Connector.ID,
		Connector:            RevisionDigest(connectorValue),
		CanonicalEndpointUrl: connector.Connector.Endpoint,
		Region:               connector.Connector.Region,
		PathStyle:            &pathStyle,
		Prefix:               connector.Connector.Prefix,
		AccessKeySlotId:      backupsecret.AccessKeySlotID,
		SecretKeySlotId:      backupsecret.SecretKeySlotID,
		AccessKeySlot:        selected[0],
		SecretKeySlot:        selected[1],
	}, nil
}

func loadSecretCredential(
	ctx context.Context,
	reader FixedReader,
	projectID, reference string,
	readRevision int64,
) (*agentpb.RevisionDigest, error) {
	indexes, err := reader.ReadFixedKeys(ctx, []string{
		secrets.SecretKeyIndexKey(backupsecret.SecretScopeProject, projectID, reference),
		secrets.SecretKeyIndexKey(backupsecret.SecretScopePlatform, "", reference),
	}, readRevision)
	if err != nil {
		return nil, err
	}
	defer etcdstore.ClearValues(indexes.Values)
	if len(indexes.Values) != 2 {
		return nil, errs.New(errs.KindInternal, "backup prune Secret index read is incomplete")
	}
	for index, value := range indexes.Values {
		if value == nil {
			continue
		}
		id := string(value.Value)
		if ids.Validate(ids.KindSecret, id) != nil {
			return nil, errs.New(errs.KindInternal, "backup prune credential Secret index is corrupt")
		}
		read, err := reader.ReadFixedKeys(ctx, []string{
			secrets.RecordKey(id),
			deletions.TombstoneKey(string(deletions.DeletionTargetSecret), id),
			secrets.ValueKey(id),
		}, readRevision)
		if err != nil {
			return nil, err
		}
		if len(read.Values) != 3 {
			etcdstore.ClearValues(read.Values)
			return nil, errs.New(errs.KindInternal, "backup prune Secret read is incomplete")
		}
		if read.Values[1] != nil {
			err := secrets.ValidateSecretDeletionFence(read.Values[1], id)
			etcdstore.ClearValues(read.Values)
			if err != nil {
				return nil, err
			}
			continue
		}
		if read.Values[0] == nil || read.Values[2] == nil {
			etcdstore.ClearValues(read.Values)
			return nil, errs.New(errs.KindStateConflict, "backup prune credential Secret snapshot is unavailable")
		}
		record, recordErr := secrets.DecodeRecord(read.Values[0].Value)
		encrypted, valueErr := secrets.DecodeEncryptedValue(read.Values[2].Value)
		clear(encrypted.Ciphertext)
		scopeMatches := index == 0 && record.Secret.Scope == backupsecret.SecretScopeProject &&
			record.Secret.ProjectID == projectID ||
			index == 1 && record.Secret.Scope == backupsecret.SecretScopePlatform && record.Secret.ProjectID == ""
		if recordErr != nil || valueErr != nil || record.Secret.ID != id || record.Secret.Key != reference ||
			record.Secret.Kind != backupsecret.SecretKindEnvVar || !scopeMatches || encrypted.SecretID != id {
			etcdstore.ClearValues(read.Values)
			return nil, errs.New(errs.KindInternal, "backup prune credential Secret snapshot is corrupt")
		}
		digest := RevisionDigest(read.Values[2])
		etcdstore.ClearValues(read.Values)
		return digest, nil
	}
	return nil, errs.New(errs.KindSecretNotFound, "backup prune credential Secret was not found in scope")
}

// RevisionDigest binds a record's exact store revision and encoded bytes.
func RevisionDigest(value *etcdstore.KeyValue) *agentpb.RevisionDigest {
	digest := sha256.Sum256(value.Value)
	return &agentpb.RevisionDigest{
		ModRevision: value.ModRevision,
		Sha256:      append([]byte(nil), digest[:]...),
	}
}

// ValidateNoDeletion rejects an active deletion owner for the selected record.
func ValidateNoDeletion(value *etcdstore.KeyValue, kind deletions.DeletionTargetKind, id string) error {
	if value == nil {
		return nil
	}
	tombstone, err := deletions.DecodeDeletionTombstone(value.Value)
	if err != nil || tombstone.TargetKind != kind || tombstone.TargetID != id {
		return errs.New(errs.KindInternal, "backup prune deletion fence is corrupt")
	}
	return errs.New(errs.KindStateConflict, "backup prune resource deletion is in progress")
}
