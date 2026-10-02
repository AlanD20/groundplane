package backupplanning

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/ids"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (repository *Planner) manualBackupCredentialAuthorities(
	ctx context.Context,
	connector connectorrecord.Record,
	direct *etcdstore.KeyValue,
	projectID string,
	fixedRevision int64,
) (*agentpb.RevisionDigest, *agentpb.RevisionDigest, error) {
	selected := make([]*agentpb.RevisionDigest, 0, 2)
	for _, name := range []backupsecret.CredentialName{backupsecret.CredentialAccessKey, backupsecret.CredentialSecretKey} {
		credential, exists := connector.Connector.Credentials[name]
		if !exists {
			return nil, nil, errs.New(errs.KindStateConflict, "backup Connector credential selection is incomplete")
		}
		switch credential.Kind {
		case backupsecret.CredentialSourceDirect:
			if direct == nil {
				return nil, nil, errs.New(errs.KindStateConflict, "backup direct credential snapshot is unavailable")
			}
			selected = append(selected, snapshotRevisionDigest(direct))
		case backupsecret.CredentialSourceSecretRef:
			value, err := repository.manualBackupSecretCredential(ctx, projectID, credential.SecretRef, fixedRevision)
			if err != nil {
				return nil, nil, err
			}
			selected = append(selected, value)
		default:
			return nil, nil, errs.New(errs.KindStateConflict, "backup Connector credential selection is unsupported")
		}
	}
	return selected[0], selected[1], nil
}

func (repository *Planner) manualBackupSecretCredential(ctx context.Context, projectID, reference string,
	fixedRevision int64) (*agentpb.RevisionDigest, error) {
	indexes, err := repository.reader.ReadFixedKeys(ctx, []string{
		secretrecord.SecretKeyIndexKey(backupsecret.SecretScopeProject, projectID, reference),
		secretrecord.SecretKeyIndexKey(backupsecret.SecretScopePlatform, "", reference),
	}, fixedRevision)
	if err != nil {
		return nil, err
	}
	defer etcdstore.ClearValues(indexes.Values)
	for index, value := range indexes.Values {
		if value == nil {
			continue
		}
		id := string(value.Value)
		if ids.Validate(ids.KindSecret, id) != nil {
			return nil, errs.New(errs.KindInternal, "backup credential Secret index is corrupt")
		}
		read, err := repository.reader.ReadFixedKeys(ctx, []string{secretrecord.RecordKey(id),
			deletionrecord.TombstoneKey(
				string(deletionrecord.DeletionTargetSecret),
				id,
			), secretrecord.ValueKey(id)}, fixedRevision)
		if err != nil {
			return nil, err
		}
		if read.Values[1] != nil {
			err := secretrecord.ValidateSecretDeletionFence(read.Values[1], id)
			etcdstore.ClearValues(read.Values)
			if err != nil {
				return nil, err
			}
			continue
		}
		if read.Values[0] == nil || read.Values[2] == nil {
			etcdstore.ClearValues(read.Values)
			return nil, errs.New(errs.KindStateConflict, "backup credential Secret snapshot is unavailable")
		}
		record, recordErr := secretrecord.DecodeRecord(read.Values[0].Value)
		encrypted, valueErr := secretrecord.DecodeEncryptedValue(read.Values[2].Value)
		clear(encrypted.Ciphertext)
		scopeMatches := index == 0 && record.Secret.Scope == backupsecret.SecretScopeProject &&
			record.Secret.ProjectID == projectID ||
			index == 1 && record.Secret.Scope == backupsecret.SecretScopePlatform && record.Secret.ProjectID == ""
		if recordErr != nil || valueErr != nil || record.Secret.ID != id || record.Secret.Key != reference ||
			record.Secret.Kind != backupsecret.SecretKindEnvVar || !scopeMatches || encrypted.SecretID != id {
			etcdstore.ClearValues(read.Values)
			return nil, errs.New(errs.KindInternal, "backup credential Secret snapshot is corrupt")
		}
		digest := snapshotRevisionDigest(read.Values[2])
		etcdstore.ClearValues(read.Values)
		return digest, nil
	}
	return nil, errs.New(errs.KindSecretNotFound, "backup credential Secret was not found in scope")
}
