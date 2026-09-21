package backupsecrets

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/ids"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (reader *Reader) resolveEncryptedCredentialValues(
	ctx context.Context,
	fixedRevision int64,
	dynamic *backupSecretDynamicRead,
	evidence *Evidence,
	second *etcdstore.GetManyResult,
) error {
	if dynamic.project < 0 || second.Values[dynamic.project] == nil {
		return errs.New(errs.KindStateConflict, "backup project evidence is unavailable")
	}
	projectValue := second.Values[dynamic.project]
	project, err := hierarchyrecord.DecodeProject(projectValue.Value)
	if err != nil || project.ID != evidence.Environment.ProjectID {
		return errs.New(errs.KindInternal, "backup project evidence is corrupt")
	}
	if err := requireNoDeletionFence(
		second.Values[dynamic.projectFence], deletionrecord.DeletionTargetProject, project.ID,
	); err != nil {
		return err
	}
	evidence.Project = project
	evidence.ProjectRevision = projectValue.ModRevision
	dynamic.last = second
	connector := evidence.Connector.Connector
	if evidence.Run != nil {
		if connectorrecord.HasDirectCredentials(evidence.Connector) != evidence.Run.ConnectorHasDirectCredentials {
			return errs.New(errs.KindStateConflict, "backup connector credential mode changed")
		}
	}
	if connectorrecord.HasDirectCredentials(evidence.Connector) {
		position, ok := dynamic.credentials[connector.ID]
		if !ok {
			return errs.New(errs.KindInternal, "backup connector credential evidence is unavailable")
		}
		value := second.Values[position]
		expectedRevision := evidence.ConnectorRevision
		if evidence.Run != nil {
			if evidence.Run.ConnectorCredentialsRevision <= 0 {
				return errs.New(errs.KindStateConflict, "backup connector credential snapshot is missing")
			}
			expectedRevision = evidence.Run.ConnectorCredentialsRevision
		}
		if value == nil || value.ModRevision != expectedRevision {
			return errs.New(errs.KindStateConflict, "backup connector credential snapshot changed")
		}
		credentials, err := connectorrecord.DecodeEncryptedCredentials(value.Value)
		if err != nil || credentials.ConnectorID != connector.ID {
			return errs.New(errs.KindInternal, "backup connector credential evidence is corrupt")
		}
		evidence.Credentials = credentials
		evidence.HasCredentials = true
	}

	refs := dynamic.secretIndexes
	if len(refs) == 0 {
		return nil
	}
	secretPlans := make([]backupSecretIndexRead, len(refs))
	for index, reference := range refs {
		projectIndexValue := second.Values[reference.projectIndex]
		platformIndexValue := second.Values[reference.platformIndex]
		for _, candidate := range []*etcdstore.KeyValue{projectIndexValue, platformIndexValue} {
			if candidate != nil && ids.Validate(ids.KindSecret, string(candidate.Value)) != nil {
				return errs.New(errs.KindInternal, "backup Secret key index is corrupt")
			}
		}
		secretPlans[index] = reference
		for _, candidate := range []*etcdstore.KeyValue{projectIndexValue, platformIndexValue} {
			if candidate == nil {
				continue
			}
			id := string(candidate.Value)
			dynamic.secretRecords[id] = dynamic.add(secretrecord.RecordKey(id))
			dynamic.secretFences[id] = dynamic.add(
				deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetSecret), id),
			)
			dynamic.secretValues[id] = dynamic.add(secretrecord.ValueKey(id))
		}
	}
	final, err := reader.readFixed(ctx, dynamic.keys, fixedRevision)
	if err != nil {
		return err
	}
	defer etcdstore.ClearValues(final.Values)
	dynamic.last = final
	for _, reference := range secretPlans {
		selected, err := selectBackupSecretCandidate(final.Values, dynamic, reference, evidence.Project.ID)
		if err != nil {
			return err
		}
		value := final.Values[dynamic.secretValues[selected.id]]
		if value == nil {
			return errs.New(errs.KindInternal, "backup Secret encrypted value is unavailable")
		}
		encrypted, decodeErr := secretrecord.DecodeEncryptedValue(value.Value)
		if decodeErr != nil || encrypted.SecretID != selected.id {
			return errs.New(errs.KindInternal, "backup Secret encrypted value is corrupt")
		}
		evidence.SecretValues = append(evidence.SecretValues, SecretValueEvidence{
			Name: reference.name, Reference: reference.reference, Value: encrypted,
		})
	}
	return nil
}

type backupSecretCandidate struct {
	id      string
	project bool
}

func selectBackupSecretCandidate(
	values []*etcdstore.KeyValue,
	dynamic *backupSecretDynamicRead,
	reference backupSecretIndexRead,
	projectID string,
) (backupSecretCandidate, error) {
	candidates := []struct {
		value   *etcdstore.KeyValue
		project bool
	}{
		{value: values[reference.projectIndex], project: true},
		{value: values[reference.platformIndex], project: false},
	}
	for _, candidate := range candidates {
		if candidate.value == nil {
			continue
		}
		id := string(candidate.value.Value)
		fence := values[dynamic.secretFences[id]]
		if fence != nil {
			if err := secretrecord.ValidateSecretDeletionFence(fence, id); err != nil {
				return backupSecretCandidate{}, err
			}
			continue
		}
		recordValue := values[dynamic.secretRecords[id]]
		if recordValue == nil {
			return backupSecretCandidate{}, errs.New(errs.KindInternal, "backup Secret record is unavailable")
		}
		record, err := secretrecord.DecodeRecord(recordValue.Value)
		if err != nil || record.Secret.ID != id || record.Secret.Key != reference.reference ||
			record.Secret.Kind != backupsecret.SecretKindEnvVar ||
			(candidate.project && (record.Secret.Scope != backupsecret.SecretScopeProject || record.Secret.ProjectID != projectID)) ||
			(!candidate.project && (record.Secret.Scope != backupsecret.SecretScopePlatform || record.Secret.ProjectID != "")) {
			return backupSecretCandidate{}, errs.New(errs.KindInternal, "backup Secret record is corrupt")
		}
		return backupSecretCandidate{id: id, project: candidate.project}, nil
	}
	return backupSecretCandidate{}, errs.New(errs.KindSecretNotFound, "backup Secret was not found in scope")
}
