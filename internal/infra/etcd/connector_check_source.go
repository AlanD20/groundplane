package etcd

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectorchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectormutations"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ConnectorCheckSource is fixed-revision admission evidence. Ciphertexts are
// delivered only to the Controller capture step; conditions stay private so
// callers cannot replace selection or hierarchy fences during publication.
type ConnectorCheckSource struct {
	environment keyvalue.Versioned[hierarchyrecord.EnvironmentRecord]
	project     keyvalue.Versioned[hierarchyrecord.ProjectRecord]
	connector   connectorrecord.Record
	revision    int64
	draft       bool
	credentials *connectorrecord.EncryptedCredentials
	secrets     []connectorCheckSecret
	conditions  []keyvalue.Condition
	fence       environmentfence.Evidence
}

type connectorCheckSecret struct {
	name      string
	reference string
	metadata  keyvalue.Versioned[secretrecord.Record]
	value     secretrecord.EncryptedValue
}

func (source *ConnectorCheckSource) Clear() {
	if source.credentials != nil {
		clear(source.credentials.Ciphertext)
		source.credentials = nil
	}
	for index := range source.secrets {
		clear(source.secrets[index].value.Ciphertext)
	}
	source.secrets = nil
}

func (source ConnectorCheckSource) Connector() connectorrecord.Record {
	copyOfRecord := source.connector
	copyOfRecord.Connector.Credentials = make(map[core.ConnectorCredentialName]core.ConnectorCredential, 2)
	for name, credential := range source.connector.Connector.Credentials {
		copyOfRecord.Connector.Credentials[name] = credential
	}
	return copyOfRecord
}

func (source ConnectorCheckSource) Revision() int64 { return source.revision }

func (source ConnectorCheckSource) Draft() bool { return source.draft }

func (source ConnectorCheckSource) DirectCredentials() *connectorrecord.EncryptedCredentials {
	if source.credentials == nil {
		return nil
	}
	copyOfValue := *source.credentials
	copyOfValue.Ciphertext = slices.Clone(source.credentials.Ciphertext)
	return &copyOfValue
}

// SecretValues returns detached encrypted inputs with exact source pins. No
// lookup occurs after capture, including during cleanup Retry.
func (source ConnectorCheckSource) SecretValues(
	operationID string,
) ([]connectorchecks.SecretSource, []secretrecord.EncryptedValue) {
	sources := make([]connectorchecks.SecretSource, len(source.secrets))
	values := make([]secretrecord.EncryptedValue, len(source.secrets))
	for index, secret := range source.secrets {
		sources[index] = connectorchecks.SecretSource{Name: secret.name, Reference: secret.reference,
			Pin: tasksecretpinrecord.Record{OperationID: operationID, SecretID: secret.metadata.Record.Secret.ID,
				MetadataRevision: secret.metadata.Revision, CiphertextSHA256: secret.value.CiphertextSHA256}}
		values[index] = secret.value
		values[index].Ciphertext = slices.Clone(secret.value.Ciphertext)
	}
	return sources, values
}

// LoadConnectorCheckSource accepts a saved version or an unsaved draft (revision
// zero). It does not create a Connector or contact the configured endpoint.
func (repository *ConnectorRepository) LoadConnectorCheckSource(ctx context.Context,
	environment keyvalue.Versioned[hierarchyrecord.EnvironmentRecord],
	project keyvalue.Versioned[hierarchyrecord.ProjectRecord],
	connector connectorrecord.Record, revision int64, draft bool,
) (ConnectorCheckSource, error) {
	if err := connectormutations.ValidateConnectorHierarchy(ctx, environment, project, connector); err != nil {
		return ConnectorCheckSource{}, err
	}
	if revision < 0 || revision == 0 && !draft {
		return ConnectorCheckSource{}, errs.New(errs.KindValidationFailed, "connector check source revision is invalid")
	}
	connector, err := connectorrecord.NewRecord(connector.Connector)
	if err != nil {
		return ConnectorCheckSource{}, err
	}
	id := connector.Connector.ID
	keys := []string{connectorrecord.RecordKey(id), connectorrecord.CredentialValueKey(id),
		deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetConnector), id)}
	if !draft {
		keys = append(keys, connectorchecks.LatestKey(id))
	}
	fence, read, err := repository.LoadConnectorMutationFence(ctx, environment, project, keys)
	if err != nil {
		return ConnectorCheckSource{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[2] != nil {
		return ConnectorCheckSource{}, errs.New(errs.KindResourceInUse, "connector deletion is in progress")
	}
	source := ConnectorCheckSource{environment: environment, project: project, connector: connector,
		revision: revision, draft: draft, fence: fence, conditions: fence.TransactionConditions()}
	if revision == 0 {
		if read.Values[0] != nil || read.Values[1] != nil {
			return ConnectorCheckSource{}, errs.New(errs.KindStateConflict, "draft connector identity already exists")
		}
	} else {
		if read.Values[0] == nil || read.Values[0].ModRevision != revision || read.Values[1] == nil {
			return ConnectorCheckSource{}, errs.New(errs.KindStateConflict, "connector changed before connection-check capture")
		}
		stored, err := connectorrecord.DecodeRecord(read.Values[0].Value)
		if err != nil || stored.Connector.ID != id {
			return ConnectorCheckSource{}, connectorrecord.CorruptRecord()
		}
		credentials, err := connectorrecord.DecodeEncryptedCredentials(read.Values[1].Value)
		if err != nil || credentials.ConnectorID != id {
			clear(credentials.Ciphertext)
			return ConnectorCheckSource{}, connectorrecord.CorruptRecord()
		}
		source.credentials = &credentials
	}
	for index, key := range keys {
		source.conditions = append(
			source.conditions,
			keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[index])},
		)
	}
	secretFence, err := repository.LoadSecretReferenceFenceAtRevision(
		ctx,
		project.Record.ID,
		source.connector,
		fence.ReadRevision(),
	)
	if err != nil {
		source.Clear()
		return ConnectorCheckSource{}, err
	}
	source.conditions = append(source.conditions, secretFence.Conditions()...)
	secrets := secretrecord.NewReader(repository.store)
	for _, name := range []core.ConnectorCredentialName{core.ConnectorCredentialAccessKey, core.ConnectorCredentialSecretKey} {
		credential := source.connector.Connector.Credentials[name]
		if credential.Kind != core.ConnectorCredentialSecretRef {
			continue
		}
		metadata, err := secrets.ResolveSecretAtRevision(
			ctx,
			project.Record.ID,
			credential.SecretRef,
			fence.ReadRevision(),
		)
		if err != nil {
			source.Clear()
			return ConnectorCheckSource{}, err
		}
		value, err := secrets.GetSecretValue(ctx, metadata)
		if err != nil {
			source.Clear()
			return ConnectorCheckSource{}, err
		}
		if metadata.Record.Secret.Kind != core.SecretKindEnvVar ||
			secretrecord.ValidateSecretValueBinding(metadata.Record, value) != nil {
			clear(value.Ciphertext)
			source.Clear()
			return ConnectorCheckSource{}, errs.New(
				errs.KindValidationFailed,
				"connector check requires a bound env_var Secret",
			)
		}
		source.secrets = append(
			source.secrets,
			connectorCheckSecret{name: string(name), reference: credential.SecretRef, metadata: metadata, value: value},
		)
	}
	return source, nil
}
