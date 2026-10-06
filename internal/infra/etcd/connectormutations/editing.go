package connectormutations

import (
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type EditPublication struct {
	Conditions []etcdstore.Condition
	Mutations  []etcdstore.Mutation
}

func ValidateEditMarker(record connectorrecord.Record, marker idempotencyrecord.IdempotencyMarker) error {
	if marker.Kind != idempotencyrecord.IdempotencyMarkerDirect || marker.State != idempotencyrecord.IdempotencyMarkerCompleted ||
		marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment || marker.Locator.ScopeID != record.Connector.EnvironmentID ||
		marker.Locator.Method != "PATCH" || marker.Locator.Route != "/api/v1/connectors/{id}" ||
		marker.Response.Status != 200 {
		return errs.New(errs.KindValidationFailed, "connector edit marker does not match the mutation")
	}
	return idempotencyrecord.ValidateIdempotencyMarker(marker)
}

func (repository *Repository) PrepareConnectorEdit(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	current etcdstore.Versioned[connectorrecord.Record],
	credentialDigest string,
	record connectorrecord.Record,
	credentials connectorrecord.EncryptedCredentials,
) (EditPublication, error) {
	if err := ValidateConnectorHierarchy(ctx, environment, project, record); err != nil {
		return EditPublication{}, err
	}
	previous, next := current.Record.Connector, record.Connector
	if current.Revision <= 0 || previous.ID != next.ID || previous.EnvironmentID != next.EnvironmentID ||
		previous.Kind != next.Kind ||
		credentials.ConnectorID != next.ID {
		return EditPublication{}, errs.New(
			errs.KindValidationFailed,
			"connector edit must retain its identity, owner and kind",
		)
	}
	keys := []string{
		connectorrecord.RecordKey(next.ID), connectorrecord.CredentialValueKey(next.ID),
		connectorrecord.ConnectorNameKey(next.EnvironmentID, previous.Name),
		connectorrecord.ConnectorEnvironmentKey(next.EnvironmentID, next.ID),
		deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetConnector), next.ID),
	}
	if previous.Name != next.Name {
		keys = append(keys, connectorrecord.ConnectorNameKey(next.EnvironmentID, next.Name))
	}
	fence, evidence, err := repository.LoadConnectorMutationFence(ctx, environment, project, keys)
	if err != nil {
		return EditPublication{}, err
	}
	defer etcdstore.ClearValues(evidence.Values)
	values := evidence.Values
	if values[0] == nil || values[0].ModRevision != current.Revision {
		return EditPublication{}, errs.New(
			errs.KindStateConflict,
			"connector changed; reload it before editing",
		)
	}
	if values[1] == nil || values[2] == nil || values[3] == nil || string(values[2].Value) != next.ID ||
		string(values[3].Value) != next.ID {
		return EditPublication{}, errs.New(errs.KindInternal, "connector edit authority is incomplete")
	}
	storedCredentials, err := connectorrecord.DecodeEncryptedCredentials(values[1].Value)
	if err != nil {
		return EditPublication{}, err
	}
	defer clear(storedCredentials.Ciphertext)
	if storedCredentials.CiphertextSHA256 != credentialDigest {
		return EditPublication{}, errs.New(
			errs.KindStateConflict,
			"connector credentials changed; reload it before editing",
		)
	}
	if values[4] != nil {
		return EditPublication{}, errs.New(errs.KindResourceInUse, "connector deletion is in progress")
	}
	if len(keys) == 6 && values[5] != nil {
		return EditPublication{}, errs.New(
			errs.KindStateConflict,
			"connector name already exists in the Environment",
		)
	}
	conditions := make([]etcdstore.Condition, 0, len(keys))
	for index, key := range keys {
		condition := etcdstore.Condition{Key: key}
		if values[index] != nil {
			condition.ModRevision = values[index].ModRevision
		}
		conditions = append(conditions, condition)
	}
	if previous.Endpoint != next.Endpoint || previous.Bucket != next.Bucket || previous.Prefix != next.Prefix ||
		previous.Region != next.Region ||
		previous.PathStyle != next.PathStyle {
		retained, retainErr := requireConnectorDestinationUnused(
			ctx,
			repository.store,
			next.ID,
			next.EnvironmentID,
			evidence.ReadRevision,
		)
		if retainErr != nil {
			return EditPublication{}, retainErr
		}
		conditions = append(conditions, retained...)
	}
	secretFence, err := repository.LoadSecretReferenceFence(ctx, project.Record.ID, record)
	if err != nil {
		return EditPublication{}, err
	}
	conditions = append(conditions, fence.TransactionConditions()...)
	conditions = append(conditions, secretFence.Conditions()...)
	primary, err := connectorrecord.EncodeRecord(record)
	if err != nil {
		return EditPublication{}, err
	}
	defer clear(primary)
	encrypted, err := connectorrecord.EncodeEncryptedCredentials(credentials)
	if err != nil {
		return EditPublication{}, err
	}
	defer clear(encrypted)
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		return EditPublication{}, err
	}
	defer clear(epoch.Value)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: keys[0], Value: append([]byte(nil), primary...)},
		{
			Type:  etcdstore.MutationPut,
			Key:   keys[1],
			Value: append([]byte(nil), encrypted...),
		}, {Type: epoch.Type, Key: epoch.Key, Value: append([]byte(nil), epoch.Value...)},
	}
	if previous.Name != next.Name {
		mutations = append(mutations,
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: keys[2]},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: keys[5], Value: []byte(next.ID)},
		)
	}
	return EditPublication{Conditions: conditions, Mutations: mutations}, nil
}

func requireConnectorDestinationUnused(
	ctx context.Context,
	store persistenceStore,
	id, environmentID string,
	revision int64,
) ([]etcdstore.Condition, error) {
	prefixes := []string{
		backupruntime.BackupRecoveryPointConnectorPrefix + id + "/",
		backupruntime.BackupOrphanConnectorPrefix + id + "/",
	}
	conditions := make([]etcdstore.Condition, 0, len(prefixes))
	for index, prefix := range prefixes {
		result, err := store.Range(ctx, etcdstore.RangeRequest{Prefix: prefix, Limit: 1, Revision: revision})
		if err != nil {
			return nil, err
		}
		if result == nil || result.ReadRevision != revision || len(result.Values) > 1 {
			return nil, errs.New(errs.KindInternal, "connector retained destination evidence is incomplete")
		}
		if len(result.Values) != 0 {
			return nil, ClassifyConnectorReference(index+1, result.Values[0], id, environmentID)
		}
		conditions = append(conditions, etcdstore.Condition{Key: prefix, Prefix: true})
	}
	return conditions, nil
}
