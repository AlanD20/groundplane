package etcd

import (
	"context"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	connectorEnvironmentIndexPrefix = "/v1/indexes/connectors/by-environment/"
	connectorNameIndexPrefix        = "/v1/indexes/connectors/by-name/environment/"
)

type ConnectorRepository struct {
	store hierarchyStore
}

func NewConnectorRepository(store etcdstore.Store) (*ConnectorRepository, error) {
	return newConnectorRepository(store)
}

func newConnectorRepository(store hierarchyStore) (*ConnectorRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Connector store is required")
	}
	return &ConnectorRepository{store: store}, nil
}

func (repository *ConnectorRepository) CreateConnector(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	record connectorrecord.Record,
	credentials connectorrecord.EncryptedCredentials,
) (etcdstore.Versioned[connectorrecord.Record], error) {
	if err := validateConnectorHierarchy(ctx, environment, project, record); err != nil {
		return etcdstore.Versioned[connectorrecord.Record]{}, err
	}
	if credentials.ConnectorID != record.Connector.ID {
		return etcdstore.Versioned[connectorrecord.Record]{}, errs.New(
			errs.KindValidationFailed,
			"Connector encrypted credentials do not match the Connector",
		)
	}
	secretFence, err := repository.loadConnectorSecretReferenceFence(ctx, project.Record.ID, record)
	if err != nil {
		return etcdstore.Versioned[connectorrecord.Record]{}, err
	}
	primaryValue, err := connectorrecord.EncodeRecord(record)
	if err != nil {
		return etcdstore.Versioned[connectorrecord.Record]{}, err
	}
	defer clear(primaryValue)
	credentialValue, err := connectorrecord.EncodeEncryptedCredentials(credentials)
	if err != nil {
		return etcdstore.Versioned[connectorrecord.Record]{}, err
	}
	defer clear(credentialValue)
	connector := record.Connector
	fence, evidence, err := repository.loadConnectorMutationFence(
		ctx,
		environment,
		project,
		[]string{
			connectorrecord.RecordKey(connector.ID),
			connectorNameKey(connector.EnvironmentID, connector.Name),
			connectorEnvironmentKey(connector.EnvironmentID, connector.ID),
			connectorrecord.CredentialValueKey(connector.ID),
			deletionTombstoneKey(string(DeletionTargetConnector), connector.ID),
		},
	)
	if err != nil {
		return etcdstore.Versioned[connectorrecord.Record]{}, err
	}
	clearKeyValues(evidence.Values)
	epochMutation, err := fence.epochRewriteMutation()
	if err != nil {
		return etcdstore.Versioned[connectorrecord.Record]{}, err
	}
	defer clear(epochMutation.Value)
	conditions := append(connectorCreateConditions(record), fence.transactionConditions()...)
	conditions = append(conditions, secretFence.conditions...)
	result, err := repository.store.Transact(ctx, conditions, []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: connectorrecord.RecordKey(record.Connector.ID), Value: primaryValue},
		{
			Type:  etcdstore.MutationPut,
			Key:   connectorEnvironmentKey(record.Connector.EnvironmentID, record.Connector.ID),
			Value: []byte(record.Connector.ID),
		},
		{
			Type:  etcdstore.MutationPut,
			Key:   connectorNameKey(record.Connector.EnvironmentID, record.Connector.Name),
			Value: []byte(record.Connector.ID),
		},
		{
			Type:  etcdstore.MutationPut,
			Key:   connectorrecord.CredentialValueKey(record.Connector.ID),
			Value: credentialValue,
		},
		epochMutation,
	})
	if err != nil {
		return etcdstore.Versioned[connectorrecord.Record]{}, err
	}
	if !result.Succeeded {
		defer clearKeyValues(result.FailureReads)
		return etcdstore.Versioned[connectorrecord.Record]{}, classifyConnectorCreateConflict(
			result.FailureReads, fence, len(secretFence.conditions),
		)
	}
	return etcdstore.Versioned[connectorrecord.Record]{
		Record: record, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func (repository *ConnectorRepository) CreateConnectorIdempotent(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	record connectorrecord.Record,
	credentials connectorrecord.EncryptedCredentials,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateConnectorHierarchy(ctx, environment, project, record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if credentials.ConnectorID != record.Connector.ID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Connector encrypted credentials do not match the Connector",
		)
	}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerDirect || marker.State != idempotencyrecord.IdempotencyMarkerCompleted ||
		marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != record.Connector.EnvironmentID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Connector creation marker must be a completed Environment-scoped direct mutation",
		)
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if existing, found, err := existingIdempotencyTransaction(
		ctx,
		repository.store,
		marker,
	); err != nil ||
		found {
		return existing, err
	}
	secretFence, err := repository.loadConnectorSecretReferenceFence(ctx, project.Record.ID, record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	primaryValue, err := connectorrecord.EncodeRecord(record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	credentialValue, err := connectorrecord.EncodeEncryptedCredentials(credentials)
	if err != nil {
		clear(primaryValue)
		return IdempotencyTransactionResult{}, err
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: connectorrecord.RecordKey(record.Connector.ID), Value: primaryValue},
		{
			Type:  etcdstore.MutationPut,
			Key:   connectorEnvironmentKey(record.Connector.EnvironmentID, record.Connector.ID),
			Value: []byte(record.Connector.ID),
		},
		{
			Type:  etcdstore.MutationPut,
			Key:   connectorNameKey(record.Connector.EnvironmentID, record.Connector.Name),
			Value: []byte(record.Connector.ID),
		},
		{
			Type:  etcdstore.MutationPut,
			Key:   connectorrecord.CredentialValueKey(record.Connector.ID),
			Value: credentialValue,
		},
	}
	connector := record.Connector
	fence, evidence, err := repository.loadConnectorMutationFence(
		ctx,
		environment,
		project,
		[]string{
			connectorrecord.RecordKey(connector.ID),
			connectorNameKey(connector.EnvironmentID, connector.Name),
			connectorEnvironmentKey(connector.EnvironmentID, connector.ID),
			connectorrecord.CredentialValueKey(connector.ID),
			deletionTombstoneKey(string(DeletionTargetConnector), connector.ID),
		},
	)
	if err != nil {
		clearMutationValues(mutations)
		return IdempotencyTransactionResult{}, err
	}
	clearKeyValues(evidence.Values)
	epochMutation, err := fence.epochRewriteMutation()
	if err != nil {
		clearMutationValues(mutations)
		return IdempotencyTransactionResult{}, err
	}
	mutations = append(mutations, epochMutation)
	defer clearMutationValues(mutations)
	conditions := append(connectorCreateConditions(record), fence.transactionConditions()...)
	conditions = append(conditions, secretFence.conditions...)
	plan, err := newIdempotencyMutationPlan(
		conditions,
		mutations,
		func(_ int64, values []*etcdstore.KeyValue) error {
			return classifyConnectorCreateConflict(values, fence, len(secretFence.conditions))
		},
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func (repository *ConnectorRepository) GetConnector(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[connectorrecord.Record], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[connectorrecord.Record]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindConnector, id); err != nil {
		return etcdstore.Versioned[connectorrecord.Record]{}, err
	}
	return getRecord(
		ctx,
		repository.store,
		connectorrecord.RecordKey(id),
		id,
		errs.KindConnectorNotFound,
		connectorrecord.DecodeRecord,
		func(record connectorrecord.Record) string { return record.Connector.ID },
	)
}

func (repository *ConnectorRepository) GetConnectorCredentials(
	ctx context.Context,
	current etcdstore.Versioned[connectorrecord.Record],
) (connectorrecord.EncryptedCredentials, error) {
	if err := validateConnectorVersion(current); err != nil {
		return connectorrecord.EncryptedCredentials{}, err
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys:     []string{connectorrecord.CredentialValueKey(current.Record.Connector.ID)},
		Revision: current.ReadRevision,
	})
	if err != nil {
		return connectorrecord.EncryptedCredentials{}, err
	}
	if result == nil || len(result.Values) != 1 || result.Values[0] == nil {
		return connectorrecord.EncryptedCredentials{}, errs.New(
			errs.KindInternal,
			"Connector encrypted credentials are missing",
		)
	}
	value, err := connectorrecord.DecodeEncryptedCredentials(result.Values[0].Value)
	if err != nil || value.ConnectorID != current.Record.Connector.ID {
		clear(value.Ciphertext)
		return connectorrecord.EncryptedCredentials{}, connectorrecord.CorruptRecord()
	}
	return value, nil
}

func (repository *ConnectorRepository) ListConnectors(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[connectorrecord.Record], error) {
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Page[connectorrecord.Record]{}, err
	}
	return listIndexPage(
		ctx,
		repository.store,
		"connectors",
		"environment",
		environmentID,
		connectorEnvironmentPrefix(environmentID),
		connectorrecord.RecordKey,
		ids.KindConnector,
		request,
		connectorrecord.DecodeRecord,
		func(record connectorrecord.Record) string { return record.Connector.ID },
		func(record connectorrecord.Record) bool { return record.Connector.EnvironmentID == environmentID },
	)
}

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
		{Key: deletionTombstoneKey(string(DeletionTargetConnector), connector.ID)},
	}
	return conditions
}

type connectorSecretReferenceFence struct {
	conditions []etcdstore.Condition
}

type connectorSecretCandidate struct {
	reference     string
	projectIndex  *etcdstore.KeyValue
	platformIndex *etcdstore.KeyValue
}

func (repository *ConnectorRepository) loadConnectorSecretReferenceFence(
	ctx context.Context,
	projectID string,
	record connectorrecord.Record,
) (connectorSecretReferenceFence, error) {
	references := connectorSecretReferences(record)
	if len(references) == 0 {
		return connectorSecretReferenceFence{}, nil
	}
	indexKeys := make([]string, 0, len(references)*2)
	for _, reference := range references {
		indexKeys = append(indexKeys,
			secretKeyIndexKey(core.SecretScopeProject, projectID, reference),
			secretKeyIndexKey(core.SecretScopePlatform, "", reference),
		)
	}
	indexes, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: indexKeys})
	if err != nil {
		return connectorSecretReferenceFence{}, err
	}
	if indexes == nil || indexes.ReadRevision <= 0 || len(indexes.Values) != len(indexKeys) {
		return connectorSecretReferenceFence{}, errs.New(
			errs.KindInternal,
			"Connector credential Secret index evidence is incomplete",
		)
	}
	defer clearKeyValues(indexes.Values)
	candidates := make([]connectorSecretCandidate, len(references))
	candidateKeys := make([]string, 0, len(references)*6)
	for index, reference := range references {
		projectIndex := indexes.Values[index*2]
		platformIndex := indexes.Values[index*2+1]
		if err := validateConnectorSecretIndex(indexKeys[index*2], projectIndex); err != nil {
			return connectorSecretReferenceFence{}, err
		}
		if err := validateConnectorSecretIndex(indexKeys[index*2+1], platformIndex); err != nil {
			return connectorSecretReferenceFence{}, err
		}
		candidates[index] = connectorSecretCandidate{
			reference: reference, projectIndex: projectIndex, platformIndex: platformIndex,
		}
		for _, selected := range []*etcdstore.KeyValue{projectIndex, platformIndex} {
			if selected == nil {
				continue
			}
			secretID := string(selected.Value)
			candidateKeys = append(candidateKeys,
				secretrecord.RecordKey(secretID),
				deletionTombstoneKey(string(DeletionTargetSecret), secretID),
				secretrecord.ValueKey(secretID),
			)
		}
	}
	if len(candidateKeys) == 0 {
		return connectorSecretReferenceFence{}, errs.New(
			errs.KindSecretNotFound,
			"Connector credential Secret was not found in scope",
		)
	}
	values, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: candidateKeys, Revision: indexes.ReadRevision,
	})
	if err != nil {
		return connectorSecretReferenceFence{}, err
	}
	if values == nil || values.ReadRevision != indexes.ReadRevision || len(values.Values) != len(candidateKeys) {
		return connectorSecretReferenceFence{}, errs.New(
			errs.KindInternal,
			"Connector credential Secret evidence is incomplete",
		)
	}
	defer clearKeyValues(values.Values)
	fence := connectorSecretReferenceFence{conditions: make([]etcdstore.Condition, 0, len(candidateKeys)+len(indexKeys))}
	offset := 0
	for _, candidate := range candidates {
		projectIndexKey := secretKeyIndexKey(core.SecretScopeProject, projectID, candidate.reference)
		fence.conditions = append(
			fence.conditions,
			connectorSecretIndexCondition(projectIndexKey, candidate.projectIndex),
		)
		selected, selectedConditions, consumed, selectedErr := selectConnectorCredentialSecret(
			candidate.reference,
			projectID,
			core.SecretScopeProject,
			candidate.projectIndex,
			values.Values[offset:],
		)
		offset += consumed
		fence.conditions = append(fence.conditions, selectedConditions...)
		if selectedErr != nil {
			return connectorSecretReferenceFence{}, selectedErr
		}
		if selected {
			if candidate.platformIndex != nil {
				offset += 3
			}
			continue
		}
		platformIndexKey := secretKeyIndexKey(core.SecretScopePlatform, "", candidate.reference)
		fence.conditions = append(
			fence.conditions,
			connectorSecretIndexCondition(platformIndexKey, candidate.platformIndex),
		)
		selected, selectedConditions, consumed, selectedErr = selectConnectorCredentialSecret(
			candidate.reference,
			projectID,
			core.SecretScopePlatform,
			candidate.platformIndex,
			values.Values[offset:],
		)
		offset += consumed
		fence.conditions = append(fence.conditions, selectedConditions...)
		if selectedErr != nil {
			return connectorSecretReferenceFence{}, selectedErr
		}
		if !selected {
			return connectorSecretReferenceFence{}, errs.New(
				errs.KindSecretNotFound,
				"Connector credential Secret was not found in scope",
			)
		}
	}
	if offset != len(values.Values) {
		return connectorSecretReferenceFence{}, errs.New(
			errs.KindInternal,
			"Connector credential Secret evidence is inconsistent",
		)
	}
	return fence, nil
}

func (repository *ConnectorRepository) loadConnectorMutationFence(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	domainKeys []string,
) (environmentMutationFenceEvidence, *etcdstore.GetManyResult, error) {
	keys := append([]string(nil), domainKeys...)
	environmentIndex := len(keys)
	keys = append(keys, hierarchyrecord.EnvironmentKey(environment.Record.ID))
	projectIndex := len(keys)
	keys = append(keys, hierarchyrecord.ProjectKey(project.Record.ID))
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return environmentMutationFenceEvidence{}, nil, err
	}
	if result == nil || result.ReadRevision <= 0 || len(result.Values) != len(keys) {
		return environmentMutationFenceEvidence{}, nil, errs.New(
			errs.KindInternal,
			"Connector mutation fixed-revision evidence is incomplete",
		)
	}
	for index, value := range result.Values {
		if value != nil && value.Key != keys[index] {
			clearKeyValues(result.Values)
			return environmentMutationFenceEvidence{}, nil, errs.New(
				errs.KindInternal,
				"Connector mutation fixed-revision evidence is corrupt",
			)
		}
	}
	if result.Values[environmentIndex] == nil {
		clearKeyValues(result.Values)
		return environmentMutationFenceEvidence{}, nil, errs.New(
			errs.KindEnvironmentNotFound,
			"Connector Environment was not found",
		)
	}
	if result.Values[environmentIndex].ModRevision != environment.Revision {
		clearKeyValues(result.Values)
		return environmentMutationFenceEvidence{}, nil, stateConflict(
			"Connector Environment",
			environment.Record.ID,
		)
	}
	if result.Values[projectIndex] == nil {
		clearKeyValues(result.Values)
		return environmentMutationFenceEvidence{}, nil, errs.New(
			errs.KindProjectNotFound,
			"Connector Project was not found",
		)
	}
	if result.Values[projectIndex].ModRevision != project.Revision {
		clearKeyValues(result.Values)
		return environmentMutationFenceEvidence{}, nil, stateConflict(
			"Connector Project",
			project.Record.ID,
		)
	}
	fence, err := loadOrdinaryEnvironmentMutationFence(
		ctx,
		repository.store,
		environment.Record.ID,
		result.ReadRevision,
	)
	if err != nil {
		clearKeyValues(result.Values)
		return environmentMutationFenceEvidence{}, nil, err
	}
	return fence, result, nil
}

func validateConnectorHierarchy(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	record connectorrecord.Record,
) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	if err := hierarchyrecord.ValidateEnvironment(environment.Record); err != nil {
		return err
	}
	if err := hierarchyrecord.ValidateProject(project.Record); err != nil {
		return err
	}
	if err := connectorrecord.ValidateRecord(record); err != nil {
		return err
	}
	if environment.Revision <= 0 || environment.ReadRevision <= 0 || project.Revision <= 0 ||
		project.ReadRevision <= 0 {
		return errs.New(errs.KindValidationFailed, "Connector owner revisions are invalid")
	}
	if record.Connector.EnvironmentID != environment.Record.ID ||
		environment.Record.ProjectID != project.Record.ID {
		return errs.New(
			errs.KindConnectorScopeInvalid,
			"Connector must have exactly one Environment owner",
		)
	}
	return nil
}

func validateConnectorVersion(current etcdstore.Versioned[connectorrecord.Record]) error {
	if err := connectorrecord.ValidateRecord(current.Record); err != nil {
		return err
	}
	if current.Revision <= 0 || current.ReadRevision < current.Revision {
		return errs.New(errs.KindValidationFailed, "Connector revision is invalid")
	}
	return nil
}

func classifyConnectorCreateConflict(
	reads []*etcdstore.KeyValue,
	fence environmentMutationFenceEvidence,
	secretConditionCount int,
) error {
	want := 5 + len(fence.conditions) + secretConditionCount
	if len(reads) != want {
		return errs.New(errs.KindInternal, "Connector create conflict read is incomplete")
	}
	if reads[0] != nil || reads[2] != nil || reads[3] != nil {
		return errs.New(errs.KindStateConflict, "Connector stable identity already exists")
	}
	if reads[1] != nil {
		return errs.New(errs.KindStateConflict, "Connector name already exists in the Environment")
	}
	if reads[4] != nil {
		return errs.New(errs.KindResourceInUse, "Connector deletion is in progress")
	}
	if conflict := fence.classifyCAS(reads[5 : 5+len(fence.conditions)]); conflict != nil {
		return conflict
	}
	if secretConditionCount > 0 {
		return errs.New(errs.KindStateConflict, "Connector credential Secret changed during creation")
	}
	return errs.New(errs.KindStateConflict, "Connector owner changed or is being deleted")
}
