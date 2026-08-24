package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	connectorEnvironmentIndexPrefix = "/v1/indexes/connectors/by-environment/"
	connectorNameIndexPrefix        = "/v1/indexes/connectors/by-name/environment/"
)

type ConnectorRepository struct {
	store hierarchyStore
}

func NewConnectorRepository(store Store) (*ConnectorRepository, error) {
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
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	record ConnectorRecord,
	credentials ConnectorEncryptedCredentials,
) (Versioned[ConnectorRecord], error) {
	if err := validateConnectorHierarchy(ctx, environment, project, record); err != nil {
		return Versioned[ConnectorRecord]{}, err
	}
	if credentials.ConnectorID != record.Connector.ID {
		return Versioned[ConnectorRecord]{}, errs.New(
			errs.KindValidationFailed,
			"Connector encrypted credentials do not match the Connector",
		)
	}
	primaryValue, err := encodeConnectorRecord(record)
	if err != nil {
		return Versioned[ConnectorRecord]{}, err
	}
	defer clear(primaryValue)
	credentialValue, err := encodeConnectorEncryptedCredentials(credentials)
	if err != nil {
		return Versioned[ConnectorRecord]{}, err
	}
	defer clear(credentialValue)
	connector := record.Connector
	fence, evidence, err := repository.loadConnectorMutationFence(
		ctx,
		environment,
		project,
		[]string{
			connectorRecordKey(connector.ID),
			connectorNameKey(connector.EnvironmentID, connector.Name),
			connectorEnvironmentKey(connector.EnvironmentID, connector.ID),
			connectorCredentialValueKey(connector.ID),
			deletionTombstoneKey(string(DeletionTargetConnector), connector.ID),
		},
	)
	if err != nil {
		return Versioned[ConnectorRecord]{}, err
	}
	clearKeyValues(evidence.Values)
	epochMutation, err := fence.epochRewriteMutation()
	if err != nil {
		return Versioned[ConnectorRecord]{}, err
	}
	defer clear(epochMutation.Value)
	conditions := append(connectorCreateConditions(record), fence.transactionConditions()...)
	result, err := repository.store.Transact(ctx, conditions, []Mutation{
		{Type: MutationPut, Key: connectorRecordKey(record.Connector.ID), Value: primaryValue},
		{
			Type:  MutationPut,
			Key:   connectorEnvironmentKey(record.Connector.EnvironmentID, record.Connector.ID),
			Value: []byte(record.Connector.ID),
		},
		{
			Type:  MutationPut,
			Key:   connectorNameKey(record.Connector.EnvironmentID, record.Connector.Name),
			Value: []byte(record.Connector.ID),
		},
		{
			Type:  MutationPut,
			Key:   connectorCredentialValueKey(record.Connector.ID),
			Value: credentialValue,
		},
		epochMutation,
	})
	if err != nil {
		return Versioned[ConnectorRecord]{}, err
	}
	if !result.Succeeded {
		defer clearKeyValues(result.FailureReads)
		return Versioned[ConnectorRecord]{}, classifyConnectorCreateConflict(
			result.FailureReads, fence,
		)
	}
	return Versioned[ConnectorRecord]{
		Record: record, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func (repository *ConnectorRepository) CreateConnectorIdempotent(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	record ConnectorRecord,
	credentials ConnectorEncryptedCredentials,
	marker IdempotencyMarker,
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
	if marker.Kind != IdempotencyMarkerDirect || marker.State != IdempotencyMarkerCompleted ||
		marker.Locator.ScopeKind != IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != record.Connector.EnvironmentID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Connector creation marker must be a completed Environment-scoped direct mutation",
		)
	}
	if err := validateIdempotencyMarker(marker); err != nil {
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
	primaryValue, err := encodeConnectorRecord(record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	credentialValue, err := encodeConnectorEncryptedCredentials(credentials)
	if err != nil {
		clear(primaryValue)
		return IdempotencyTransactionResult{}, err
	}
	mutations := []Mutation{
		{Type: MutationPut, Key: connectorRecordKey(record.Connector.ID), Value: primaryValue},
		{
			Type:  MutationPut,
			Key:   connectorEnvironmentKey(record.Connector.EnvironmentID, record.Connector.ID),
			Value: []byte(record.Connector.ID),
		},
		{
			Type:  MutationPut,
			Key:   connectorNameKey(record.Connector.EnvironmentID, record.Connector.Name),
			Value: []byte(record.Connector.ID),
		},
		{
			Type:  MutationPut,
			Key:   connectorCredentialValueKey(record.Connector.ID),
			Value: credentialValue,
		},
	}
	connector := record.Connector
	fence, evidence, err := repository.loadConnectorMutationFence(
		ctx,
		environment,
		project,
		[]string{
			connectorRecordKey(connector.ID),
			connectorNameKey(connector.EnvironmentID, connector.Name),
			connectorEnvironmentKey(connector.EnvironmentID, connector.ID),
			connectorCredentialValueKey(connector.ID),
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
	plan, err := newIdempotencyMutationPlan(
		append(connectorCreateConditions(record), fence.transactionConditions()...),
		mutations,
		func(_ int64, values []*KeyValue) error {
			return classifyConnectorCreateConflict(values, fence)
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
) (Versioned[ConnectorRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[ConnectorRecord]{}, err
	}
	if err := validateID(ids.KindConnector, id); err != nil {
		return Versioned[ConnectorRecord]{}, err
	}
	return getRecord(
		ctx,
		repository.store,
		connectorRecordKey(id),
		id,
		errs.KindConnectorNotFound,
		decodeConnectorRecord,
		func(record ConnectorRecord) string { return record.Connector.ID },
	)
}

func (repository *ConnectorRepository) GetConnectorCredentials(
	ctx context.Context,
	current Versioned[ConnectorRecord],
) (ConnectorEncryptedCredentials, error) {
	if err := validateConnectorVersion(current); err != nil {
		return ConnectorEncryptedCredentials{}, err
	}
	result, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys:     []string{connectorCredentialValueKey(current.Record.Connector.ID)},
		Revision: current.ReadRevision,
	})
	if err != nil {
		return ConnectorEncryptedCredentials{}, err
	}
	if result == nil || len(result.Values) != 1 || result.Values[0] == nil {
		return ConnectorEncryptedCredentials{}, errs.New(
			errs.KindInternal,
			"Connector encrypted credentials are missing",
		)
	}
	value, err := decodeConnectorEncryptedCredentials(result.Values[0].Value)
	if err != nil || value.ConnectorID != current.Record.Connector.ID {
		clear(value.Ciphertext)
		return ConnectorEncryptedCredentials{}, corruptConnectorRecord()
	}
	return value, nil
}

func (repository *ConnectorRepository) ListConnectors(
	ctx context.Context,
	environmentID string,
	request PageRequest,
) (Page[ConnectorRecord], error) {
	if err := validateID(ids.KindEnvironment, environmentID); err != nil {
		return Page[ConnectorRecord]{}, err
	}
	return listIndexPage(
		ctx,
		repository.store,
		"connectors",
		"environment",
		environmentID,
		connectorEnvironmentPrefix(environmentID),
		connectorRecordKey,
		ids.KindConnector,
		request,
		decodeConnectorRecord,
		func(record ConnectorRecord) string { return record.Connector.ID },
		func(record ConnectorRecord) bool { return record.Connector.EnvironmentID == environmentID },
	)
}

func connectorEnvironmentPrefix(environmentID string) string {
	return connectorEnvironmentIndexPrefix + environmentID + "/"
}

func connectorEnvironmentKey(environmentID string, connectorID string) string {
	return connectorEnvironmentPrefix(environmentID) + connectorID
}

func connectorNameKey(environmentID string, name string) string {
	return connectorNameIndexPrefix + environmentID + "/" + encodeDynamicSegment(name)
}

func connectorCreateConditions(
	record ConnectorRecord,
) []Condition {
	connector := record.Connector
	conditions := []Condition{
		{Key: connectorRecordKey(connector.ID)},
		{Key: connectorNameKey(connector.EnvironmentID, connector.Name)},
		{Key: connectorEnvironmentKey(connector.EnvironmentID, connector.ID)},
		{Key: connectorCredentialValueKey(connector.ID)},
		{Key: deletionTombstoneKey(string(DeletionTargetConnector), connector.ID)},
	}
	return conditions
}

func (repository *ConnectorRepository) loadConnectorMutationFence(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	domainKeys []string,
) (environmentMutationFenceEvidence, *GetManyResult, error) {
	keys := append([]string(nil), domainKeys...)
	environmentIndex := len(keys)
	keys = append(keys, environmentKey(environment.Record.ID))
	projectIndex := len(keys)
	keys = append(keys, projectKey(project.Record.ID))
	result, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys})
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
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	record ConnectorRecord,
) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := validateEnvironment(environment.Record); err != nil {
		return err
	}
	if err := validateProject(project.Record); err != nil {
		return err
	}
	if err := validateConnectorRecord(record); err != nil {
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

func validateConnectorVersion(current Versioned[ConnectorRecord]) error {
	if err := validateConnectorRecord(current.Record); err != nil {
		return err
	}
	if current.Revision <= 0 || current.ReadRevision < current.Revision {
		return errs.New(errs.KindValidationFailed, "Connector revision is invalid")
	}
	return nil
}

func classifyConnectorCreateConflict(
	reads []*KeyValue,
	fence environmentMutationFenceEvidence,
) error {
	want := 5 + len(fence.conditions)
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
	if conflict := fence.classifyCAS(reads[5:]); conflict != nil {
		return conflict
	}
	return errs.New(errs.KindStateConflict, "Connector owner changed or is being deleted")
}
