package etcd

import (
	"context"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

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
			connectorrecord.ConnectorNameKey(connector.EnvironmentID, connector.Name),
			connectorrecord.ConnectorEnvironmentKey(connector.EnvironmentID, connector.ID),
			connectorrecord.CredentialValueKey(connector.ID),
			deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetConnector), connector.ID),
		},
	)
	if err != nil {
		return etcdstore.Versioned[connectorrecord.Record]{}, err
	}
	etcdstore.ClearValues(evidence.Values)
	epochMutation, err := fence.EpochRewriteMutation()
	if err != nil {
		return etcdstore.Versioned[connectorrecord.Record]{}, err
	}
	defer clear(epochMutation.Value)
	conditions := append(connectorrecord.ConnectorCreateConditions(record), fence.TransactionConditions()...)
	conditions = append(conditions, secretFence.conditions...)
	result, err := repository.store.Transact(ctx, conditions, []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: connectorrecord.RecordKey(record.Connector.ID), Value: primaryValue},
		{
			Type:  etcdstore.MutationPut,
			Key:   connectorrecord.ConnectorEnvironmentKey(record.Connector.EnvironmentID, record.Connector.ID),
			Value: []byte(record.Connector.ID),
		},
		{
			Type:  etcdstore.MutationPut,
			Key:   connectorrecord.ConnectorNameKey(record.Connector.EnvironmentID, record.Connector.Name),
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
		defer etcdstore.ClearValues(result.FailureReads)
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
			Key:   connectorrecord.ConnectorEnvironmentKey(record.Connector.EnvironmentID, record.Connector.ID),
			Value: []byte(record.Connector.ID),
		},
		{
			Type:  etcdstore.MutationPut,
			Key:   connectorrecord.ConnectorNameKey(record.Connector.EnvironmentID, record.Connector.Name),
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
			connectorrecord.ConnectorNameKey(connector.EnvironmentID, connector.Name),
			connectorrecord.ConnectorEnvironmentKey(connector.EnvironmentID, connector.ID),
			connectorrecord.CredentialValueKey(connector.ID),
			deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetConnector), connector.ID),
		},
	)
	if err != nil {
		etcdstore.ClearMutationValues(mutations)
		return IdempotencyTransactionResult{}, err
	}
	etcdstore.ClearValues(evidence.Values)
	epochMutation, err := fence.EpochRewriteMutation()
	if err != nil {
		etcdstore.ClearMutationValues(mutations)
		return IdempotencyTransactionResult{}, err
	}
	mutations = append(mutations, epochMutation)
	defer etcdstore.ClearMutationValues(mutations)
	conditions := append(connectorrecord.ConnectorCreateConditions(record), fence.TransactionConditions()...)
	conditions = append(conditions, secretFence.conditions...)
	plan, err := NewIdempotencyMutationPlan(
		conditions,
		mutations,
		func(_ int64, values []*etcdstore.KeyValue) error {
			return classifyConnectorCreateConflict(values, fence, len(secretFence.conditions))
		},
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}
