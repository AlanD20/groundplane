package connectormutations

import (
	"context"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Repository) CreateConnector(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	record connectorrecord.Record,
	credentials connectorrecord.EncryptedCredentials,
) (etcdstore.Versioned[connectorrecord.Record], error) {
	if err := ValidateConnectorHierarchy(ctx, environment, project, record); err != nil {
		return etcdstore.Versioned[connectorrecord.Record]{}, err
	}
	if credentials.ConnectorID != record.Connector.ID {
		return etcdstore.Versioned[connectorrecord.Record]{}, errs.New(
			errs.KindValidationFailed,
			"Connector encrypted credentials do not match the Connector",
		)
	}
	secretFence, err := repository.LoadSecretReferenceFence(ctx, project.Record.ID, record)
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
	fence, evidence, err := repository.LoadConnectorMutationFence(
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
	conditions = append(conditions, secretFence.Conditions()...)
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
		return etcdstore.Versioned[connectorrecord.Record]{}, ClassifyConnectorCreateConflict(
			result.FailureReads, fence, secretFence.ConditionCount(),
		)
	}
	return etcdstore.Versioned[connectorrecord.Record]{
		Record: record, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}
