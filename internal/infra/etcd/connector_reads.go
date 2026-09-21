package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	recordquery "github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
	"github.com/AlanD20/groundplane/pkg/errs"
)

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
	return recordquery.Get(
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
	return recordquery.ListIndex(
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
