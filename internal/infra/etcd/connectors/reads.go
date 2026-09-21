package connectors

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	recordquery "github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Reader) GetConnector(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[Record], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindConnector, id); err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	return recordquery.Get(
		ctx,
		repository.store,
		RecordKey(id),
		id,
		errs.KindConnectorNotFound,
		DecodeRecord,
		func(record Record) string { return record.Connector.ID },
	)
}

func (repository *Reader) GetConnectorCredentials(
	ctx context.Context,
	current etcdstore.Versioned[Record],
) (EncryptedCredentials, error) {
	if err := ValidateConnectorVersion(current); err != nil {
		return EncryptedCredentials{}, err
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys:     []string{CredentialValueKey(current.Record.Connector.ID)},
		Revision: current.ReadRevision,
	})
	if err != nil {
		return EncryptedCredentials{}, err
	}
	if result == nil || len(result.Values) != 1 || result.Values[0] == nil {
		return EncryptedCredentials{}, errs.New(
			errs.KindInternal,
			"Connector encrypted credentials are missing",
		)
	}
	value, err := DecodeEncryptedCredentials(result.Values[0].Value)
	if err != nil || value.ConnectorID != current.Record.Connector.ID {
		clear(value.Ciphertext)
		return EncryptedCredentials{}, CorruptRecord()
	}
	return value, nil
}

func (repository *Reader) ListConnectors(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[Record], error) {
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Page[Record]{}, err
	}
	return recordquery.ListIndex(
		ctx,
		repository.store,
		"connectors",
		"environment",
		environmentID,
		ConnectorEnvironmentPrefix(environmentID),
		RecordKey,
		ids.KindConnector,
		request,
		DecodeRecord,
		func(record Record) string { return record.Connector.ID },
		func(record Record) bool { return record.Connector.EnvironmentID == environmentID },
	)
}
