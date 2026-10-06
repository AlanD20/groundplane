package connectors

import (
	"github.com/AlanD20/groundplane/internal/core"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"unicode/utf8"
)

func applyConnectorEdit(
	current connectorrecord.Record,
	input apiTypes.ConnectorEditRequest,
) (connectorrecord.Record, error) {
	connector := current.Connector
	if input.Name != nil {
		connector.Name = *input.Name
	}
	if input.Endpoint != nil {
		connector.Endpoint = *input.Endpoint
	}
	if input.Bucket != nil {
		connector.Bucket = *input.Bucket
	}
	if input.Prefix != nil {
		connector.Prefix = *input.Prefix
	}
	if input.Region != nil {
		connector.Region = *input.Region
	}
	if input.PathStyle != nil {
		connector.PathStyle = *input.PathStyle
	}
	connector.Credentials = make(map[core.ConnectorCredentialName]core.ConnectorCredential, 2)
	for name, credential := range current.Connector.Credentials {
		connector.Credentials[name] = credential
	}
	for name, change := range input.Credentials {
		if name != string(core.ConnectorCredentialAccessKey) && name != string(core.ConnectorCredentialSecretKey) {
			return connectorrecord.Record{}, errs.New(
				errs.KindValidationFailed,
				"connector credential name must be access_key or secret_key",
			)
		}
		if (change.SecretRef == "") == (change.Value == "") || !utf8.ValidString(change.Value) ||
			len(change.Value) > apiTypes.MaximumSecretValueBytes {
			return connectorrecord.Record{}, errs.New(
				errs.KindValidationFailed,
				"connector credential requires exactly one valid secret_ref or non-empty value",
			)
		}
		credential := core.ConnectorCredential{Kind: core.ConnectorCredentialDirect}
		if change.SecretRef != "" {
			credential = core.ConnectorCredential{Kind: core.ConnectorCredentialSecretRef, SecretRef: change.SecretRef}
		}
		connector.Credentials[core.ConnectorCredentialName(name)] = credential
	}
	return connectorrecord.NewRecord(connector)
}
