package app

import (
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func prepareConnectorCreation(
	connectorID string,
	environmentID string,
	input apiTypes.ConnectorCreateRequest,
) (etcd.ConnectorRecord, map[string]string, error) {
	if input.PathStyle == nil {
		return etcd.ConnectorRecord{}, nil, errs.New(
			errs.KindValidationFailed,
			"Connector path_style decision is required",
		)
	}
	if len(input.Credentials) != 2 {
		return etcd.ConnectorRecord{}, nil, errs.New(
			errs.KindValidationFailed,
			"Connector credentials must contain access_key and secret_key",
		)
	}
	metadata := make(map[core.ConnectorCredentialName]core.ConnectorCredential, 2)
	direct := make(map[string]string, 2)
	for _, name := range []core.ConnectorCredentialName{
		core.ConnectorCredentialAccessKey, core.ConnectorCredentialSecretKey,
	} {
		credential, ok := input.Credentials[string(name)]
		if !ok {
			clearConnectorDirectValues(direct)
			return etcd.ConnectorRecord{}, nil, errs.New(
				errs.KindValidationFailed,
				"Connector credentials must contain access_key and secret_key",
			)
		}
		if (credential.SecretRef == "") == (credential.Value == "") {
			clearConnectorDirectValues(direct)
			return etcd.ConnectorRecord{}, nil, errs.New(
				errs.KindValidationFailed,
				"Connector credential requires exactly one secret_ref or value",
			)
		}
		if credential.SecretRef != "" {
			metadata[name] = core.ConnectorCredential{
				Kind: core.ConnectorCredentialSecretRef, SecretRef: credential.SecretRef,
			}
			continue
		}
		metadata[name] = core.ConnectorCredential{Kind: core.ConnectorCredentialDirect}
		direct[string(name)] = credential.Value
	}
	record, err := etcd.NewConnectorRecord(core.Connector{
		ID: connectorID, EnvironmentID: environmentID, Name: input.Name,
		Kind: core.ConnectorKind(input.Kind), Endpoint: input.Endpoint, Bucket: input.Bucket,
		Prefix: input.Prefix, Region: input.Region, PathStyle: *input.PathStyle, Credentials: metadata,
	})
	if err != nil {
		clearConnectorDirectValues(direct)
		return etcd.ConnectorRecord{}, nil, err
	}
	return record, direct, nil
}

func connectorResponse(record etcd.ConnectorRecord) apiTypes.Connector {
	connector := record.Connector
	credentials := make(map[string]apiTypes.ConnectorCredential, len(connector.Credentials))
	for name, credential := range connector.Credentials {
		credentials[string(name)] = apiTypes.ConnectorCredential{
			Kind: apiTypes.ConnectorCredentialKind(credential.Kind), SecretRef: credential.SecretRef,
		}
	}
	return apiTypes.Connector{
		ID: connector.ID, EnvironmentID: connector.EnvironmentID, Name: connector.Name,
		Kind: string(connector.Kind), Endpoint: connector.Endpoint, Bucket: connector.Bucket,
		Prefix: connector.Prefix, Region: connector.Region, PathStyle: connector.PathStyle,
		Credentials: credentials,
	}
}

func clearConnectorDirectValues(values map[string]string) {
	for name := range values {
		values[name] = ""
		delete(values, name)
	}
}
