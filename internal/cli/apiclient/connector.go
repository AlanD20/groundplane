package apiclient

import (
	"bytes"
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func (c *Client) CreateConnector(
	ctx context.Context,
	environmentID string,
	input apiTypes.ConnectorCreateRequest,
) (apiTypes.Connector, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Connector{}, err
	}
	credentials := make(map[string]generated.ConnectorCredentialInput, len(input.Credentials))
	for name, credential := range input.Credentials {
		converted := generated.ConnectorCredentialInput{}
		if credential.SecretRef != "" {
			value := credential.SecretRef
			converted.SecretRef = &value
		}
		if credential.Value != "" {
			value := credential.Value
			converted.Value = &value
		}
		credentials[name] = converted
	}
	body := generated.ConnectorCreateRequest{
		Name: input.Name, Kind: input.Kind, Endpoint: input.Endpoint, Bucket: input.Bucket,
		Region: input.Region, PathStyle: input.PathStyle, Credentials: credentials,
	}
	if input.Prefix != "" {
		body.Prefix = &input.Prefix
	}
	response, err := client.ConnectorCreateWithResponse(
		ctx,
		&generated.ConnectorCreateParams{
			Environment: environmentID, IdempotencyKey: ids.NewULID(),
		},
		body,
	)
	if err != nil {
		return apiTypes.Connector{}, generatedCallError(ctx, http.MethodPost, "/api/v1/connectors", err)
	}
	if err := generatedResponseError(
		http.MethodPost,
		"/api/v1/connectors",
		response.HTTPResponse,
		response.Body,
		http.StatusCreated,
	); err != nil {
		return apiTypes.Connector{}, err
	}
	parsed := response.JSON201
	if parsed == nil {
		parsed = &generated.Connector{}
		if err := decodeSingleJSON(
			http.MethodPost,
			"/api/v1/connectors",
			bytes.NewReader(response.Body),
			parsed,
		); err != nil {
			return apiTypes.Connector{}, err
		}
	}
	return connectorFromGenerated(*parsed), nil
}

func (c *Client) ListConnectors(
	ctx context.Context,
	environmentID string,
	limit int,
	cursor string,
) (apiTypes.Page[apiTypes.Connector], error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Page[apiTypes.Connector]{}, err
	}
	params := &generated.ConnectorListParams{Environment: environmentID}
	if limit != 0 {
		value := int64(limit)
		params.Limit = &value
	}
	if cursor != "" {
		params.Cursor = &cursor
	}
	response, err := client.ConnectorListWithResponse(ctx, params)
	if err != nil {
		return apiTypes.Page[apiTypes.Connector]{}, generatedCallError(
			ctx,
			http.MethodGet,
			"/api/v1/connectors",
			err,
		)
	}
	if err := generatedResponseError(
		http.MethodGet,
		"/api/v1/connectors",
		response.HTTPResponse,
		response.Body,
		http.StatusOK,
	); err != nil {
		return apiTypes.Page[apiTypes.Connector]{}, err
	}
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.PageConnector{}
		if err := decodeSingleJSON(
			http.MethodGet,
			"/api/v1/connectors",
			bytes.NewReader(response.Body),
			parsed,
		); err != nil {
			return apiTypes.Page[apiTypes.Connector]{}, err
		}
	}
	items := []generated.Connector(nil)
	if parsed.Items != nil {
		items = *parsed.Items
	}
	page := apiTypes.Page[apiTypes.Connector]{Items: make([]apiTypes.Connector, len(items))}
	if parsed.NextCursor != nil {
		page.NextCursor = *parsed.NextCursor
	}
	for index, item := range items {
		page.Items[index] = connectorFromGenerated(item)
	}
	return page, nil
}

func (c *Client) ShowConnector(ctx context.Context, id string) (apiTypes.Connector, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Connector{}, err
	}
	path := "/api/v1/connectors/" + id
	response, err := client.ConnectorShowWithResponse(ctx, id)
	if err != nil {
		return apiTypes.Connector{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(
		http.MethodGet,
		path,
		response.HTTPResponse,
		response.Body,
		http.StatusOK,
	); err != nil {
		return apiTypes.Connector{}, err
	}
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.Connector{}
		if err := decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), parsed); err != nil {
			return apiTypes.Connector{}, err
		}
	}
	return connectorFromGenerated(*parsed), nil
}

func connectorFromGenerated(connector generated.Connector) apiTypes.Connector {
	prefix := ""
	if connector.Prefix != nil {
		prefix = *connector.Prefix
	}
	credentials := make(map[string]apiTypes.ConnectorCredential, len(connector.Credentials))
	for name, credential := range connector.Credentials {
		secretRef := ""
		if credential.SecretRef != nil {
			secretRef = *credential.SecretRef
		}
		credentials[name] = apiTypes.ConnectorCredential{
			Kind: apiTypes.ConnectorCredentialKind(credential.Kind), SecretRef: secretRef,
		}
	}
	return apiTypes.Connector{
		ID: connector.Id, EnvironmentID: connector.EnvironmentId, Name: connector.Name,
		Kind: connector.Kind, Endpoint: connector.Endpoint, Bucket: connector.Bucket,
		Prefix: prefix, Region: connector.Region, PathStyle: connector.PathStyle,
		Credentials: credentials,
	}
}
