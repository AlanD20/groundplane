package apiclient

import (
	"context"
	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/http"
)

func (c *Client) EditConnector(
	ctx context.Context,
	id string,
	input apiTypes.ConnectorEditRequest,
) (apiTypes.Connector, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Connector{}, err
	}
	path := "/api/v1/connectors/" + id
	snapshot, err := client.ConnectorShowWithResponse(ctx, id)
	if err != nil {
		return apiTypes.Connector{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(http.MethodGet, path, snapshot.HTTPResponse, snapshot.Body, http.StatusOK); err != nil {
		return apiTypes.Connector{}, err
	}
	etag := snapshot.HTTPResponse.Header.Get("ETag")
	if etag == "" {
		return apiTypes.Connector{}, errs.New(errs.KindInternal, "connector response is missing ETag")
	}
	credentials := make(map[string]generated.ConnectorCredentialInput, len(input.Credentials))
	for name, input := range input.Credentials {
		credential := generated.ConnectorCredentialInput{}
		if input.SecretRef != "" {
			value := input.SecretRef
			credential.SecretRef = &value
		}
		if input.Value != "" {
			value := input.Value
			credential.Value = &value
		}
		credentials[name] = credential
	}
	body := generated.ConnectorEditRequest{
		Name: input.Name, Endpoint: input.Endpoint, Bucket: input.Bucket, Prefix: input.Prefix,
		Region: input.Region, PathStyle: input.PathStyle,
	}
	if len(credentials) > 0 {
		body.Credentials = &struct {
			AccessKey *generated.ConnectorCredentialInput `json:"access_key,omitempty"`
			SecretKey *generated.ConnectorCredentialInput `json:"secret_key,omitempty"`
		}{}
		for name, credential := range credentials {
			switch name {
			case "access_key":
				body.Credentials.AccessKey = &credential
			case "secret_key":
				body.Credentials.SecretKey = &credential
			default:
				return apiTypes.Connector{}, errs.New(
					errs.KindValidationFailed,
					"connector credential name must be access_key or secret_key",
				)
			}
		}
	}
	response, err := client.ConnectorEditWithResponse(
		ctx,
		id,
		&generated.ConnectorEditParams{IfMatch: etag, IdempotencyKey: ids.NewULID()},
		body,
	)
	if err != nil {
		return apiTypes.Connector{}, generatedCallError(ctx, http.MethodPatch, path, err)
	}
	if err := generatedResponseError(http.MethodPatch, path, response.HTTPResponse, response.Body, http.StatusOK); err != nil {
		return apiTypes.Connector{}, err
	}
	if response.JSON200 == nil {
		return apiTypes.Connector{}, errs.New(errs.KindInternal, "connector edit response is missing metadata")
	}
	return connectorFromGenerated(*response.JSON200), nil
}
