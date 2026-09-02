package apiclient

import (
	"bytes"
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func (c *Client) ShowControllerConfig(ctx context.Context) (apiTypes.ControllerConfigDocument, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.ControllerConfigDocument{}, err
	}
	const path = "/api/v1/controller/config"
	response, err := client.ControllerConfigShowWithResponse(ctx)
	if err != nil {
		return apiTypes.ControllerConfigDocument{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(
		http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.ControllerConfigDocument{}, err
	}
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.ControllerConfigDocument{}
		if err := decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), parsed); err != nil {
			return apiTypes.ControllerConfigDocument{}, err
		}
	}
	return controllerConfigDocumentFromGenerated(*parsed), nil
}

func (c *Client) SetControllerConfig(
	ctx context.Context,
	replacement apiTypes.ControllerConfigReplacement,
) (apiTypes.ControllerConfigDocument, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.ControllerConfigDocument{}, err
	}
	const path = "/api/v1/controller/config"
	response, err := client.ControllerConfigSetWithResponse(
		ctx,
		&generated.ControllerConfigSetParams{IdempotencyKey: ids.NewULID()},
		generated.ControllerConfigSetJSONRequestBody{
			Content: replacement.Content, ExpectedRevision: replacement.ExpectedRevision,
		},
	)
	if err != nil {
		return apiTypes.ControllerConfigDocument{}, generatedCallError(ctx, http.MethodPut, path, err)
	}
	if err := generatedResponseError(
		http.MethodPut, path, response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.ControllerConfigDocument{}, err
	}
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.ControllerConfigDocument{}
		if err := decodeSingleJSON(http.MethodPut, path, bytes.NewReader(response.Body), parsed); err != nil {
			return apiTypes.ControllerConfigDocument{}, err
		}
	}
	return controllerConfigDocumentFromGenerated(*parsed), nil
}

func controllerConfigDocumentFromGenerated(
	document generated.ControllerConfigDocument,
) apiTypes.ControllerConfigDocument {
	return apiTypes.ControllerConfigDocument{
		Path:            document.Path,
		Content:         document.Content,
		Revision:        document.Revision,
		RestartRequired: document.RestartRequired,
	}
}
