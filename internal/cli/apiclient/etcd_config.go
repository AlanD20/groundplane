package apiclient

import (
	"bytes"
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func (c *Client) ShowEtcdConfig(ctx context.Context) (apiTypes.EtcdConfigDocument, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.EtcdConfigDocument{}, err
	}
	const path = "/api/v1/etcd/config"
	response, err := client.EtcdConfigShowWithResponse(ctx)
	if err != nil {
		return apiTypes.EtcdConfigDocument{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK); err != nil {
		return apiTypes.EtcdConfigDocument{}, err
	}
	var document apiTypes.EtcdConfigDocument
	if err := decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), &document); err != nil {
		return document, err
	}
	return document, nil
}

func (c *Client) SetEtcdConfig(
	ctx context.Context,
	replacement apiTypes.EtcdConfigReplacement,
) (apiTypes.EtcdConfigDocument, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.EtcdConfigDocument{}, err
	}
	const path = "/api/v1/etcd/config"
	response, err := client.EtcdConfigSetWithResponse(
		ctx,
		&generated.EtcdConfigSetParams{IdempotencyKey: ids.NewULID()},
		generated.EtcdConfigSetJSONRequestBody{
			Content:          replacement.Content,
			ExpectedRevision: replacement.ExpectedRevision,
		},
	)
	if err != nil {
		return apiTypes.EtcdConfigDocument{}, generatedCallError(ctx, http.MethodPut, path, err)
	}
	if err := generatedResponseError(http.MethodPut, path, response.HTTPResponse, response.Body, http.StatusOK); err != nil {
		return apiTypes.EtcdConfigDocument{}, err
	}
	var document apiTypes.EtcdConfigDocument
	if err := decodeSingleJSON(http.MethodPut, path, bytes.NewReader(response.Body), &document); err != nil {
		return document, err
	}
	return document, nil
}
func (c *Client) ApplyEtcdConfig(ctx context.Context, revision string) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	const path = "/api/v1/etcd/config/apply"
	response, err := client.EtcdConfigApplyWithResponse(
		ctx,
		&generated.EtcdConfigApplyParams{IdempotencyKey: ids.NewULID()},
		generated.EtcdConfigApplyJSONRequestBody{ExpectedRevision: revision},
	)
	if err != nil {
		return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodPost, path, err)
	}
	if err := generatedResponseError(http.MethodPost, path, response.HTTPResponse, response.Body, http.StatusAccepted); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	return generatedTaskAccepted(http.MethodPost, path, response.Body, response.JSON202)
}
