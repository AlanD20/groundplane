package apiclient

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func (c *Client) ListComponents(
	ctx context.Context,
	environmentID string,
	platform bool,
	kind string,
	limit int,
	cursor string,
) (apiTypes.Page[apiTypes.Component], error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Page[apiTypes.Component]{}, err
	}
	params := &generated.ComponentListParams{}
	if environmentID != "" {
		params.Environment = &environmentID
	}
	if platform {
		params.Platform = &platform
	}
	if kind != "" {
		params.Kind = &kind
	}
	if limit != 0 {
		value := int64(limit)
		params.Limit = &value
	}
	if cursor != "" {
		params.Cursor = &cursor
	}
	response, err := client.ComponentListWithResponse(ctx, params)
	if err != nil {
		return apiTypes.Page[apiTypes.Component]{}, generatedCallError(
			ctx, http.MethodGet, "/api/v1/components", err,
		)
	}
	if err := generatedResponseError(
		http.MethodGet, "/api/v1/components", response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.Page[apiTypes.Component]{}, err
	}
	var page apiTypes.Page[apiTypes.Component]
	if err := decodeSingleJSON(
		http.MethodGet, "/api/v1/components", bytes.NewReader(response.Body), &page,
	); err != nil {
		return apiTypes.Page[apiTypes.Component]{}, err
	}
	return page, nil
}

func (c *Client) ShowComponent(ctx context.Context, id string) (apiTypes.Component, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Component{}, err
	}
	path := "/api/v1/components/" + id
	response, err := client.ComponentShowWithResponse(ctx, id)
	if err != nil {
		return apiTypes.Component{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(
		http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.Component{}, err
	}
	var component apiTypes.Component
	if err := decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), &component); err != nil {
		return apiTypes.Component{}, err
	}
	return component, nil
}

func (c *Client) ShowComponentConfig(ctx context.Context, id string) (apiTypes.ComponentConfig, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.ComponentConfig{}, err
	}
	path := "/api/v1/components/" + id + "/config"
	response, err := client.ComponentConfigShowWithResponse(ctx, id)
	if err != nil {
		return apiTypes.ComponentConfig{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(
		http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.ComponentConfig{}, err
	}
	var config apiTypes.ComponentConfig
	if err := decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), &config); err != nil {
		return apiTypes.ComponentConfig{}, err
	}
	return config, nil
}

func (c *Client) SetComponentConfig(
	ctx context.Context,
	id string,
	config apiTypes.ComponentConfigMutationInput,
) (apiTypes.ComponentConfigMutationResult, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.ComponentConfigMutationResult{}, err
	}
	path := "/api/v1/components/" + id + "/config"
	body, err := json.Marshal(apiTypes.ComponentConfigMutationRequest{Config: config})
	if err != nil {
		return apiTypes.ComponentConfigMutationResult{}, err
	}
	response, err := client.ComponentConfigSetWithBodyWithResponse(
		ctx,
		id,
		&generated.ComponentConfigSetParams{IdempotencyKey: ids.NewULID()},
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		return apiTypes.ComponentConfigMutationResult{}, generatedCallError(ctx, http.MethodPut, path, err)
	}
	if err := generatedResponseError(
		http.MethodPut, path, response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.ComponentConfigMutationResult{}, err
	}
	var result apiTypes.ComponentConfigMutationResult
	if err := decodeSingleJSON(http.MethodPut, path, bytes.NewReader(response.Body), &result); err != nil {
		return apiTypes.ComponentConfigMutationResult{}, err
	}
	return result, nil
}

func (c *Client) EnableComponent(ctx context.Context, id string) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	path := "/api/v1/components/" + id + "/enable"
	response, err := client.ComponentEnableWithResponse(
		ctx, id, &generated.ComponentEnableParams{IdempotencyKey: ids.NewULID()},
	)
	if err != nil {
		return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodPost, path, err)
	}
	if err := generatedResponseError(
		http.MethodPost, path, response.HTTPResponse, response.Body, http.StatusAccepted,
	); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	return generatedTaskAccepted(http.MethodPost, path, response.Body, response.JSON202)
}

func (c *Client) DisableComponent(ctx context.Context, id string) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	path := "/api/v1/components/" + id + "/disable"
	response, err := client.ComponentDisableWithResponse(
		ctx, id, &generated.ComponentDisableParams{IdempotencyKey: ids.NewULID()},
	)
	if err != nil {
		return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodPost, path, err)
	}
	if err := generatedResponseError(
		http.MethodPost, path, response.HTTPResponse, response.Body, http.StatusAccepted,
	); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	return generatedTaskAccepted(http.MethodPost, path, response.Body, response.JSON202)
}

func (c *Client) UpdateComponent(ctx context.Context, id string) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	path := "/api/v1/components/" + id + "/update"
	response, err := client.ComponentUpdateWithResponse(
		ctx, id, &generated.ComponentUpdateParams{IdempotencyKey: ids.NewULID()},
	)
	if err != nil {
		return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodPost, path, err)
	}
	if err := generatedResponseError(
		http.MethodPost, path, response.HTTPResponse, response.Body, http.StatusAccepted,
	); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	return generatedTaskAccepted(http.MethodPost, path, response.Body, response.JSON202)
}
