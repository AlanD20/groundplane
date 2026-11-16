package apiclient

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
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
	response, err := client.ComponentList(ctx, params)
	if err != nil {
		return apiTypes.Page[apiTypes.Component]{}, generatedCallError(
			ctx, http.MethodGet, "/api/v1/components", err,
		)
	}
	if response == nil {
		return apiTypes.Page[apiTypes.Component]{}, generatedResponseError(
			http.MethodGet, "/api/v1/components", nil, nil, http.StatusOK,
		)
	}
	defer response.Body.Close()
	body, err := readBoundedRawResponseInto(
		response.Body,
		make([]byte, maximumRawResponseByteSize+1),
		http.MethodGet,
		"/api/v1/components",
	)
	if err != nil {
		return apiTypes.Page[apiTypes.Component]{}, err
	}
	if err := generatedResponseError(
		http.MethodGet, "/api/v1/components", response, body, http.StatusOK,
	); err != nil {
		return apiTypes.Page[apiTypes.Component]{}, err
	}
	var page apiTypes.Page[apiTypes.Component]
	if err := decodeSingleJSON(
		http.MethodGet, "/api/v1/components", bytes.NewReader(body), &page,
	); err != nil {
		return apiTypes.Page[apiTypes.Component]{}, err
	}
	for _, component := range page.Items {
		if component.Software != nil {
			if err := component.Software.Validate(); err != nil {
				return apiTypes.Page[apiTypes.Component]{}, errs.Wrap(errs.KindInternal, err)
			}
		}
	}
	return page, nil
}

func (c *Client) ShowComponent(ctx context.Context, id string) (apiTypes.Component, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Component{}, err
	}
	path := "/api/v1/components/" + id
	response, err := client.ComponentShow(ctx, id)
	if err != nil {
		return apiTypes.Component{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if response == nil {
		return apiTypes.Component{}, generatedResponseError(http.MethodGet, path, nil, nil, http.StatusOK)
	}
	defer response.Body.Close()
	body, err := readBoundedRawResponseInto(
		response.Body,
		make([]byte, maximumRawResponseByteSize+1),
		http.MethodGet,
		path,
	)
	if err != nil {
		return apiTypes.Component{}, err
	}
	if err := generatedResponseError(
		http.MethodGet, path, response, body, http.StatusOK,
	); err != nil {
		return apiTypes.Component{}, err
	}
	var component apiTypes.Component
	if err := decodeSingleJSON(http.MethodGet, path, bytes.NewReader(body), &component); err != nil {
		return apiTypes.Component{}, err
	}
	if component.Software != nil {
		if err := component.Software.Validate(); err != nil {
			return apiTypes.Component{}, errs.Wrap(errs.KindInternal, err)
		}
	}
	return component, nil
}

func (c *Client) ShowComponentConfig(ctx context.Context, id string) (apiTypes.ComponentConfigResponse, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.ComponentConfigResponse{}, err
	}
	path := "/api/v1/components/" + id + "/config"
	response, err := client.ComponentConfigShowWithResponse(ctx, id)
	if err != nil {
		return apiTypes.ComponentConfigResponse{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(
		http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.ComponentConfigResponse{}, err
	}
	var envelope apiTypes.ComponentConfigResponse
	if err := decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), &envelope); err != nil {
		return apiTypes.ComponentConfigResponse{}, err
	}
	return envelope, nil
}

type ComponentConfigSaveResult struct {
	Saved *apiTypes.ComponentConfigMutationResult
	Task  *apiTypes.TaskAccepted
}

func (c *Client) SetComponentConfig(
	ctx context.Context,
	id string,
	config apiTypes.ComponentConfigMutationInput,
	after []string,
) (ComponentConfigSaveResult, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return ComponentConfigSaveResult{}, err
	}
	path := "/api/v1/components/" + id + "/config"
	body, err := json.Marshal(apiTypes.ComponentConfigMutationRequest{Config: config, After: after})
	if err != nil {
		return ComponentConfigSaveResult{}, err
	}
	response, err := client.ComponentConfigSetWithBodyWithResponse(
		ctx,
		id,
		&generated.ComponentConfigSetParams{IdempotencyKey: ids.NewULID()},
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		return ComponentConfigSaveResult{}, generatedCallError(ctx, http.MethodPut, path, err)
	}
	expectedStatus := http.StatusOK
	if response.HTTPResponse != nil && response.HTTPResponse.StatusCode == http.StatusAccepted {
		expectedStatus = http.StatusAccepted
	}
	if err := generatedResponseError(
		http.MethodPut, path, response.HTTPResponse, response.Body, expectedStatus,
	); err != nil {
		return ComponentConfigSaveResult{}, err
	}
	if response.HTTPResponse.StatusCode == http.StatusAccepted {
		var task apiTypes.TaskAccepted
		if err := decodeSingleJSON(http.MethodPut, path, bytes.NewReader(response.Body), &task); err != nil {
			return ComponentConfigSaveResult{}, err
		}
		return ComponentConfigSaveResult{Task: &task}, nil
	}
	var saved apiTypes.ComponentConfigMutationResult
	if err := decodeSingleJSON(http.MethodPut, path, bytes.NewReader(response.Body), &saved); err != nil {
		return ComponentConfigSaveResult{}, err
	}
	return ComponentConfigSaveResult{Saved: &saved}, nil
}

func (c *Client) EnableComponent(
	ctx context.Context,
	id string,
	request apiTypes.ComponentEnableRequest,
) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	path := "/api/v1/components/" + id + "/enable"
	var body []byte
	if request.Config != nil || len(request.After) > 0 {
		body, err = json.Marshal(request)
		if err != nil {
			return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodPost, path, err)
		}
	}
	response, err := client.ComponentEnableWithBodyWithResponse(
		ctx, id, &generated.ComponentEnableParams{IdempotencyKey: ids.NewULID()},
		"application/json", bytes.NewReader(body),
	)
	if err != nil {
		return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodPost, path, err)
	}
	if err := generatedResponseError(http.MethodPost, path, response.HTTPResponse, response.Body, http.StatusAccepted); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	return generatedTaskAccepted(http.MethodPost, path, response.Body, response.JSON202)
}

func (c *Client) DisableComponent(ctx context.Context, id string, after []string) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	path := "/api/v1/components/" + id + "/disable"
	response, err := client.ComponentDisableWithResponse(
		ctx, id, &generated.ComponentDisableParams{IdempotencyKey: ids.NewULID()},
		generated.ComponentDisableJSONRequestBody{After: optionalServiceStrings(after)},
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

func (c *Client) UpdateComponent(ctx context.Context, id string, after []string) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	path := "/api/v1/components/" + id + "/update"
	response, err := client.ComponentUpdateWithResponse(
		ctx, id, &generated.ComponentUpdateParams{IdempotencyKey: ids.NewULID()},
		generated.ComponentUpdateJSONRequestBody{After: optionalServiceStrings(after)},
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
