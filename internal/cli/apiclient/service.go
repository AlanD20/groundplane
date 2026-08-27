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

func (c *Client) CreateService(ctx context.Context, input apiTypes.ServiceCreate) (apiTypes.Service, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Service{}, err
	}
	body, err := generatedServiceBody[generated.ServiceCreateJSONRequestBody](input)
	if err != nil {
		return apiTypes.Service{}, err
	}
	response, err := client.ServiceCreateWithResponse(
		ctx,
		&generated.ServiceCreateParams{IdempotencyKey: ids.NewULID()},
		body,
	)
	if err != nil {
		return apiTypes.Service{}, generatedCallError(ctx, http.MethodPost, "/api/v1/services", err)
	}
	if err := generatedResponseError(
		http.MethodPost,
		"/api/v1/services",
		response.HTTPResponse,
		response.Body,
		http.StatusCreated,
	); err != nil {
		return apiTypes.Service{}, err
	}
	return decodeServiceResponse(http.MethodPost, "/api/v1/services", response.Body)
}

func (c *Client) ListServices(
	ctx context.Context,
	environmentID string,
	limit int,
	cursor string,
) (apiTypes.Page[apiTypes.Service], error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Page[apiTypes.Service]{}, err
	}
	params := &generated.ServiceListParams{Environment: environmentID}
	if limit != 0 {
		value := int64(limit)
		params.Limit = &value
	}
	if cursor != "" {
		params.Cursor = &cursor
	}
	response, err := client.ServiceListWithResponse(ctx, params)
	if err != nil {
		return apiTypes.Page[apiTypes.Service]{}, generatedCallError(ctx, http.MethodGet, "/api/v1/services", err)
	}
	if err := generatedResponseError(
		http.MethodGet, "/api/v1/services", response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.Page[apiTypes.Service]{}, err
	}
	page := apiTypes.Page[apiTypes.Service]{}
	if err := decodeSingleJSON(http.MethodGet, "/api/v1/services", bytes.NewReader(response.Body), &page); err != nil {
		return apiTypes.Page[apiTypes.Service]{}, err
	}
	return page, nil
}

func (c *Client) GetService(ctx context.Context, id string) (apiTypes.Service, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Service{}, err
	}
	path := "/api/v1/services/" + id
	response, err := client.ServiceShowWithResponse(ctx, id)
	if err != nil {
		return apiTypes.Service{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(
		http.MethodGet,
		path,
		response.HTTPResponse,
		response.Body,
		http.StatusOK,
	); err != nil {
		return apiTypes.Service{}, err
	}
	return decodeServiceResponse(http.MethodGet, path, response.Body)
}

func (c *Client) EditService(ctx context.Context, id string, input apiTypes.ServiceEdit) (apiTypes.Service, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Service{}, err
	}
	body, err := generatedServiceBody[generated.ServiceEditJSONRequestBody](input)
	if err != nil {
		return apiTypes.Service{}, err
	}
	path := "/api/v1/services/" + id
	response, err := client.ServiceEditWithResponse(
		ctx,
		id,
		&generated.ServiceEditParams{IdempotencyKey: ids.NewULID()},
		body,
	)
	if err != nil {
		return apiTypes.Service{}, generatedCallError(ctx, http.MethodPatch, path, err)
	}
	if err := generatedResponseError(
		http.MethodPatch,
		path,
		response.HTTPResponse,
		response.Body,
		http.StatusOK,
	); err != nil {
		return apiTypes.Service{}, err
	}
	return decodeServiceResponse(http.MethodPatch, path, response.Body)
}

func (c *Client) DeployService(
	ctx context.Context,
	id string,
	input apiTypes.DeployRequest,
) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	body, err := generatedServiceBody[generated.ServiceDeployJSONRequestBody](input)
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	path := "/api/v1/services/" + id + "/deploy"
	response, err := client.ServiceDeployWithResponse(
		ctx, id, &generated.ServiceDeployParams{IdempotencyKey: ids.NewULID()}, body,
	)
	if err != nil {
		return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodPost, path, err)
	}
	if err := generatedResponseError(
		http.MethodPost, path, response.HTTPResponse, response.Body, http.StatusAccepted,
	); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	return generatedReleaseTaskAccepted(http.MethodPost, path, response.Body, response.JSON202)
}

func (c *Client) RollbackService(
	ctx context.Context,
	id string,
	input apiTypes.RollbackRequest,
) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	body, err := generatedServiceBody[generated.ServiceRollbackJSONRequestBody](input)
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	path := "/api/v1/services/" + id + "/rollback"
	response, err := client.ServiceRollbackWithResponse(
		ctx, id, &generated.ServiceRollbackParams{IdempotencyKey: ids.NewULID()}, body,
	)
	if err != nil {
		return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodPost, path, err)
	}
	if err := generatedResponseError(
		http.MethodPost, path, response.HTTPResponse, response.Body, http.StatusAccepted,
	); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	return generatedReleaseTaskAccepted(http.MethodPost, path, response.Body, response.JSON202)
}

func (c *Client) StartService(ctx context.Context, id string) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	path := "/api/v1/services/" + id + "/start"
	response, err := client.ServiceStartWithResponse(
		ctx, id, &generated.ServiceStartParams{IdempotencyKey: ids.NewULID()},
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

func (c *Client) StopService(ctx context.Context, id string) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	path := "/api/v1/services/" + id + "/stop"
	response, err := client.ServiceStopWithResponse(
		ctx, id, &generated.ServiceStopParams{IdempotencyKey: ids.NewULID()},
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

func (c *Client) DestroyService(ctx context.Context, id string) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	path := "/api/v1/services/" + id + "/destroy"
	response, err := client.ServiceDestroyWithResponse(
		ctx, id, &generated.ServiceDestroyParams{IdempotencyKey: ids.NewULID()},
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

func generatedServiceBody[Body any](input any) (Body, error) {
	var body Body
	encoded, err := json.Marshal(input)
	if err != nil {
		return body, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(encoded)
	if err := json.Unmarshal(encoded, &body); err != nil {
		return body, errs.Wrap(errs.KindInternal, err)
	}
	return body, nil
}

func decodeServiceResponse(method string, path string, body []byte) (apiTypes.Service, error) {
	result := apiTypes.Service{}
	if err := decodeSingleJSON(method, path, bytes.NewReader(body), &result); err != nil {
		return apiTypes.Service{}, err
	}
	return result, nil
}
