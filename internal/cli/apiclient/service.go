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
