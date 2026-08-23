package apiclient

import (
	"bytes"
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func (c *Client) CreateRoute(ctx context.Context, input apiTypes.RouteCreate) (apiTypes.Route, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Route{}, err
	}
	body := generated.RouteCreateJSONRequestBody{
		EnvironmentId: input.EnvironmentID, Exposure: input.Exposure,
		TargetServiceId: input.TargetServiceID, TargetPort: int32(input.TargetPort),
	}
	if input.Host != "" {
		body.Host = &input.Host
	}
	if input.Path != "" {
		body.Path = &input.Path
	}
	response, err := client.RouteCreateWithResponse(
		ctx, &generated.RouteCreateParams{IdempotencyKey: ids.NewULID()}, body,
	)
	if err != nil {
		return apiTypes.Route{}, generatedCallError(ctx, http.MethodPost, "/api/v1/routes", err)
	}
	if err := generatedResponseError(
		http.MethodPost, "/api/v1/routes", response.HTTPResponse, response.Body, http.StatusCreated,
	); err != nil {
		return apiTypes.Route{}, err
	}
	parsed := response.JSON201
	if parsed == nil {
		parsed = &generated.Route{}
		if err := decodeSingleJSON(
			http.MethodPost, "/api/v1/routes", bytes.NewReader(response.Body), parsed,
		); err != nil {
			return apiTypes.Route{}, err
		}
	}
	return routeFromGenerated(*parsed), nil
}

func (c *Client) ListRoutes(
	ctx context.Context,
	environmentID string,
	limit int,
	cursor string,
) (apiTypes.Page[apiTypes.Route], error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Page[apiTypes.Route]{}, err
	}
	params := &generated.RouteListParams{Environment: environmentID}
	if limit != 0 {
		value := int64(limit)
		params.Limit = &value
	}
	if cursor != "" {
		params.Cursor = &cursor
	}
	response, err := client.RouteListWithResponse(ctx, params)
	if err != nil {
		return apiTypes.Page[apiTypes.Route]{}, generatedCallError(ctx, http.MethodGet, "/api/v1/routes", err)
	}
	if err := generatedResponseError(
		http.MethodGet, "/api/v1/routes", response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.Page[apiTypes.Route]{}, err
	}
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.PageRoute{}
		if err := decodeSingleJSON(
			http.MethodGet, "/api/v1/routes", bytes.NewReader(response.Body), parsed,
		); err != nil {
			return apiTypes.Page[apiTypes.Route]{}, err
		}
	}
	items := []generated.Route(nil)
	if parsed.Items != nil {
		items = *parsed.Items
	}
	page := apiTypes.Page[apiTypes.Route]{Items: make([]apiTypes.Route, len(items))}
	if parsed.NextCursor != nil {
		page.NextCursor = *parsed.NextCursor
	}
	for index, route := range items {
		page.Items[index] = routeFromGenerated(route)
	}
	return page, nil
}

func (c *Client) GetRoute(ctx context.Context, id string) (apiTypes.Route, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Route{}, err
	}
	path := "/api/v1/routes/" + id
	response, err := client.RouteShowWithResponse(ctx, id)
	if err != nil {
		return apiTypes.Route{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(
		http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.Route{}, err
	}
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.Route{}
		if err := decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), parsed); err != nil {
			return apiTypes.Route{}, err
		}
	}
	return routeFromGenerated(*parsed), nil
}

func (c *Client) EditRoute(
	ctx context.Context,
	id string,
	input apiTypes.RouteEdit,
) (apiTypes.Route, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Route{}, err
	}
	path := "/api/v1/routes/" + id
	response, err := client.RouteEditWithResponse(
		ctx,
		id,
		&generated.RouteEditParams{IdempotencyKey: ids.NewULID()},
		generated.RouteEditJSONRequestBody{Exposure: input.Exposure},
	)
	if err != nil {
		return apiTypes.Route{}, generatedCallError(ctx, http.MethodPatch, path, err)
	}
	if err := generatedResponseError(
		http.MethodPatch, path, response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.Route{}, err
	}
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.Route{}
		if err := decodeSingleJSON(http.MethodPatch, path, bytes.NewReader(response.Body), parsed); err != nil {
			return apiTypes.Route{}, err
		}
	}
	return routeFromGenerated(*parsed), nil
}

func (c *Client) RemoveRoute(ctx context.Context, id string) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	path := "/api/v1/routes/" + id
	response, err := client.RouteRemoveWithResponse(
		ctx,
		id,
		&generated.RouteRemoveParams{IdempotencyKey: ids.NewULID()},
	)
	if err != nil {
		return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodDelete, path, err)
	}
	if err := generatedResponseError(
		http.MethodDelete, path, response.HTTPResponse, response.Body, http.StatusAccepted,
	); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	return generatedTaskAccepted(http.MethodDelete, path, response.Body, response.JSON202)
}

func routeFromGenerated(route generated.Route) apiTypes.Route {
	host := ""
	if route.Host != nil {
		host = *route.Host
	}
	return apiTypes.Route{
		ID: route.Id, EnvironmentID: route.EnvironmentId, Host: host,
		Path: route.Path, Exposure: route.Exposure,
		TargetServiceID: route.TargetServiceId, TargetPort: uint16(route.TargetPort),
	}
}
