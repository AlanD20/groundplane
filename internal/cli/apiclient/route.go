package apiclient

import (
	"bytes"
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (c *Client) CreateRoute(ctx context.Context, input apiTypes.RouteCreate) (apiTypes.RouteTaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.RouteTaskAccepted{}, err
	}
	exposure := generated.RouteCreateExposure(input.Exposure)
	if !exposure.Valid() {
		return apiTypes.RouteTaskAccepted{}, errs.New(
			errs.KindValidationFailed,
			"Route exposure must be public or internal",
		)
	}
	body := generated.RouteCreateJSONRequestBody{
		EnvironmentId: input.EnvironmentID, Exposure: exposure,
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
		return apiTypes.RouteTaskAccepted{}, generatedCallError(ctx, http.MethodPost, "/api/v1/routes", err)
	}
	if err := generatedResponseError(
		http.MethodPost, "/api/v1/routes", response.HTTPResponse, response.Body, http.StatusAccepted,
	); err != nil {
		return apiTypes.RouteTaskAccepted{}, err
	}
	parsed := response.JSON202
	if parsed == nil {
		parsed = &generated.RouteTaskAccepted{}
		if err := decodeSingleJSON(
			http.MethodPost, "/api/v1/routes", bytes.NewReader(response.Body), parsed,
		); err != nil {
			return apiTypes.RouteTaskAccepted{}, err
		}
	}
	route, err := routeFromGenerated(parsed.Route)
	if err != nil {
		return apiTypes.RouteTaskAccepted{}, err
	}
	return apiTypes.RouteTaskAccepted{Route: route, TaskID: parsed.TaskId}, nil
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
		projected, err := routeFromGenerated(route)
		if err != nil {
			return apiTypes.Page[apiTypes.Route]{}, err
		}
		page.Items[index] = projected
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
	return routeFromGenerated(*parsed)
}

func (c *Client) EditRoute(
	ctx context.Context,
	id string,
	input apiTypes.RouteEdit,
) (apiTypes.RouteTaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.RouteTaskAccepted{}, err
	}
	path := "/api/v1/routes/" + id
	exposure := generated.RouteEditExposure(input.Exposure)
	if !exposure.Valid() {
		return apiTypes.RouteTaskAccepted{}, errs.New(
			errs.KindValidationFailed,
			"Route exposure must be public or internal",
		)
	}
	response, err := client.RouteEditWithResponse(
		ctx,
		id,
		&generated.RouteEditParams{IdempotencyKey: ids.NewULID()},
		generated.RouteEditJSONRequestBody{Exposure: exposure},
	)
	if err != nil {
		return apiTypes.RouteTaskAccepted{}, generatedCallError(ctx, http.MethodPatch, path, err)
	}
	if err := generatedResponseError(
		http.MethodPatch, path, response.HTTPResponse, response.Body, http.StatusAccepted,
	); err != nil {
		return apiTypes.RouteTaskAccepted{}, err
	}
	parsed := response.JSON202
	if parsed == nil {
		parsed = &generated.RouteTaskAccepted{}
		if err := decodeSingleJSON(http.MethodPatch, path, bytes.NewReader(response.Body), parsed); err != nil {
			return apiTypes.RouteTaskAccepted{}, err
		}
	}
	route, err := routeFromGenerated(parsed.Route)
	if err != nil {
		return apiTypes.RouteTaskAccepted{}, err
	}
	return apiTypes.RouteTaskAccepted{Route: route, TaskID: parsed.TaskId}, nil
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

func routeFromGenerated(route generated.Route) (apiTypes.Route, error) {
	if !route.Exposure.Valid() || !route.Status.Valid() || route.TargetPort < 1 || route.TargetPort > 65535 {
		return apiTypes.Route{}, errs.New(errs.KindInternal, "Controller returned an invalid Route")
	}
	host := ""
	if route.Host != nil {
		host = *route.Host
	}
	return apiTypes.Route{
		ID: route.Id, EnvironmentID: route.EnvironmentId, Host: host,
		Path: route.Path, Exposure: string(route.Exposure),
		TargetServiceID: route.TargetServiceId, TargetPort: uint16(route.TargetPort),
		Status: string(route.Status),
	}, nil
}
