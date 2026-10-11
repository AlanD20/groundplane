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

func (c *Client) CheckRouteConnections(
	ctx context.Context,
	routeID, connectorID, path string,
) (apiTypes.TaskAccepted, error) {
	route, err := c.GetRoute(ctx, routeID)
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	if route.Revision <= 0 {
		return apiTypes.TaskAccepted{}, errs.New(
			errs.KindInternal,
			"Controller returned a Route without its configuration revision",
		)
	}
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	body := generated.RouteCheckJSONRequestBody{RouteId: routeID, ConnectorId: connectorID, Revision: route.Revision}
	if path != "" {
		body.Path = &path
	}
	response, err := client.RouteCheckWithResponse(
		ctx,
		&generated.RouteCheckParams{Environment: route.EnvironmentID, IdempotencyKey: ids.NewULID()},
		body,
	)
	if err != nil {
		return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodPost, "/api/v1/routes/checks", err)
	}
	if err := generatedResponseError(http.MethodPost, "/api/v1/routes/checks", response.HTTPResponse, response.Body, http.StatusAccepted); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	return generatedTaskAccepted(http.MethodPost, "/api/v1/routes/checks", response.Body, response.JSON202)
}

func (c *Client) ShowRouteCheck(
	ctx context.Context,
	environmentID, taskID, routeID string,
) (apiTypes.EdgeCheckResult, error) {
	if (taskID == "") == (routeID == "") {
		return apiTypes.EdgeCheckResult{}, errs.New(
			errs.KindValidationFailed,
			"check lookup requires one Task or Route",
		)
	}
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.EdgeCheckResult{}, err
	}
	params := &generated.RouteCheckShowParams{Environment: environmentID}
	if taskID != "" {
		params.Task = &taskID
	} else {
		params.Route = &routeID
	}
	response, err := client.RouteCheckShowWithResponse(ctx, params)
	if err != nil {
		return apiTypes.EdgeCheckResult{}, generatedCallError(ctx, http.MethodGet, "/api/v1/routes/checks", err)
	}
	if err := generatedResponseError(http.MethodGet, "/api/v1/routes/checks", response.HTTPResponse, response.Body, http.StatusOK); err != nil {
		return apiTypes.EdgeCheckResult{}, err
	}
	var result apiTypes.EdgeCheckResult
	err = decodeSingleJSON(http.MethodGet, "/api/v1/routes/checks", bytes.NewReader(response.Body), &result)
	return result, err
}
