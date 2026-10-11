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

func (c *Client) CheckTunnelConnections(
	ctx context.Context,
	connectorID, originServiceID string,
	port uint16,
	hostname, path string,
) (apiTypes.TaskAccepted, error) {
	connector, err := c.ShowComponent(ctx, connectorID)
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	if connector.Kind != "cloudflare-tunnel" || connector.Owner != "environment" || connector.EnvironmentID == "" ||
		connector.Revision <= 0 {
		return apiTypes.TaskAccepted{}, errs.New(errs.KindValidationFailed, "select an Environment's Cloudflare Tunnel")
	}
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	endpoint := "/api/v1/components/" + connectorID + "/tunnel-checks"
	response, err := client.ComponentTunnelCheckWithResponse(ctx, connectorID, &generated.ComponentTunnelCheckParams{
		Environment: connector.EnvironmentID, IdempotencyKey: ids.NewULID()}, generated.ComponentTunnelCheckJSONRequestBody{
		Revision: connector.Revision, OriginServiceId: originServiceID, OriginPort: int32(port), Hostname: hostname, Path: path})
	if err != nil {
		return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodPost, endpoint, err)
	}
	if err := generatedResponseError(http.MethodPost, endpoint, response.HTTPResponse, response.Body, http.StatusAccepted); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	return generatedTaskAccepted(http.MethodPost, endpoint, response.Body, response.JSON202)
}

func (c *Client) ShowTunnelCheck(ctx context.Context, connectorID, taskID string) (apiTypes.EdgeCheckResult, error) {
	connector, err := c.ShowComponent(ctx, connectorID)
	if err != nil {
		return apiTypes.EdgeCheckResult{}, err
	}
	if connector.EnvironmentID == "" {
		return apiTypes.EdgeCheckResult{}, errs.New(errs.KindValidationFailed, "select an Environment's Tunnel")
	}
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.EdgeCheckResult{}, err
	}
	endpoint := "/api/v1/components/" + connectorID + "/tunnel-checks"
	params := &generated.ComponentTunnelCheckShowParams{Environment: connector.EnvironmentID}
	if taskID != "" {
		params.Task = &taskID
	}
	response, err := client.ComponentTunnelCheckShowWithResponse(ctx, connectorID, params)
	if err != nil {
		return apiTypes.EdgeCheckResult{}, generatedCallError(ctx, http.MethodGet, endpoint, err)
	}
	if err := generatedResponseError(http.MethodGet, endpoint, response.HTTPResponse, response.Body, http.StatusOK); err != nil {
		return apiTypes.EdgeCheckResult{}, err
	}
	var result apiTypes.EdgeCheckResult
	err = decodeSingleJSON(http.MethodGet, endpoint, bytes.NewReader(response.Body), &result)
	return result, err
}
