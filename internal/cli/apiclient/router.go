package apiclient

import (
	"bytes"
	"context"
	"net/http"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func (c *Client) ShowRouter(ctx context.Context, environmentID string) (apiTypes.Router, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Router{}, err
	}
	path := "/api/v1/environments/" + environmentID + "/router"
	response, err := client.RouterShowWithResponse(ctx, environmentID)
	if err != nil {
		return apiTypes.Router{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(
		http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.Router{}, err
	}
	var router apiTypes.Router
	if err := decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), &router); err != nil {
		return apiTypes.Router{}, err
	}
	return router, nil
}
