package apiclient

import (
	"bytes"
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func (c *Client) ListReleases(
	ctx context.Context,
	environmentID string,
	serviceID string,
	limit int,
	cursor string,
) (apiTypes.Page[apiTypes.ReleaseSummary], error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Page[apiTypes.ReleaseSummary]{}, err
	}
	params := &generated.ReleaseListParams{EnvironmentId: environmentID}
	if serviceID != "" {
		params.ServiceId = &serviceID
	}
	if limit != 0 {
		value := int64(limit)
		params.Limit = &value
	}
	if cursor != "" {
		params.Cursor = &cursor
	}
	response, err := client.ReleaseListWithResponse(ctx, params)
	if err != nil {
		return apiTypes.Page[apiTypes.ReleaseSummary]{}, generatedCallError(
			ctx,
			http.MethodGet,
			"/api/v1/releases",
			err,
		)
	}
	if err := generatedResponseError(
		http.MethodGet, "/api/v1/releases", response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.Page[apiTypes.ReleaseSummary]{}, err
	}
	page := apiTypes.Page[apiTypes.ReleaseSummary]{}
	if err := decodeSingleJSON(http.MethodGet, "/api/v1/releases", bytes.NewReader(response.Body), &page); err != nil {
		return apiTypes.Page[apiTypes.ReleaseSummary]{}, err
	}
	return page, nil
}

func (c *Client) GetRelease(ctx context.Context, id string) (apiTypes.ReleaseDetail, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.ReleaseDetail{}, err
	}
	path := "/api/v1/releases/" + id
	response, err := client.ReleaseShowWithResponse(ctx, id)
	if err != nil {
		return apiTypes.ReleaseDetail{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(
		http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.ReleaseDetail{}, err
	}
	release := apiTypes.ReleaseDetail{}
	if err := decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), &release); err != nil {
		return apiTypes.ReleaseDetail{}, err
	}
	return release, nil
}
