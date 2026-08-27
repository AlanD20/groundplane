package apiclient

import (
	"bytes"
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func (c *Client) ListBackingServices(
	ctx context.Context,
	limit int,
	cursor string,
) (apiTypes.Page[apiTypes.BackingService], error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Page[apiTypes.BackingService]{}, err
	}
	params := &generated.BackingServiceListParams{}
	if limit != 0 {
		value := int64(limit)
		params.Limit = &value
	}
	if cursor != "" {
		params.Cursor = &cursor
	}
	response, err := client.BackingServiceListWithResponse(ctx, params)
	if err != nil {
		return apiTypes.Page[apiTypes.BackingService]{}, generatedCallError(
			ctx, http.MethodGet, "/api/v1/backing-services", err,
		)
	}
	if err := generatedResponseError(
		http.MethodGet, "/api/v1/backing-services", response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.Page[apiTypes.BackingService]{}, err
	}
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.PageBackingService{}
		if err := decodeSingleJSON(
			http.MethodGet, "/api/v1/backing-services", bytes.NewReader(response.Body), parsed,
		); err != nil {
			return apiTypes.Page[apiTypes.BackingService]{}, err
		}
	}
	items := []generated.BackingService(nil)
	if parsed.Items != nil {
		items = *parsed.Items
	}
	page := apiTypes.Page[apiTypes.BackingService]{Items: make([]apiTypes.BackingService, len(items))}
	if parsed.NextCursor != nil {
		page.NextCursor = *parsed.NextCursor
	}
	for index, item := range items {
		page.Items[index] = backingServiceFromGenerated(item)
	}
	return page, nil
}

func (c *Client) ShowBackingService(ctx context.Context, projectID string) (apiTypes.BackingService, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.BackingService{}, err
	}
	path := "/api/v1/backing-services/" + projectID
	response, err := client.BackingServiceShowWithResponse(ctx, projectID)
	if err != nil {
		return apiTypes.BackingService{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(
		http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.BackingService{}, err
	}
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.BackingService{}
		if err := decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), parsed); err != nil {
			return apiTypes.BackingService{}, err
		}
	}
	return backingServiceFromGenerated(*parsed), nil
}

func (c *Client) StartBackingService(ctx context.Context, projectID string) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	path := "/api/v1/backing-services/" + projectID + "/start"
	response, err := client.BackingServiceStartWithResponse(
		ctx, projectID, &generated.BackingServiceStartParams{IdempotencyKey: ids.NewULID()},
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

func (c *Client) StopBackingService(ctx context.Context, projectID string) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	path := "/api/v1/backing-services/" + projectID + "/stop"
	response, err := client.BackingServiceStopWithResponse(
		ctx, projectID, &generated.BackingServiceStopParams{IdempotencyKey: ids.NewULID()},
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

func (c *Client) DestroyBackingService(ctx context.Context, projectID string) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	path := "/api/v1/backing-services/" + projectID + "/destroy"
	response, err := client.BackingServiceDestroyWithResponse(
		ctx, projectID, &generated.BackingServiceDestroyParams{IdempotencyKey: ids.NewULID()},
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

func backingServiceFromGenerated(item generated.BackingService) apiTypes.BackingService {
	return apiTypes.BackingService{
		ProjectID: item.ProjectId, EnvironmentID: item.EnvironmentId, ServiceID: item.ServiceId,
	}
}
