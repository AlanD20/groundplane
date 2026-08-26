package apiclient

import (
	"context"
	"net/http"
	"strconv"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func (c *Client) ListVolumes(
	ctx context.Context,
	environmentID string,
	limit int,
	cursor string,
) (apiTypes.Page[apiTypes.Volume], error) {
	query := map[string]string{"environment": environmentID}
	if limit != 0 {
		query["limit"] = strconv.Itoa(limit)
	}
	if cursor != "" {
		query["cursor"] = cursor
	}
	var page apiTypes.Page[apiTypes.Volume]
	request := c.NewRequest(http.MethodGet, "/api/v1/volumes", query, nil, http.StatusOK)
	if err := c.Do(ctx, request, &page); err != nil {
		return apiTypes.Page[apiTypes.Volume]{}, err
	}
	return page, nil
}

func (c *Client) GetVolume(ctx context.Context, id string) (apiTypes.Volume, error) {
	var volume apiTypes.Volume
	path := "/api/v1/volumes/" + id
	request := c.NewRequest(http.MethodGet, path, nil, nil, http.StatusOK)
	if err := c.Do(ctx, request, &volume); err != nil {
		return apiTypes.Volume{}, err
	}
	return volume, nil
}

func (c *Client) CreateVolume(ctx context.Context, input apiTypes.VolumeCreate) (apiTypes.VolumeMutationResponse, error) {
	var response apiTypes.VolumeMutationResponse
	request := c.NewRequest(http.MethodPost, "/api/v1/volumes", nil, input, http.StatusCreated)
	if err := c.Do(ctx, request, &response); err != nil {
		return apiTypes.VolumeMutationResponse{}, err
	}
	return response, nil
}

func (c *Client) EditVolume(
	ctx context.Context,
	id string,
	input apiTypes.VolumeEdit,
) (apiTypes.VolumeMutationResponse, error) {
	var response apiTypes.VolumeMutationResponse
	path := "/api/v1/volumes/" + id
	request := c.NewRequest(http.MethodPatch, path, nil, input, http.StatusOK)
	if err := c.Do(ctx, request, &response); err != nil {
		return apiTypes.VolumeMutationResponse{}, err
	}
	return response, nil
}

func (c *Client) GetVolumeDeletionImpact(
	ctx context.Context,
	id string,
	cursor string,
	limit int,
) (apiTypes.VolumeDeletionImpactPage, error) {
	query := map[string]string{"limit": strconv.Itoa(limit)}
	if cursor != "" {
		query["cursor"] = cursor
	}
	var page apiTypes.VolumeDeletionImpactPage
	path := "/api/v1/volumes/" + id + "/deletion-impact"
	request := c.NewRequest(http.MethodGet, path, query, nil, http.StatusOK)
	if err := c.Do(ctx, request, &page); err != nil {
		return apiTypes.VolumeDeletionImpactPage{}, err
	}
	return page, nil
}

func (c *Client) RemoveVolume(
	ctx context.Context,
	id string,
	impactToken string,
	confirmKey string,
) (apiTypes.TaskAccepted, error) {
	query := map[string]string{"impact_token": impactToken, "confirm_key": confirmKey}
	var response apiTypes.TaskAccepted
	path := "/api/v1/volumes/" + id
	request := c.NewRequest(http.MethodDelete, path, query, nil, http.StatusAccepted)
	if err := c.Do(ctx, request, &response); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	return response, nil
}
