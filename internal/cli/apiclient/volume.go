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

func (c *Client) ListVolumes(
	ctx context.Context,
	environmentID string,
	limit int,
	cursor string,
) (apiTypes.Page[apiTypes.Volume], error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Page[apiTypes.Volume]{}, err
	}
	params := &generated.VolumeListParams{Environment: environmentID}
	if limit != 0 {
		value := int64(limit)
		params.Limit = &value
	}
	if cursor != "" {
		params.Cursor = &cursor
	}
	response, err := client.VolumeListWithResponse(ctx, params)
	if err != nil {
		return apiTypes.Page[apiTypes.Volume]{}, generatedCallError(ctx, http.MethodGet, "/api/v1/volumes", err)
	}
	if err := generatedResponseError(
		http.MethodGet, "/api/v1/volumes", response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.Page[apiTypes.Volume]{}, err
	}
	var page apiTypes.Page[apiTypes.Volume]
	if err := decodeSingleJSON(http.MethodGet, "/api/v1/volumes", bytes.NewReader(response.Body), &page); err != nil {
		return apiTypes.Page[apiTypes.Volume]{}, err
	}
	return page, nil
}

func (c *Client) GetVolume(ctx context.Context, id string) (apiTypes.Volume, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Volume{}, err
	}
	path := "/api/v1/volumes/" + id
	response, err := client.VolumeShowWithResponse(ctx, id)
	if err != nil {
		return apiTypes.Volume{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK); err != nil {
		return apiTypes.Volume{}, err
	}
	var volume apiTypes.Volume
	if err := decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), &volume); err != nil {
		return apiTypes.Volume{}, err
	}
	return volume, nil
}

func (c *Client) CreateVolume(ctx context.Context, input apiTypes.VolumeCreate) (apiTypes.VolumeMutationResponse, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.VolumeMutationResponse{}, err
	}
	body, err := generatedVolumeBody[generated.VolumeCreateJSONRequestBody](input)
	if err != nil {
		return apiTypes.VolumeMutationResponse{}, err
	}
	response, err := client.VolumeCreateWithResponse(
		ctx, &generated.VolumeCreateParams{IdempotencyKey: ids.NewULID()}, body,
	)
	if err != nil {
		return apiTypes.VolumeMutationResponse{}, generatedCallError(ctx, http.MethodPost, "/api/v1/volumes", err)
	}
	if err := generatedResponseError(
		http.MethodPost, "/api/v1/volumes", response.HTTPResponse, response.Body, http.StatusCreated,
	); err != nil {
		return apiTypes.VolumeMutationResponse{}, err
	}
	var result apiTypes.VolumeMutationResponse
	if err := decodeSingleJSON(http.MethodPost, "/api/v1/volumes", bytes.NewReader(response.Body), &result); err != nil {
		return apiTypes.VolumeMutationResponse{}, err
	}
	return result, nil
}

func (c *Client) EditVolume(
	ctx context.Context,
	id string,
	input apiTypes.VolumeEdit,
) (apiTypes.VolumeMutationResponse, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.VolumeMutationResponse{}, err
	}
	body, err := generatedVolumeBody[generated.VolumeEditJSONRequestBody](input)
	if err != nil {
		return apiTypes.VolumeMutationResponse{}, err
	}
	path := "/api/v1/volumes/" + id
	response, err := client.VolumeEditWithResponse(
		ctx, id, &generated.VolumeEditParams{IdempotencyKey: ids.NewULID()}, body,
	)
	if err != nil {
		return apiTypes.VolumeMutationResponse{}, generatedCallError(ctx, http.MethodPatch, path, err)
	}
	if err := generatedResponseError(http.MethodPatch, path, response.HTTPResponse, response.Body, http.StatusOK); err != nil {
		return apiTypes.VolumeMutationResponse{}, err
	}
	var result apiTypes.VolumeMutationResponse
	if err := decodeSingleJSON(http.MethodPatch, path, bytes.NewReader(response.Body), &result); err != nil {
		return apiTypes.VolumeMutationResponse{}, err
	}
	return result, nil
}

func (c *Client) GetVolumeDeletionImpact(
	ctx context.Context,
	id string,
	cursor string,
	limit int,
) (apiTypes.VolumeDeletionImpactPage, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.VolumeDeletionImpactPage{}, err
	}
	params := &generated.VolumeRemovalImpactParams{}
	if cursor != "" {
		params.Cursor = &cursor
	}
	if limit != 0 {
		value := int64(limit)
		params.Limit = &value
	}
	path := "/api/v1/volumes/" + id + "/deletion-impact"
	response, err := client.VolumeRemovalImpactWithResponse(ctx, id, params)
	if err != nil {
		return apiTypes.VolumeDeletionImpactPage{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK); err != nil {
		return apiTypes.VolumeDeletionImpactPage{}, err
	}
	var page apiTypes.VolumeDeletionImpactPage
	if err := decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), &page); err != nil {
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
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	path := "/api/v1/volumes/" + id
	response, err := client.VolumeRemoveWithResponse(ctx, id, &generated.VolumeRemoveParams{
		ImpactToken: impactToken, ConfirmKey: confirmKey, IdempotencyKey: ids.NewULID(),
	})
	if err != nil {
		return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodDelete, path, err)
	}
	if err := generatedResponseError(http.MethodDelete, path, response.HTTPResponse, response.Body, http.StatusAccepted); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	var result apiTypes.TaskAccepted
	if err := decodeSingleJSON(http.MethodDelete, path, bytes.NewReader(response.Body), &result); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	return result, nil
}

func generatedVolumeBody[Body any](input any) (Body, error) {
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
