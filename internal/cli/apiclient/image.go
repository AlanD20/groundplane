package apiclient

import (
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (c *Client) FetchImage(ctx context.Context, image, key string) (apiTypes.ImageFetchAccepted, error) {
	if key == "" {
		key = ids.NewULID()
	}
	if !idempotencyKeyPattern.MatchString(key) {
		return apiTypes.ImageFetchAccepted{}, errs.New(errs.KindValidationFailed, "invalid image fetch idempotency key")
	}
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.ImageFetchAccepted{}, err
	}
	const path = "/api/v1/images/fetch"
	response, err := client.ImageFetchWithResponse(ctx, &generated.ImageFetchParams{IdempotencyKey: key},
		generated.ImageFetchJSONRequestBody{Image: image})
	if err != nil {
		return apiTypes.ImageFetchAccepted{}, generatedCallError(ctx, http.MethodPost, path, err)
	}
	if err := generatedResponseError(http.MethodPost, path, response.HTTPResponse, response.Body, http.StatusAccepted); err != nil {
		return apiTypes.ImageFetchAccepted{}, err
	}
	parsed := response.JSON202
	if parsed == nil || ids.Validate(ids.KindTask, parsed.TaskId) != nil || parsed.Image == "" ||
		parsed.ConfigDigest == "" {
		return apiTypes.ImageFetchAccepted{}, errs.New(errs.KindInternal, "image fetch response is incomplete")
	}
	return apiTypes.ImageFetchAccepted{
		TaskID:       parsed.TaskId,
		Image:        parsed.Image,
		ConfigDigest: parsed.ConfigDigest,
	}, nil
}

func (c *Client) ListImages(ctx context.Context) (apiTypes.ImageList, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.ImageList{}, err
	}
	const path = "/api/v1/images"
	response, err := client.ImageListWithResponse(ctx)
	if err != nil {
		return apiTypes.ImageList{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK); err != nil {
		return apiTypes.ImageList{}, err
	}
	if response.JSON200 == nil {
		return apiTypes.ImageList{}, errs.New(errs.KindInternal, "image inventory response is missing")
	}
	result := apiTypes.ImageList{
		ObservedAt: response.JSON200.ObservedAt,
		Images:     make([]apiTypes.HostImage, 0, len(response.JSON200.Images)),
	}
	for _, image := range response.JSON200.Images {
		result.Images = append(
			result.Images,
			apiTypes.HostImage{
				ID:             image.Id,
				Tags:           image.Tags,
				Digests:        image.Digests,
				SizeBytes:      image.SizeBytes,
				CreatedAt:      image.CreatedAt,
				Containers:     int(image.Containers),
				RemovalBlocked: image.RemovalBlocked,
			},
		)
	}
	return result, nil
}

func (c *Client) RemoveImage(ctx context.Context, id, key string) (apiTypes.TaskAccepted, error) {
	if key == "" {
		key = ids.NewULID()
	}
	if !idempotencyKeyPattern.MatchString(key) {
		return apiTypes.TaskAccepted{}, errs.New(errs.KindValidationFailed, "invalid image removal idempotency key")
	}
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	const path = "/api/v1/images/{id}"
	response, err := client.ImageRemoveWithResponse(ctx, id, &generated.ImageRemoveParams{IdempotencyKey: key})
	if err != nil {
		return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodDelete, path, err)
	}
	if err := generatedResponseError(http.MethodDelete, path, response.HTTPResponse, response.Body, http.StatusAccepted); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	if response.JSON202 == nil || ids.Validate(ids.KindTask, response.JSON202.TaskId) != nil {
		return apiTypes.TaskAccepted{}, errs.New(errs.KindInternal, "image removal response is missing its Task")
	}
	return apiTypes.TaskAccepted{TaskID: response.JSON202.TaskId}, nil
}
