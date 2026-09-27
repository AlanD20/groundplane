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
