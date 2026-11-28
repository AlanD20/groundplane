package apiclient

import (
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (c *Client) FetchRemoteImage(
	ctx context.Context,
	host, image, key string,
	after []string,
) (api.TaskAccepted, error) {
	if key == "" {
		key = ids.NewULID()
	}
	if !idempotencyKeyPattern.MatchString(key) {
		return api.TaskAccepted{}, errs.New(errs.KindValidationFailed, "invalid image Fetch idempotency key")
	}
	client, err := c.generatedHumanClient()
	if err != nil {
		return api.TaskAccepted{}, err
	}
	const path = "/api/v1/hosts/{id}/images/fetch"
	response, err := client.RemoteHostFetchImageWithResponse(ctx, host,
		&generated.RemoteHostFetchImageParams{IdempotencyKey: key},
		generated.RemoteHostFetchImageJSONRequestBody{Image: image, After: optionalServiceStrings(after)})
	if err != nil {
		return api.TaskAccepted{}, generatedCallError(ctx, http.MethodPost, path, err)
	}
	if err := generatedResponseError(http.MethodPost, path, response.HTTPResponse, response.Body, http.StatusAccepted); err != nil {
		return api.TaskAccepted{}, err
	}
	if response.JSON202 == nil || ids.Validate(ids.KindTask, response.JSON202.TaskId) != nil {
		return api.TaskAccepted{}, errs.New(errs.KindInternal, "Remote image Fetch receipt is incomplete")
	}
	return api.TaskAccepted{TaskID: response.JSON202.TaskId}, nil
}
