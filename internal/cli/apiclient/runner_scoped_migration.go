package apiclient

import (
	"bytes"
	"context"
	"net/http"
	"strconv"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (c *Client) ShowRunnerScopedMigration(
	ctx context.Context,
	runnerID string,
) (apiTypes.RunnerScopedMigrationStatus, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.RunnerScopedMigrationStatus{}, err
	}
	path := "/api/v1/runners/" + runnerID + "/scoped-migration"
	response, err := client.RunnerScopedMigrationShowWithResponse(ctx, runnerID)
	if err != nil {
		return apiTypes.RunnerScopedMigrationStatus{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK); err != nil {
		return apiTypes.RunnerScopedMigrationStatus{}, err
	}
	var status apiTypes.RunnerScopedMigrationStatus
	err = decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), &status)
	return status, err
}

func (c *Client) StartRunnerScopedMigration(
	ctx context.Context,
	runnerID string,
	revision int64,
	input apiTypes.RunnerScopedMigrationRequest,
	key string,
) (apiTypes.TaskAccepted, error) {
	if revision <= 0 {
		return apiTypes.TaskAccepted{}, errs.New(
			errs.KindValidationFailed,
			"scoped Runner migration requires a positive revision",
		)
	}
	if key == "" {
		key = ids.NewULID()
	}
	if !idempotencyKeyPattern.MatchString(key) {
		return apiTypes.TaskAccepted{}, errs.New(errs.KindValidationFailed, "invalid Runner migration idempotency key")
	}
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	path := "/api/v1/runners/" + runnerID + "/scoped-migration"
	response, err := client.RunnerScopedMigrationStartWithResponse(
		ctx,
		runnerID,
		&generated.RunnerScopedMigrationStartParams{
			IfMatch:        strconv.Quote(strconv.FormatInt(revision, 10)),
			IdempotencyKey: key,
		},
		generated.RunnerScopedMigrationStartJSONRequestBody{
			ConnectionId: input.ConnectionID,
			RepositoryId: input.RepositoryID,
		},
	)
	if err != nil {
		return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodPost, path, err)
	}
	if err := generatedResponseError(http.MethodPost, path, response.HTTPResponse, response.Body, http.StatusAccepted); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	accepted, err := generatedTaskAccepted(http.MethodPost, path, response.Body, response.JSON202)
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	if ids.Validate(ids.KindTask, accepted.TaskID) != nil {
		return apiTypes.TaskAccepted{}, errs.New(
			errs.KindInternal,
			"Runner migration response is missing a valid Task ID",
		)
	}
	return accepted, nil
}
