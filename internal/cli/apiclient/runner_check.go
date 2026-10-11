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

func (client *Client) CheckRunner(
	ctx context.Context,
	runnerID string,
	revision int64,
	key string,
) (apiTypes.TaskAccepted, error) {
	if revision <= 0 {
		return apiTypes.TaskAccepted{}, errs.New(
			errs.KindStateConflict,
			"Runner read has no current revision; refresh before testing",
		)
	}
	if key == "" {
		key = ids.NewULID()
	}
	transport, err := client.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	path := "/api/v1/runners/" + runnerID + "/checks"
	response, err := transport.RunnerCheckWithResponse(
		ctx,
		runnerID,
		&generated.RunnerCheckParams{IdempotencyKey: key},
		generated.RunnerCheckJSONRequestBody{Revision: revision},
	)
	if err != nil {
		return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodPost, path, err)
	}
	if err := generatedResponseError(http.MethodPost, path, response.HTTPResponse, response.Body, http.StatusAccepted); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	return generatedTaskAccepted(http.MethodPost, path, response.Body, response.JSON202)
}

func (client *Client) ShowRunnerCheck(
	ctx context.Context,
	runnerID, taskID string,
) (apiTypes.RunnerCheckResult, error) {
	transport, err := client.generatedHumanClient()
	if err != nil {
		return apiTypes.RunnerCheckResult{}, err
	}
	params := &generated.RunnerCheckShowParams{}
	if taskID != "" {
		params.Task = &taskID
	}
	path := "/api/v1/runners/" + runnerID + "/checks"
	response, err := transport.RunnerCheckShowWithResponse(ctx, runnerID, params)
	if err != nil {
		return apiTypes.RunnerCheckResult{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK); err != nil {
		return apiTypes.RunnerCheckResult{}, err
	}
	var result apiTypes.RunnerCheckResult
	err = decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), &result)
	return result, err
}
