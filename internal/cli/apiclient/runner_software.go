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

func (client *Client) UpdateRunnerSoftware(
	ctx context.Context,
	runnerID string,
	revision int64, version string,
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
	path := "/api/v1/runners/" + runnerID + "/updates"
	response, err := transport.RunnerUpdateWithResponse(
		ctx,
		runnerID,
		&generated.RunnerUpdateParams{IdempotencyKey: key},
		generated.RunnerUpdateJSONRequestBody{Revision: revision, Version: version},
	)
	if err != nil {
		return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodPost, path, err)
	}
	if err := generatedResponseError(http.MethodPost, path, response.HTTPResponse, response.Body, http.StatusAccepted); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	return generatedTaskAccepted(http.MethodPost, path, response.Body, response.JSON202)
}

func (client *Client) ShowRunnerUpdate(
	ctx context.Context,
	runnerID, taskID string,
) (apiTypes.RunnerUpdateResult, error) {
	transport, err := client.generatedHumanClient()
	if err != nil {
		return apiTypes.RunnerUpdateResult{}, err
	}
	params := &generated.RunnerUpdateShowParams{}
	if taskID != "" {
		params.Task = &taskID
	}
	path := "/api/v1/runners/" + runnerID + "/updates"
	response, err := transport.RunnerUpdateShowWithResponse(ctx, runnerID, params)
	if err != nil {
		return apiTypes.RunnerUpdateResult{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK); err != nil {
		return apiTypes.RunnerUpdateResult{}, err
	}
	var result apiTypes.RunnerUpdateResult
	err = decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), &result)
	return result, err
}

func (client *Client) ListRunnerVersions(ctx context.Context, id string) (apiTypes.RunnerVersions, error) {
	transport, err := client.generatedHumanClient()
	if err != nil {
		return apiTypes.RunnerVersions{}, err
	}
	response, err := transport.RunnerVersionsWithResponse(ctx, id)
	path := "/api/v1/runners/" + id + "/versions"
	if err != nil {
		return apiTypes.RunnerVersions{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK); err != nil {
		return apiTypes.RunnerVersions{}, err
	}
	var result apiTypes.RunnerVersions
	err = decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), &result)
	return result, err
}
