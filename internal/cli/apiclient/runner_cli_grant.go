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

func (c *Client) ShowRunnerCLIGrant(ctx context.Context, runnerID string) (apiTypes.RunnerCLIGrantStatus, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.RunnerCLIGrantStatus{}, err
	}
	path := runnerCLIGrantPath(runnerID)
	response, err := client.RunnerCliGrantShowWithResponse(ctx, runnerID)
	if err != nil {
		return apiTypes.RunnerCLIGrantStatus{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	return decodeRunnerCLIGrantStatus(http.MethodGet, path, response.HTTPResponse, response.Body)
}

func (c *Client) SetRunnerCLIGrant(
	ctx context.Context, runnerID string, revision int64, input apiTypes.RunnerCLIGrantWrite, key string,
) (apiTypes.RunnerCLIGrantStatus, error) {
	if revision < 0 {
		return apiTypes.RunnerCLIGrantStatus{}, errs.New(
			errs.KindValidationFailed,
			"Runner CLI grant revision must not be negative",
		)
	}
	key, err := runnerCLIGrantKey(key)
	if err != nil {
		return apiTypes.RunnerCLIGrantStatus{}, err
	}
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.RunnerCLIGrantStatus{}, err
	}
	path := runnerCLIGrantPath(runnerID)
	response, err := client.RunnerCliGrantSetWithResponse(ctx, runnerID, &generated.RunnerCliGrantSetParams{
		IfMatch:        strconv.Quote(strconv.FormatInt(revision, 10)),
		IdempotencyKey: key,
	}, runnerCLIGrantBody(input))
	if err != nil {
		return apiTypes.RunnerCLIGrantStatus{}, generatedCallError(ctx, http.MethodPut, path, err)
	}
	return decodeRunnerCLIGrantStatus(http.MethodPut, path, response.HTTPResponse, response.Body)
}

func (c *Client) RevokeRunnerCLIGrant(ctx context.Context, runnerID string, revision int64, key string) error {
	if revision <= 0 {
		return errs.New(errs.KindValidationFailed, "revoking a Runner CLI grant requires a positive revision")
	}
	key, err := runnerCLIGrantKey(key)
	if err != nil {
		return err
	}
	client, err := c.generatedHumanClient()
	if err != nil {
		return err
	}
	path := runnerCLIGrantPath(runnerID)
	response, err := client.RunnerCliGrantRevokeWithResponse(ctx, runnerID, &generated.RunnerCliGrantRevokeParams{
		IfMatch:        strconv.Quote(strconv.FormatInt(revision, 10)),
		IdempotencyKey: key,
	})
	if err != nil {
		return generatedCallError(ctx, http.MethodDelete, path, err)
	}
	return generatedResponseError(http.MethodDelete, path, response.HTTPResponse, response.Body, http.StatusNoContent)
}

func runnerCLIGrantKey(key string) (string, error) {
	if key == "" {
		key = ids.NewULID()
	}
	if !idempotencyKeyPattern.MatchString(key) {
		return "", errs.New(errs.KindValidationFailed, "invalid Runner CLI grant idempotency key")
	}
	return key, nil
}

func runnerCLIGrantPath(runnerID string) string {
	return "/api/v1/runners/" + runnerID + "/cli-grant"
}

func runnerCLIGrantBody(input apiTypes.RunnerCLIGrantWrite) generated.RunnerCliGrantSetJSONRequestBody {
	actions := make([]generated.RunnerCLIGrantAction, 0, len(input.Actions))
	for _, action := range input.Actions {
		actions = append(actions, generated.RunnerCLIGrantAction{
			Operation: generated.RunnerCLIGrantActionOperation(action.Operation),
			TargetId:  action.TargetID,
			Image:     optionalServiceString(action.Image),
			TaskIds:   optionalServiceStrings(action.TaskIDs),
		})
	}
	return generated.RunnerCliGrantSetJSONRequestBody{
		Scope: generated.RunnerCLIGrantScope{
			TenantId: input.Scope.TenantID, ProjectId: input.Scope.ProjectID, EnvironmentId: input.Scope.EnvironmentID,
		},
		Actions:         &actions,
		LifetimeSeconds: int64(input.LifetimeSeconds),
	}
}

func decodeRunnerCLIGrantStatus(
	method, path string,
	response *http.Response,
	body []byte,
) (apiTypes.RunnerCLIGrantStatus, error) {
	if err := generatedResponseError(method, path, response, body, http.StatusOK); err != nil {
		return apiTypes.RunnerCLIGrantStatus{}, err
	}
	var status apiTypes.RunnerCLIGrantStatus
	err := decodeSingleJSON(method, path, bytes.NewReader(body), &status)
	return status, err
}
