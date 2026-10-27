package apiclient

import (
	"bytes"
	"context"
	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"net/http"
)

func alertScope(scope string) (*string, *bool) {
	if scope == "platform" {
		yes := true
		return nil, &yes
	}
	return &scope, nil
}
func (client *Client) ShowTaskAlerts(ctx context.Context, scope string) (apiTypes.TaskAlertSettings, error) {
	wire, err := client.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAlertSettings{}, err
	}
	environment, platform := alertScope(scope)
	response, err := wire.TaskAlertShowWithResponse(
		ctx,
		&generated.TaskAlertShowParams{Environment: environment, Platform: platform},
	)
	const path = "/api/v1/task-alerts"
	if err != nil {
		return apiTypes.TaskAlertSettings{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK); err != nil {
		return apiTypes.TaskAlertSettings{}, err
	}
	var document apiTypes.TaskAlertSettings
	err = decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), &document)
	return document, err
}

func (client *Client) SetTaskAlerts(
	ctx context.Context,
	scope string,
	input apiTypes.TaskAlertSettingsWrite,
) (apiTypes.TaskAlertSettings, error) {
	wire, err := client.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAlertSettings{}, err
	}
	environment, platform := alertScope(scope)
	response, err := wire.TaskAlertSetWithResponse(
		ctx,
		&generated.TaskAlertSetParams{Environment: environment, Platform: platform, IdempotencyKey: ids.NewULID()},
		generated.TaskAlertSetJSONRequestBody{
			ExpectedRevision: input.ExpectedRevision,
			Enabled:          input.Enabled,
			WebhookUrl:       input.WebhookURL,
			ConsoleUrl:       input.ConsoleURL,
		},
	)
	const path = "/api/v1/task-alerts"
	if err != nil {
		return apiTypes.TaskAlertSettings{}, generatedCallError(ctx, http.MethodPut, path, err)
	}
	if err := generatedResponseError(http.MethodPut, path, response.HTTPResponse, response.Body, http.StatusOK); err != nil {
		return apiTypes.TaskAlertSettings{}, err
	}
	var document apiTypes.TaskAlertSettings
	err = decodeSingleJSON(http.MethodPut, path, bytes.NewReader(response.Body), &document)
	return document, err
}

func (client *Client) TestTaskAlerts(
	ctx context.Context,
	scope, revision string,
) (apiTypes.TaskAlertTestAccepted, error) {
	wire, err := client.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAlertTestAccepted{}, err
	}
	environment, platform := alertScope(scope)
	response, err := wire.TaskAlertTestWithResponse(
		ctx,
		&generated.TaskAlertTestParams{Environment: environment, Platform: platform, IdempotencyKey: ids.NewULID()},
		generated.TaskAlertTestJSONRequestBody{ExpectedRevision: revision},
	)
	const path = "/api/v1/task-alerts/test"
	if err != nil {
		return apiTypes.TaskAlertTestAccepted{}, generatedCallError(ctx, http.MethodPost, path, err)
	}
	if err := generatedResponseError(http.MethodPost, path, response.HTTPResponse, response.Body, http.StatusOK); err != nil {
		return apiTypes.TaskAlertTestAccepted{}, err
	}
	var document apiTypes.TaskAlertTestAccepted
	err = decodeSingleJSON(http.MethodPost, path, bytes.NewReader(response.Body), &document)
	return document, err
}
