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

func (c *Client) CreateScript(ctx context.Context, input apiTypes.ScriptCreate) (apiTypes.Script, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Script{}, err
	}
	order := int32(input.Order)
	response, err := client.ScriptCreateWithResponse(ctx, &generated.ScriptCreateParams{
		IdempotencyKey: ids.NewULID(),
	}, generated.ScriptCreateJSONRequestBody{
		EnvironmentId: input.EnvironmentID, Slug: input.Slug, ServiceId: input.ServiceID,
		Script: input.Body, When: input.When, Order: &order,
		Execution: scriptExecutionToGenerated(input.Execution),
	})
	if err != nil {
		return apiTypes.Script{}, generatedCallError(ctx, http.MethodPost, "/api/v1/scripts", err)
	}
	if err := generatedResponseError(
		http.MethodPost, "/api/v1/scripts", response.HTTPResponse, response.Body, http.StatusCreated,
	); err != nil {
		return apiTypes.Script{}, err
	}
	return decodeScriptResponse(http.MethodPost, "/api/v1/scripts", response.Body)
}

func (c *Client) ListScripts(
	ctx context.Context,
	environmentID string,
	limit int,
	cursor string,
) (apiTypes.Page[apiTypes.Script], error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Page[apiTypes.Script]{}, err
	}
	params := &generated.ScriptListParams{Environment: environmentID}
	if limit != 0 {
		value := int64(limit)
		params.Limit = &value
	}
	if cursor != "" {
		params.Cursor = &cursor
	}
	response, err := client.ScriptListWithResponse(ctx, params)
	if err != nil {
		return apiTypes.Page[apiTypes.Script]{}, generatedCallError(ctx, http.MethodGet, "/api/v1/scripts", err)
	}
	if err := generatedResponseError(
		http.MethodGet, "/api/v1/scripts", response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.Page[apiTypes.Script]{}, err
	}
	var page apiTypes.Page[apiTypes.Script]
	if err := decodeSingleJSON(http.MethodGet, "/api/v1/scripts", bytes.NewReader(response.Body), &page); err != nil {
		return apiTypes.Page[apiTypes.Script]{}, err
	}
	for _, item := range page.Items {
		if err := validateScriptResponse(item); err != nil {
			return apiTypes.Page[apiTypes.Script]{}, err
		}
	}
	return page, nil
}

func (c *Client) GetScript(ctx context.Context, id string) (apiTypes.Script, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Script{}, err
	}
	path := "/api/v1/scripts/" + id
	response, err := client.ScriptShowWithResponse(ctx, id)
	if err != nil {
		return apiTypes.Script{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(
		http.MethodGet,
		path,
		response.HTTPResponse,
		response.Body,
		http.StatusOK,
	); err != nil {
		return apiTypes.Script{}, err
	}
	return decodeScriptResponse(http.MethodGet, path, response.Body)
}

func (c *Client) EditScript(ctx context.Context, id string, input apiTypes.ScriptEdit) (apiTypes.Script, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Script{}, err
	}
	path := "/api/v1/scripts/" + id
	var order *int32
	if input.Order != nil {
		value := int32(*input.Order)
		order = &value
	}
	response, err := client.ScriptEditWithResponse(ctx, id, &generated.ScriptEditParams{
		IdempotencyKey: ids.NewULID(),
	}, generated.ScriptEditJSONRequestBody{Slug: input.Slug, Script: input.Body, When: input.When, Order: order,
		Execution: scriptExecutionToGenerated(input.Execution)})
	if err != nil {
		return apiTypes.Script{}, generatedCallError(ctx, http.MethodPatch, path, err)
	}
	if err := generatedResponseError(
		http.MethodPatch,
		path,
		response.HTTPResponse,
		response.Body,
		http.StatusOK,
	); err != nil {
		return apiTypes.Script{}, err
	}
	return decodeScriptResponse(http.MethodPatch, path, response.Body)
}

func (c *Client) RemoveScript(ctx context.Context, id string) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	path := "/api/v1/scripts/" + id
	response, err := client.ScriptRemoveWithResponse(
		ctx, id, &generated.ScriptRemoveParams{IdempotencyKey: ids.NewULID()},
	)
	if err != nil {
		return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodDelete, path, err)
	}
	if err := generatedResponseError(
		http.MethodDelete, path, response.HTTPResponse, response.Body, http.StatusAccepted,
	); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	return generatedTaskAccepted(http.MethodDelete, path, response.Body, response.JSON202)
}

func decodeScriptResponse(method, path string, body []byte) (apiTypes.Script, error) {
	// Decode original HTTP bytes so generated scalar defaults cannot erase
	// missing decisions in the closed execution context.
	var script apiTypes.Script
	if err := decodeSingleJSON(method, path, bytes.NewReader(body), &script); err != nil {
		return apiTypes.Script{}, err
	}
	return script, validateScriptResponse(script)
}

func validateScriptResponse(script apiTypes.Script) error {
	if (script.Origin != "api" && script.Origin != "blueprint") || script.ActiveGeneration < 1 ||
		(script.Execution.Mode != "inherited" && script.Execution.Mode != "explicit") {
		return errs.New(errs.KindInternal, "Controller returned an invalid Script")
	}
	return nil
}
