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
	response, err := client.ScriptCreateWithResponse(ctx, &generated.ScriptCreateParams{
		IdempotencyKey: ids.NewULID(),
	}, generated.ScriptCreateJSONRequestBody{
		EnvironmentId: input.EnvironmentID, Slug: input.Slug, ServiceId: input.ServiceID,
		Script: input.Body, When: input.When,
	})
	if err != nil {
		return apiTypes.Script{}, generatedCallError(ctx, http.MethodPost, "/api/v1/scripts", err)
	}
	if err := generatedResponseError(
		http.MethodPost, "/api/v1/scripts", response.HTTPResponse, response.Body, http.StatusCreated,
	); err != nil {
		return apiTypes.Script{}, err
	}
	parsed := response.JSON201
	if parsed == nil {
		parsed = &generated.Script{}
		if err := decodeSingleJSON(
			http.MethodPost,
			"/api/v1/scripts",
			bytes.NewReader(response.Body),
			parsed,
		); err != nil {
			return apiTypes.Script{}, err
		}
	}
	return scriptFromGenerated(*parsed)
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
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.PageScript{}
		if err := decodeSingleJSON(
			http.MethodGet,
			"/api/v1/scripts",
			bytes.NewReader(response.Body),
			parsed,
		); err != nil {
			return apiTypes.Page[apiTypes.Script]{}, err
		}
	}
	items := []generated.Script(nil)
	if parsed.Items != nil {
		items = *parsed.Items
	}
	page := apiTypes.Page[apiTypes.Script]{Items: make([]apiTypes.Script, len(items))}
	if parsed.NextCursor != nil {
		page.NextCursor = *parsed.NextCursor
	}
	for index, item := range items {
		converted, convertErr := scriptFromGenerated(item)
		if convertErr != nil {
			return apiTypes.Page[apiTypes.Script]{}, convertErr
		}
		page.Items[index] = converted
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
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.Script{}
		if err := decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), parsed); err != nil {
			return apiTypes.Script{}, err
		}
	}
	return scriptFromGenerated(*parsed)
}

func (c *Client) EditScript(ctx context.Context, id string, input apiTypes.ScriptEdit) (apiTypes.Script, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Script{}, err
	}
	path := "/api/v1/scripts/" + id
	response, err := client.ScriptEditWithResponse(ctx, id, &generated.ScriptEditParams{
		IdempotencyKey: ids.NewULID(),
	}, generated.ScriptEditJSONRequestBody{Slug: input.Slug, Script: input.Body, When: input.When})
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
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.Script{}
		if err := decodeSingleJSON(http.MethodPatch, path, bytes.NewReader(response.Body), parsed); err != nil {
			return apiTypes.Script{}, err
		}
	}
	return scriptFromGenerated(*parsed)
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

func scriptFromGenerated(script generated.Script) (apiTypes.Script, error) {
	if !script.Origin.Valid() || script.ActiveGeneration < 1 {
		return apiTypes.Script{}, errs.New(errs.KindInternal, "Controller returned an invalid Script")
	}
	reconciliationKey := ""
	if script.ReconciliationKey != nil {
		reconciliationKey = *script.ReconciliationKey
	}
	return apiTypes.Script{
		ID: script.Id, EnvironmentID: script.EnvironmentId, Slug: script.Slug,
		ServiceID: script.ServiceId, ServiceName: script.Service, Body: script.Script,
		When: script.When, Origin: string(script.Origin), ReconciliationKey: reconciliationKey,
		ActiveGeneration: uint64(script.ActiveGeneration),
	}, nil
}
