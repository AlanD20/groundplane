package apiclient

import (
	"bytes"
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func (c *Client) ListEnvironments(
	ctx context.Context,
	projectID string,
	limit int,
	cursor string,
) (apiTypes.EnvironmentPage, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.EnvironmentPage{}, err
	}
	params := &generated.EnvironmentListParams{Project: projectID}
	if limit != 0 {
		value := int64(limit)
		params.Limit = &value
	}
	if cursor != "" {
		params.Cursor = &cursor
	}
	response, err := client.EnvironmentListWithResponse(ctx, params)
	if err != nil {
		return apiTypes.EnvironmentPage{}, generatedCallError(ctx, http.MethodGet, "/api/v1/environments", err)
	}
	if err := generatedResponseError(
		http.MethodGet, "/api/v1/environments", response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.EnvironmentPage{}, err
	}
	generatedPage := response.JSON200
	if generatedPage == nil {
		generatedPage = &generated.EnvironmentPage{}
		if err := decodeSingleJSON(
			http.MethodGet, "/api/v1/environments", bytes.NewReader(response.Body), generatedPage,
		); err != nil {
			return apiTypes.EnvironmentPage{}, err
		}
	}
	items := []generated.Environment(nil)
	if generatedPage.Items != nil {
		items = *generatedPage.Items
	}
	page := apiTypes.EnvironmentPage{Items: make([]apiTypes.Environment, len(items))}
	if generatedPage.NextCursor != nil {
		page.NextCursor = *generatedPage.NextCursor
	}
	for index, environment := range items {
		page.Items[index] = environmentFromGenerated(environment)
	}
	return page, nil
}

func (c *Client) ShowEnvironment(ctx context.Context, id string) (apiTypes.Environment, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Environment{}, err
	}
	path := "/api/v1/environments/" + id
	response, err := client.EnvironmentShowWithResponse(ctx, id)
	if err != nil {
		return apiTypes.Environment{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(
		http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.Environment{}, err
	}
	return generatedEnvironmentBody(http.MethodGet, path, response.Body, response.JSON200)
}

func (c *Client) CreateEnvironment(
	ctx context.Context,
	input apiTypes.EnvironmentCreate,
) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	params := &generated.EnvironmentCreateParams{IdempotencyKey: ids.NewULID()}
	body := generated.EnvironmentCreateJSONRequestBody{
		ProjectId: input.ProjectID, Name: input.Name, NetworkPool: input.NetworkPool,
	}
	response, err := client.EnvironmentCreateWithResponse(ctx, params, body)
	if err != nil {
		return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodPost, "/api/v1/environments", err)
	}
	if err := generatedResponseError(
		http.MethodPost, "/api/v1/environments", response.HTTPResponse, response.Body, http.StatusAccepted,
	); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	parsed := response.JSON202
	if parsed == nil {
		parsed = &generated.TaskAccepted{}
		if err := decodeSingleJSON(
			http.MethodPost, "/api/v1/environments", bytes.NewReader(response.Body), parsed,
		); err != nil {
			return apiTypes.TaskAccepted{}, err
		}
	}
	return apiTypes.TaskAccepted{TaskID: parsed.TaskId}, nil
}

func (c *Client) RenameEnvironment(ctx context.Context, id, name string) (apiTypes.Environment, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Environment{}, err
	}
	path := "/api/v1/environments/" + id + "/rename"
	params := &generated.EnvironmentRenameParams{IdempotencyKey: ids.NewULID()}
	body := generated.EnvironmentRenameJSONRequestBody{Name: name}
	response, err := client.EnvironmentRenameWithResponse(ctx, id, params, body)
	if err != nil {
		return apiTypes.Environment{}, generatedCallError(ctx, http.MethodPost, path, err)
	}
	if err := generatedResponseError(
		http.MethodPost, path, response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.Environment{}, err
	}
	return generatedEnvironmentBody(http.MethodPost, path, response.Body, response.JSON200)
}

func (c *Client) EditEnvironment(
	ctx context.Context,
	id string,
	networkPool string,
) (apiTypes.Environment, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Environment{}, err
	}
	path := "/api/v1/environments/" + id
	params := &generated.EnvironmentEditParams{IdempotencyKey: ids.NewULID()}
	body := generated.EnvironmentEditJSONRequestBody{NetworkPool: networkPool}
	response, err := client.EnvironmentEditWithResponse(ctx, id, params, body)
	if err != nil {
		return apiTypes.Environment{}, generatedCallError(ctx, http.MethodPatch, path, err)
	}
	if err := generatedResponseError(
		http.MethodPatch, path, response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.Environment{}, err
	}
	return generatedEnvironmentBody(http.MethodPatch, path, response.Body, response.JSON200)
}

func (c *Client) DeleteEnvironment(ctx context.Context, id string) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	path := "/api/v1/environments/" + id
	params := &generated.EnvironmentDeleteParams{IdempotencyKey: ids.NewULID()}
	response, err := client.EnvironmentDeleteWithResponse(ctx, id, params)
	if err != nil {
		return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodDelete, path, err)
	}
	if err := generatedResponseError(
		http.MethodDelete, path, response.HTTPResponse, response.Body, http.StatusAccepted,
	); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	parsed := response.JSON202
	if parsed == nil {
		parsed = &generated.HierarchyDeleteOutputBody{}
		if err := decodeSingleJSON(http.MethodDelete, path, bytes.NewReader(response.Body), parsed); err != nil {
			return apiTypes.TaskAccepted{}, err
		}
	}
	return apiTypes.TaskAccepted{TaskID: parsed.TaskId}, nil
}

func (c *Client) ShowEnvironmentBlueprint(
	ctx context.Context,
	id string,
) (apiTypes.EnvironmentBlueprintDocument, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.EnvironmentBlueprintDocument{}, err
	}
	path := "/api/v1/environments/" + id + "/blueprint"
	response, err := client.BlueprintShowWithResponse(ctx, id)
	if err != nil {
		return apiTypes.EnvironmentBlueprintDocument{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(
		http.MethodGet,
		path,
		response.HTTPResponse,
		response.Body,
		http.StatusOK,
	); err != nil {
		return apiTypes.EnvironmentBlueprintDocument{}, err
	}
	var document apiTypes.EnvironmentBlueprintDocument
	if err := decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), &document); err != nil {
		return apiTypes.EnvironmentBlueprintDocument{}, err
	}
	return document, nil
}

func (c *Client) ValidateEnvironmentBlueprint(
	ctx context.Context,
	id string,
	expectedRevision string,
	body []byte,
	contentType string,
) (apiTypes.EnvironmentBlueprintValidation, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.EnvironmentBlueprintValidation{}, err
	}
	path := "/api/v1/environments/" + id + "/blueprint/validate"
	params := &generated.BlueprintValidateParams{IfMatch: quoteBlueprintRevision(expectedRevision)}
	response, err := client.BlueprintValidateWithBodyWithResponse(
		ctx,
		id,
		params,
		contentType,
		bytes.NewReader(body),
	)
	if err != nil {
		return apiTypes.EnvironmentBlueprintValidation{}, generatedCallError(ctx, http.MethodPost, path, err)
	}
	if err := generatedResponseError(
		http.MethodPost,
		path,
		response.HTTPResponse,
		response.Body,
		http.StatusOK,
	); err != nil {
		return apiTypes.EnvironmentBlueprintValidation{}, err
	}
	var validation apiTypes.EnvironmentBlueprintValidation
	if err := decodeSingleJSON(http.MethodPost, path, bytes.NewReader(response.Body), &validation); err != nil {
		return apiTypes.EnvironmentBlueprintValidation{}, err
	}
	return validation, nil
}

func (c *Client) ApplyEnvironmentBlueprint(
	ctx context.Context,
	id string,
	expectedRevision string,
	body []byte,
	contentType string,
) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	path := "/api/v1/environments/" + id + "/blueprint"
	params := &generated.BlueprintApplyParams{
		IdempotencyKey: ids.NewULID(),
		IfMatch:        quoteBlueprintRevision(expectedRevision),
	}
	response, err := client.BlueprintApplyWithBodyWithResponse(
		ctx, id, params, contentType, bytes.NewReader(body),
	)
	if err != nil {
		return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodPut, path, err)
	}
	if err := generatedResponseError(
		http.MethodPut, path, response.HTTPResponse, response.Body, http.StatusAccepted,
	); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	parsed := response.JSON202
	if parsed == nil {
		parsed = &generated.TaskAccepted{}
		if err := decodeSingleJSON(http.MethodPut, path, bytes.NewReader(response.Body), parsed); err != nil {
			return apiTypes.TaskAccepted{}, err
		}
	}
	return apiTypes.TaskAccepted{TaskID: parsed.TaskId}, nil
}

func quoteBlueprintRevision(revision string) string {
	return "\"" + revision + "\""
}

func environmentFromGenerated(environment generated.Environment) apiTypes.Environment {
	var createTaskID *string
	if environment.CreateTaskId != nil {
		createTaskID = environment.CreateTaskId
	}
	var deletionTaskID *string
	if environment.DeletionTaskId != nil {
		deletionTaskID = environment.DeletionTaskId
	}
	return apiTypes.Environment{
		ID: environment.Id, ProjectID: environment.ProjectId, Name: environment.Name,
		NetworkPool:       environment.NetworkPool,
		VolumeDir:         environment.VolumeDir,
		ProvisioningState: apiTypes.EnvironmentProvisioningState(environment.ProvisioningState),
		CreateTaskID:      createTaskID,
		DeletionTaskID:    deletionTaskID,
		NetworkCapacity: apiTypes.EnvironmentNetworkCapacity{
			TotalAddresses:     environment.NetworkCapacity.TotalAddresses,
			AllocatedAddresses: environment.NetworkCapacity.AllocatedAddresses,
			AvailableAddresses: environment.NetworkCapacity.AvailableAddresses,
			ZoneCount:          environment.NetworkCapacity.ZoneCount,
		},
	}
}

func generatedEnvironmentBody(
	method string,
	path string,
	body []byte,
	parsed *generated.Environment,
) (apiTypes.Environment, error) {
	if parsed == nil {
		parsed = &generated.Environment{}
		if err := decodeSingleJSON(method, path, bytes.NewReader(body), parsed); err != nil {
			return apiTypes.Environment{}, err
		}
	}
	return environmentFromGenerated(*parsed), nil
}
