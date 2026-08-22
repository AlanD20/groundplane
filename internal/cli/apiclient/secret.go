package apiclient

import (
	"bytes"
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func (c *Client) CreateSecret(
	ctx context.Context,
	input apiTypes.SecretCreateRequest,
) (apiTypes.Secret, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Secret{}, err
	}
	params := &generated.SecretCreateParams{IdempotencyKey: ids.NewULID()}
	body := generated.SecretCreateRequest{Key: input.Key, Kind: input.Kind, Value: input.Value}
	if input.ProjectID != "" {
		body.ProjectId = &input.ProjectID
	}
	if input.Platform {
		body.Platform = &input.Platform
	}
	if input.Path != "" {
		body.Path = &input.Path
	}
	response, err := client.SecretCreateWithResponse(ctx, params, body)
	if err != nil {
		return apiTypes.Secret{}, generatedCallError(ctx, http.MethodPost, "/api/v1/secrets", err)
	}
	if err := generatedResponseError(
		http.MethodPost,
		"/api/v1/secrets",
		response.HTTPResponse,
		response.Body,
		http.StatusCreated,
	); err != nil {
		return apiTypes.Secret{}, err
	}
	parsed := response.JSON201
	if parsed == nil {
		parsed = &generated.Secret{}
		if err := decodeSingleJSON(
			http.MethodPost,
			"/api/v1/secrets",
			bytes.NewReader(response.Body),
			parsed,
		); err != nil {
			return apiTypes.Secret{}, err
		}
	}
	return secretFromGenerated(*parsed), nil
}

func (c *Client) ListSecrets(
	ctx context.Context,
	projectID string,
	platform bool,
	limit int,
	cursor string,
) (apiTypes.Page[apiTypes.Secret], error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Page[apiTypes.Secret]{}, err
	}
	params := &generated.SecretListParams{}
	if projectID != "" {
		params.Project = &projectID
	}
	if platform {
		params.Platform = &platform
	}
	if limit != 0 {
		value := int64(limit)
		params.Limit = &value
	}
	if cursor != "" {
		params.Cursor = &cursor
	}
	response, err := client.SecretListWithResponse(ctx, params)
	if err != nil {
		return apiTypes.Page[apiTypes.Secret]{}, generatedCallError(ctx, http.MethodGet, "/api/v1/secrets", err)
	}
	if err := generatedResponseError(
		http.MethodGet, "/api/v1/secrets", response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.Page[apiTypes.Secret]{}, err
	}
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.PageSecret{}
		if err := decodeSingleJSON(
			http.MethodGet,
			"/api/v1/secrets",
			bytes.NewReader(response.Body),
			parsed,
		); err != nil {
			return apiTypes.Page[apiTypes.Secret]{}, err
		}
	}
	items := []generated.Secret(nil)
	if parsed.Items != nil {
		items = *parsed.Items
	}
	page := apiTypes.Page[apiTypes.Secret]{Items: make([]apiTypes.Secret, len(items))}
	if parsed.NextCursor != nil {
		page.NextCursor = *parsed.NextCursor
	}
	for index, item := range items {
		page.Items[index] = secretFromGenerated(item)
	}
	return page, nil
}

func (c *Client) ShowSecret(ctx context.Context, id string) (apiTypes.Secret, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Secret{}, err
	}
	path := "/api/v1/secrets/" + id
	response, err := client.SecretShowWithResponse(ctx, id)
	if err != nil {
		return apiTypes.Secret{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(
		http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.Secret{}, err
	}
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.Secret{}
		if err := decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), parsed); err != nil {
			return apiTypes.Secret{}, err
		}
	}
	return secretFromGenerated(*parsed), nil
}

func (c *Client) RemoveSecret(ctx context.Context, id string) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	path := "/api/v1/secrets/" + id
	response, err := client.SecretRemoveWithResponse(
		ctx,
		id,
		&generated.SecretRemoveParams{IdempotencyKey: ids.NewULID()},
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

func secretFromGenerated(secret generated.Secret) apiTypes.Secret {
	projectID := ""
	if secret.ProjectId != nil {
		projectID = *secret.ProjectId
	}
	return apiTypes.Secret{
		ID: secret.Id, Scope: secret.Scope, ProjectID: projectID, Key: secret.Key,
		Kind: secret.Kind, Ref: secret.Ref, UpdatedAt: secret.UpdatedAt,
	}
}
