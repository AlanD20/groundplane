package apiclient

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (c *Client) ListReleaseGroups(
	ctx context.Context,
	environmentID string,
	limit int,
	cursor string,
) (apiTypes.Page[apiTypes.ReleaseGroup], error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Page[apiTypes.ReleaseGroup]{}, err
	}
	params := &generated.ReleaseGroupListParams{EnvironmentId: environmentID}
	if limit != 0 {
		value := int64(limit)
		params.Limit = &value
	}
	if cursor != "" {
		params.Cursor = &cursor
	}
	response, err := client.ReleaseGroupListWithResponse(ctx, params)
	if err != nil {
		return apiTypes.Page[apiTypes.ReleaseGroup]{}, generatedCallError(
			ctx,
			http.MethodGet,
			"/api/v1/release-groups",
			err,
		)
	}
	if err := generatedResponseError(http.MethodGet, "/api/v1/release-groups", response.HTTPResponse, response.Body, http.StatusOK); err != nil {
		return apiTypes.Page[apiTypes.ReleaseGroup]{}, err
	}
	page := apiTypes.Page[apiTypes.ReleaseGroup]{}
	if err := decodeSingleJSON(http.MethodGet, "/api/v1/release-groups", bytes.NewReader(response.Body), &page); err != nil {
		return apiTypes.Page[apiTypes.ReleaseGroup]{}, err
	}
	return page, nil
}

func (c *Client) GetReleaseGroup(ctx context.Context, id string) (apiTypes.ReleaseGroup, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.ReleaseGroup{}, err
	}
	path := "/api/v1/release-groups/" + id
	response, err := client.ReleaseGroupShowWithResponse(ctx, id)
	if err != nil {
		return apiTypes.ReleaseGroup{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK); err != nil {
		return apiTypes.ReleaseGroup{}, err
	}
	return decodeReleaseGroupResponse(http.MethodGet, path, response.Body)
}

func (c *Client) AddReleaseGroup(
	ctx context.Context,
	input apiTypes.ReleaseGroupAddRequest,
) (apiTypes.ReleaseGroup, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.ReleaseGroup{}, err
	}
	body := generated.ReleaseGroupAddJSONRequestBody{
		EnvironmentId: input.EnvironmentID,
		Name:          input.Name,
		ServiceIds:    optionalServiceStrings(input.ServiceIDs),
		Order:         optionalServiceStrings(input.Order),
		Tag:           optionalServiceString(input.Tag),
		OnFailure:     optionalServiceString(string(input.OnFailure)),
	}
	response, err := client.ReleaseGroupAddWithResponse(
		ctx,
		&generated.ReleaseGroupAddParams{IdempotencyKey: ids.NewULID()},
		body,
	)
	if err != nil {
		return apiTypes.ReleaseGroup{}, generatedCallError(ctx, http.MethodPost, "/api/v1/release-groups", err)
	}
	if err := generatedResponseError(http.MethodPost, "/api/v1/release-groups", response.HTTPResponse, response.Body, http.StatusCreated); err != nil {
		return apiTypes.ReleaseGroup{}, err
	}
	return decodeReleaseGroupResponse(http.MethodPost, "/api/v1/release-groups", response.Body)
}

func (c *Client) EditReleaseGroup(
	ctx context.Context,
	id string,
	input apiTypes.ReleaseGroupEditRequest,
) (apiTypes.ReleaseGroup, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.ReleaseGroup{}, err
	}
	body, err := json.Marshal(input)
	if err != nil {
		return apiTypes.ReleaseGroup{}, errs.Wrap(errs.KindInternal, err)
	}
	path := "/api/v1/release-groups/" + id
	response, err := client.ReleaseGroupEditWithBodyWithResponse(
		ctx,
		id,
		&generated.ReleaseGroupEditParams{IdempotencyKey: ids.NewULID()},
		"application/json", bytes.NewReader(body),
	)
	if err != nil {
		return apiTypes.ReleaseGroup{}, generatedCallError(ctx, http.MethodPatch, path, err)
	}
	if err := generatedResponseError(http.MethodPatch, path, response.HTTPResponse, response.Body, http.StatusOK); err != nil {
		return apiTypes.ReleaseGroup{}, err
	}
	return decodeReleaseGroupResponse(http.MethodPatch, path, response.Body)
}

func (c *Client) RemoveReleaseGroup(ctx context.Context, id string) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	path := "/api/v1/release-groups/" + id
	response, err := client.ReleaseGroupRemoveWithResponse(
		ctx,
		id,
		&generated.ReleaseGroupRemoveParams{IdempotencyKey: ids.NewULID()},
	)
	if err != nil {
		return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodDelete, path, err)
	}
	if err := generatedResponseError(http.MethodDelete, path, response.HTTPResponse, response.Body, http.StatusAccepted); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	accepted := apiTypes.TaskAccepted{}
	if err := decodeSingleJSON(http.MethodDelete, path, bytes.NewReader(response.Body), &accepted); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	return accepted, nil
}

func decodeReleaseGroupResponse(method, path string, body []byte) (apiTypes.ReleaseGroup, error) {
	result := apiTypes.ReleaseGroup{}
	if err := decodeSingleJSON(method, path, bytes.NewReader(body), &result); err != nil {
		return apiTypes.ReleaseGroup{}, err
	}
	return result, nil
}
