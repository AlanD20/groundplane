package apiclient

import (
	"bytes"
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func (c *Client) ListAttaches(
	ctx context.Context,
	environmentID string,
	limit int,
	cursor string,
) (apiTypes.Page[apiTypes.Attach], error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Page[apiTypes.Attach]{}, err
	}
	params := &generated.AttachListParams{Environment: environmentID}
	if limit != 0 {
		value := int64(limit)
		params.Limit = &value
	}
	if cursor != "" {
		params.Cursor = &cursor
	}
	response, err := client.AttachListWithResponse(ctx, params)
	if err != nil {
		return apiTypes.Page[apiTypes.Attach]{}, generatedCallError(ctx, http.MethodGet, "/api/v1/attaches", err)
	}
	if err := generatedResponseError(
		http.MethodGet, "/api/v1/attaches", response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.Page[apiTypes.Attach]{}, err
	}
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.PageAttach{}
		if err := decodeSingleJSON(
			http.MethodGet,
			"/api/v1/attaches",
			bytes.NewReader(response.Body),
			parsed,
		); err != nil {
			return apiTypes.Page[apiTypes.Attach]{}, err
		}
	}
	items := []generated.Attach(nil)
	if parsed.Items != nil {
		items = *parsed.Items
	}
	page := apiTypes.Page[apiTypes.Attach]{Items: make([]apiTypes.Attach, len(items))}
	if parsed.NextCursor != nil {
		page.NextCursor = *parsed.NextCursor
	}
	for index, attach := range items {
		page.Items[index] = attachFromGenerated(attach)
	}
	return page, nil
}

func (c *Client) CreateAttach(ctx context.Context, input apiTypes.AttachRequest) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	body := generated.AttachCreateJSONRequestBody{
		BackingServiceId: input.BackingServiceID,
		ServiceIds:       append([]string(nil), input.ServiceIDs...),
	}
	if input.Name != "" {
		body.Name = &input.Name
	}
	if len(input.GrantAttachIDs) > 0 {
		grants := append([]string(nil), input.GrantAttachIDs...)
		body.GrantAttachIds = &grants
	}
	response, err := client.AttachCreateWithResponse(
		ctx,
		&generated.AttachCreateParams{IdempotencyKey: ids.NewULID()},
		body,
	)
	if err != nil {
		return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodPost, "/api/v1/attaches", err)
	}
	if err := generatedResponseError(
		http.MethodPost, "/api/v1/attaches", response.HTTPResponse, response.Body, http.StatusAccepted,
	); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	return generatedTaskAccepted(http.MethodPost, "/api/v1/attaches", response.Body, response.JSON202)
}

func (c *Client) DetachAttach(ctx context.Context, id string) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	path := "/api/v1/attaches/" + id
	response, err := client.AttachDetachWithResponse(
		ctx,
		id,
		&generated.AttachDetachParams{IdempotencyKey: ids.NewULID()},
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

func (c *Client) RenameAttach(ctx context.Context, id, name string) (apiTypes.Attach, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Attach{}, err
	}
	path := "/api/v1/attaches/" + id + "/rename"
	response, err := client.AttachRenameWithResponse(
		ctx,
		id,
		&generated.AttachRenameParams{IdempotencyKey: ids.NewULID()},
		generated.AttachRenameJSONRequestBody{Name: name},
	)
	if err != nil {
		return apiTypes.Attach{}, generatedCallError(ctx, http.MethodPost, path, err)
	}
	if err := generatedResponseError(
		http.MethodPost, path, response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.Attach{}, err
	}
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.Attach{}
		if err := decodeSingleJSON(http.MethodPost, path, bytes.NewReader(response.Body), parsed); err != nil {
			return apiTypes.Attach{}, err
		}
	}
	return attachFromGenerated(*parsed), nil
}

func attachFromGenerated(attach generated.Attach) apiTypes.Attach {
	serviceIDs := []string(nil)
	if attach.ServiceIds != nil {
		serviceIDs = append(serviceIDs, (*attach.ServiceIds)...)
	}
	grantIDs := []string(nil)
	if attach.GrantAttachIds != nil {
		grantIDs = append(grantIDs, (*attach.GrantAttachIds)...)
	}
	backingEnvironmentID := ""
	if attach.BackingEnvironmentId != nil {
		backingEnvironmentID = *attach.BackingEnvironmentId
	}
	return apiTypes.Attach{
		ID: attach.Id, Name: attach.Name, ServiceIDs: serviceIDs,
		BackingServiceID: attach.BackingServiceId, BackingEnvironmentID: backingEnvironmentID,
		BackingNetworkID: attach.BackingNetworkId, GrantAttachIDs: grantIDs, Status: attach.Status,
	}
}

func generatedTaskAccepted(
	method string,
	path string,
	body []byte,
	parsed *generated.TaskAccepted,
) (apiTypes.TaskAccepted, error) {
	if parsed == nil {
		parsed = &generated.TaskAccepted{}
		if err := decodeSingleJSON(method, path, bytes.NewReader(body), parsed); err != nil {
			return apiTypes.TaskAccepted{}, err
		}
	}
	return apiTypes.TaskAccepted{TaskID: parsed.TaskId}, nil
}
