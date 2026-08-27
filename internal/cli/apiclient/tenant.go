package apiclient

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (c *Client) ListTenants(
	ctx context.Context,
	limit int,
	cursor string,
) (apiTypes.TenantPage, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TenantPage{}, err
	}
	params := &generated.TenantListParams{}
	if limit != 0 {
		value := int64(limit)
		params.Limit = &value
	}
	if cursor != "" {
		params.Cursor = &cursor
	}
	response, err := client.TenantListWithResponse(ctx, params)
	if err != nil {
		return apiTypes.TenantPage{}, generatedCallError(ctx, http.MethodGet, "/api/v1/tenants", err)
	}
	if err := generatedResponseError(
		http.MethodGet,
		"/api/v1/tenants",
		response.HTTPResponse,
		response.Body,
		http.StatusOK,
	); err != nil {
		return apiTypes.TenantPage{}, err
	}
	generatedPage := response.JSON200
	if generatedPage == nil {
		generatedPage = &generated.TenantPage{}
		if err := decodeSingleJSON(
			http.MethodGet,
			"/api/v1/tenants",
			bytes.NewReader(response.Body),
			generatedPage,
		); err != nil {
			return apiTypes.TenantPage{}, err
		}
	}
	items := []generated.Tenant(nil)
	if generatedPage.Items != nil {
		items = *generatedPage.Items
	}
	page := apiTypes.TenantPage{Items: make([]apiTypes.Tenant, len(items))}
	if generatedPage.NextCursor != nil {
		page.NextCursor = *generatedPage.NextCursor
	}
	for index, tenant := range items {
		page.Items[index] = tenantFromGenerated(tenant)
	}
	return page, nil
}

func (c *Client) ShowTenant(ctx context.Context, id string) (apiTypes.Tenant, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Tenant{}, err
	}
	path := "/api/v1/tenants/" + id
	response, err := client.TenantShowWithResponse(ctx, id)
	if err != nil {
		return apiTypes.Tenant{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(
		http.MethodGet,
		path,
		response.HTTPResponse,
		response.Body,
		http.StatusOK,
	); err != nil {
		return apiTypes.Tenant{}, err
	}
	return generatedTenantBody(http.MethodGet, path, response.Body, response.JSON200)
}

func (c *Client) CreateTenant(ctx context.Context, input apiTypes.TenantCreate) (apiTypes.Tenant, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Tenant{}, err
	}
	params := &generated.TenantCreateParams{IdempotencyKey: ids.NewULID()}
	body := generated.TenantCreateJSONRequestBody{
		Slug: input.Slug, Name: input.Name, Description: input.Description,
	}
	response, err := client.TenantCreateWithResponse(ctx, params, body)
	if err != nil {
		return apiTypes.Tenant{}, generatedCallError(ctx, http.MethodPost, "/api/v1/tenants", err)
	}
	if err := generatedResponseError(
		http.MethodPost,
		"/api/v1/tenants",
		response.HTTPResponse,
		response.Body,
		http.StatusCreated,
	); err != nil {
		return apiTypes.Tenant{}, err
	}
	return generatedTenantBody(http.MethodPost, "/api/v1/tenants", response.Body, response.JSON201)
}

func (c *Client) EditTenant(ctx context.Context, id string, input apiTypes.TenantEdit) (apiTypes.Tenant, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Tenant{}, err
	}
	path := "/api/v1/tenants/" + id
	params := &generated.TenantEditParams{IdempotencyKey: ids.NewULID()}
	body := generated.TenantEditJSONRequestBody{Name: input.Name, Description: input.Description}
	response, err := client.TenantEditWithResponse(ctx, id, params, body)
	if err != nil {
		return apiTypes.Tenant{}, generatedCallError(ctx, http.MethodPatch, path, err)
	}
	if err := generatedResponseError(
		http.MethodPatch,
		path,
		response.HTTPResponse,
		response.Body,
		http.StatusOK,
	); err != nil {
		return apiTypes.Tenant{}, err
	}
	return generatedTenantBody(http.MethodPatch, path, response.Body, response.JSON200)
}

func (c *Client) RenameTenant(ctx context.Context, id, slug string) (apiTypes.Tenant, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Tenant{}, err
	}
	path := "/api/v1/tenants/" + id + "/rename"
	params := &generated.TenantRenameParams{IdempotencyKey: ids.NewULID()}
	body := generated.TenantRenameJSONRequestBody{Slug: slug}
	response, err := client.TenantRenameWithResponse(ctx, id, params, body)
	if err != nil {
		return apiTypes.Tenant{}, generatedCallError(ctx, http.MethodPost, path, err)
	}
	if err := generatedResponseError(
		http.MethodPost,
		path,
		response.HTTPResponse,
		response.Body,
		http.StatusOK,
	); err != nil {
		return apiTypes.Tenant{}, err
	}
	return generatedTenantBody(http.MethodPost, path, response.Body, response.JSON200)
}

func (c *Client) DeleteTenant(ctx context.Context, id string) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	path := "/api/v1/tenants/" + id
	response, err := client.TenantDeleteWithResponse(
		ctx, id, &generated.TenantDeleteParams{IdempotencyKey: ids.NewULID()},
	)
	if err != nil {
		return apiTypes.TaskAccepted{}, generatedCallError(ctx, http.MethodDelete, path, err)
	}
	if err := generatedResponseError(
		http.MethodDelete, path, response.HTTPResponse, response.Body, http.StatusAccepted,
	); err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	return generatedHierarchyTaskAccepted(http.MethodDelete, path, response.Body, response.JSON202)
}

func (c *Client) generatedHumanClient() (*generated.ClientWithResponses, error) {
	client, err := generated.NewClientWithResponses(
		c.BaseURL+"/api/v1",
		generated.WithHTTPClient(c.HTTP),
	)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, fmt.Errorf("apiclient: configure generated client: %w", err))
	}
	return client, nil
}

func generatedCallError(ctx context.Context, method, path string, err error) error {
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	return errs.Wrap(
		errs.KindInternal,
		fmt.Errorf("apiclient: %s %s: %w (is the Controller running? --host / GROUNDPLANE_HOST)", method, path, err),
	)
}

func generatedResponseError(
	method string,
	path string,
	response *http.Response,
	body []byte,
	expectedStatus int,
) error {
	if response == nil {
		return errs.Newf(errs.KindInternal, "apiclient: %s %s returned no HTTP response", method, path)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		copy := &http.Response{
			StatusCode: response.StatusCode,
			Header:     response.Header.Clone(),
			Body:       io.NopCloser(bytes.NewReader(body)),
		}
		return responseProblem(method, path, copy)
	}
	if response.StatusCode != expectedStatus {
		return errs.Newf(
			errs.KindInternal,
			"apiclient: %s %s returned unexpected success status %d (want %d)",
			method,
			path,
			response.StatusCode,
			expectedStatus,
		)
	}
	return nil
}

func tenantFromGenerated(tenant generated.Tenant) apiTypes.Tenant {
	return apiTypes.Tenant{
		ID: tenant.Id, Slug: tenant.Slug, Name: tenant.Name, Description: tenant.Description,
	}
}

func generatedTenantBody(
	method string,
	path string,
	body []byte,
	parsed *generated.Tenant,
) (apiTypes.Tenant, error) {
	if parsed == nil {
		parsed = &generated.Tenant{}
		if err := decodeSingleJSON(method, path, bytes.NewReader(body), parsed); err != nil {
			return apiTypes.Tenant{}, err
		}
	}
	return tenantFromGenerated(*parsed), nil
}
