package apiclient

import (
	"bytes"
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func (c *Client) ListProjects(
	ctx context.Context,
	tenantID string,
	kind string,
	limit int,
	cursor string,
) (apiTypes.ProjectPage, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.ProjectPage{}, err
	}
	params := &generated.ProjectListParams{}
	if tenantID != "" {
		params.Tenant = &tenantID
	}
	if kind != "" {
		params.Kind = &kind
	}
	if limit != 0 {
		value := int64(limit)
		params.Limit = &value
	}
	if cursor != "" {
		params.Cursor = &cursor
	}
	response, err := client.ProjectListWithResponse(ctx, params)
	if err != nil {
		return apiTypes.ProjectPage{}, generatedCallError(ctx, http.MethodGet, "/api/v1/projects", err)
	}
	if err := generatedResponseError(
		http.MethodGet, "/api/v1/projects", response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.ProjectPage{}, err
	}
	generatedPage := response.JSON200
	if generatedPage == nil {
		generatedPage = &generated.ProjectPage{}
		if err := decodeSingleJSON(
			http.MethodGet, "/api/v1/projects", bytes.NewReader(response.Body), generatedPage,
		); err != nil {
			return apiTypes.ProjectPage{}, err
		}
	}
	items := []generated.Project(nil)
	if generatedPage.Items != nil {
		items = *generatedPage.Items
	}
	page := apiTypes.ProjectPage{Items: make([]apiTypes.Project, len(items))}
	if generatedPage.NextCursor != nil {
		page.NextCursor = *generatedPage.NextCursor
	}
	for index, project := range items {
		page.Items[index] = projectFromGenerated(project)
	}
	return page, nil
}

func (c *Client) ShowProject(ctx context.Context, id string) (apiTypes.Project, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Project{}, err
	}
	path := "/api/v1/projects/" + id
	response, err := client.ProjectShowWithResponse(ctx, id)
	if err != nil {
		return apiTypes.Project{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(
		http.MethodGet,
		path,
		response.HTTPResponse,
		response.Body,
		http.StatusOK,
	); err != nil {
		return apiTypes.Project{}, err
	}
	return generatedProjectBody(http.MethodGet, path, response.Body, response.JSON200)
}

func (c *Client) CreateProject(ctx context.Context, input apiTypes.ProjectCreate) (apiTypes.Project, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Project{}, err
	}
	params := &generated.ProjectCreateParams{IdempotencyKey: ids.NewULID()}
	body := generated.ProjectCreateJSONRequestBody{
		TenantId: input.TenantID, Slug: input.Slug, Name: input.Name, Description: input.Description,
	}
	response, err := client.ProjectCreateWithResponse(ctx, params, body)
	if err != nil {
		return apiTypes.Project{}, generatedCallError(ctx, http.MethodPost, "/api/v1/projects", err)
	}
	if err := generatedResponseError(
		http.MethodPost, "/api/v1/projects", response.HTTPResponse, response.Body, http.StatusCreated,
	); err != nil {
		return apiTypes.Project{}, err
	}
	return generatedProjectBody(http.MethodPost, "/api/v1/projects", response.Body, response.JSON201)
}

func (c *Client) EditProject(ctx context.Context, id string, input apiTypes.ProjectEdit) (apiTypes.Project, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Project{}, err
	}
	path := "/api/v1/projects/" + id
	params := &generated.ProjectEditParams{IdempotencyKey: ids.NewULID()}
	body := generated.ProjectEditJSONRequestBody{Name: input.Name}
	response, err := client.ProjectEditWithResponse(ctx, id, params, body)
	if err != nil {
		return apiTypes.Project{}, generatedCallError(ctx, http.MethodPatch, path, err)
	}
	if err := generatedResponseError(
		http.MethodPatch,
		path,
		response.HTTPResponse,
		response.Body,
		http.StatusOK,
	); err != nil {
		return apiTypes.Project{}, err
	}
	return generatedProjectBody(http.MethodPatch, path, response.Body, response.JSON200)
}

func (c *Client) RenameProject(ctx context.Context, id, slug string) (apiTypes.Project, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Project{}, err
	}
	path := "/api/v1/projects/" + id + "/rename"
	params := &generated.ProjectRenameParams{IdempotencyKey: ids.NewULID()}
	body := generated.ProjectRenameJSONRequestBody{Slug: slug}
	response, err := client.ProjectRenameWithResponse(ctx, id, params, body)
	if err != nil {
		return apiTypes.Project{}, generatedCallError(ctx, http.MethodPost, path, err)
	}
	if err := generatedResponseError(
		http.MethodPost,
		path,
		response.HTTPResponse,
		response.Body,
		http.StatusOK,
	); err != nil {
		return apiTypes.Project{}, err
	}
	return generatedProjectBody(http.MethodPost, path, response.Body, response.JSON200)
}

func (c *Client) DeleteProject(ctx context.Context, id string) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	path := "/api/v1/projects/" + id
	response, err := client.ProjectDeleteWithResponse(
		ctx, id, &generated.ProjectDeleteParams{IdempotencyKey: ids.NewULID()},
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

func projectFromGenerated(project generated.Project) apiTypes.Project {
	tenantID := ""
	if project.TenantId != nil {
		tenantID = *project.TenantId
	}
	return apiTypes.Project{
		ID: project.Id, TenantID: tenantID, Slug: project.Slug, Name: project.Name,
		Description: project.Description, Kind: project.Kind,
	}
}

func generatedProjectBody(
	method string,
	path string,
	body []byte,
	parsed *generated.Project,
) (apiTypes.Project, error) {
	if parsed == nil {
		parsed = &generated.Project{}
		if err := decodeSingleJSON(method, path, bytes.NewReader(body), parsed); err != nil {
			return apiTypes.Project{}, err
		}
	}
	return projectFromGenerated(*parsed), nil
}
