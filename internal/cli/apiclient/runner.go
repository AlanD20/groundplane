package apiclient

import (
	"bytes"
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func (c *Client) ListRunners(
	ctx context.Context,
	tenantID string,
	projectID string,
	limit int,
	cursor string,
) (apiTypes.RunnerPage, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.RunnerPage{}, err
	}
	params := &generated.RunnerListParams{}
	if tenantID != "" {
		params.Tenant = &tenantID
	}
	if projectID != "" {
		params.Project = &projectID
	}
	if limit != 0 {
		value := int64(limit)
		params.Limit = &value
	}
	if cursor != "" {
		params.Cursor = &cursor
	}
	response, err := client.RunnerListWithResponse(ctx, params)
	if err != nil {
		return apiTypes.RunnerPage{}, generatedCallError(ctx, http.MethodGet, "/api/v1/runners", err)
	}
	if err := generatedResponseError(
		http.MethodGet, "/api/v1/runners", response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.RunnerPage{}, err
	}
	generatedPage := response.JSON200
	if generatedPage == nil {
		generatedPage = &generated.PageRunner{}
		if err := decodeSingleJSON(
			http.MethodGet, "/api/v1/runners", bytes.NewReader(response.Body), generatedPage,
		); err != nil {
			return apiTypes.RunnerPage{}, err
		}
	}
	items := []generated.Runner(nil)
	if generatedPage.Items != nil {
		items = *generatedPage.Items
	}
	page := apiTypes.RunnerPage{Items: make([]apiTypes.Runner, len(items))}
	if generatedPage.NextCursor != nil {
		page.NextCursor = *generatedPage.NextCursor
	}
	for index, runner := range items {
		page.Items[index] = runnerFromGenerated(runner)
	}
	return page, nil
}

func (c *Client) ShowRunner(ctx context.Context, id string) (apiTypes.Runner, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Runner{}, err
	}
	path := "/api/v1/runners/" + id
	response, err := client.RunnerShowWithResponse(ctx, id)
	if err != nil {
		return apiTypes.Runner{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(
		http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.Runner{}, err
	}
	return generatedRunnerBody(http.MethodGet, path, response.Body, response.JSON200)
}

func (c *Client) EditRunner(ctx context.Context, id, slug string) (apiTypes.Runner, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Runner{}, err
	}
	path := "/api/v1/runners/" + id
	params := &generated.RunnerEditParams{IdempotencyKey: ids.NewULID()}
	body := generated.RunnerEditJSONRequestBody{Slug: slug}
	response, err := client.RunnerEditWithResponse(ctx, id, params, body)
	if err != nil {
		return apiTypes.Runner{}, generatedCallError(ctx, http.MethodPatch, path, err)
	}
	if err := generatedResponseError(
		http.MethodPatch, path, response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.Runner{}, err
	}
	return generatedRunnerBody(http.MethodPatch, path, response.Body, response.JSON200)
}

func (c *Client) RemoveRunner(ctx context.Context, id string) (apiTypes.TaskAccepted, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAccepted{}, err
	}
	path := "/api/v1/runners/" + id
	response, err := client.RunnerRemoveWithResponse(
		ctx,
		id,
		&generated.RunnerRemoveParams{IdempotencyKey: ids.NewULID()},
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

func runnerFromGenerated(runner generated.Runner) apiTypes.Runner {
	projectID := ""
	if runner.ProjectId != nil {
		projectID = *runner.ProjectId
	}
	labels := []string(nil)
	if runner.Labels != nil {
		labels = append(labels, (*runner.Labels)...)
	}
	return apiTypes.Runner{
		ID: runner.Id, Slug: runner.Slug, TenantID: runner.TenantId, ProjectID: projectID,
		GitHubURL: runner.GithubUrl, Name: runner.Name, Labels: labels,
		Lifecycle: apiTypes.RunnerLifecycle(runner.Lifecycle), CreateTaskID: runner.CreateTaskId,
		RemoveTaskID: runner.RemoveTaskId, Online: runner.Online,
		ObservedAt: runner.ObservedAt, CreatedAt: runner.CreatedAt,
	}
}

func generatedRunnerBody(
	method string,
	path string,
	body []byte,
	parsed *generated.Runner,
) (apiTypes.Runner, error) {
	if parsed == nil {
		parsed = &generated.Runner{}
		if err := decodeSingleJSON(method, path, bytes.NewReader(body), parsed); err != nil {
			return apiTypes.Runner{}, err
		}
	}
	return runnerFromGenerated(*parsed), nil
}
