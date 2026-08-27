package apiclient

import (
	"bytes"
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
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
		ID: runner.Id, TenantID: runner.TenantId, ProjectID: projectID,
		Labels: labels, Online: runner.Online,
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
