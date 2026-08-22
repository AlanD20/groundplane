package apiclient

import (
	"bytes"
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func (c *Client) ShowTask(ctx context.Context, id string) (apiTypes.Task, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Task{}, err
	}
	path := "/api/v1/tasks/" + id
	response, err := client.TaskShowWithResponse(ctx, id)
	if err != nil {
		return apiTypes.Task{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(
		http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.Task{}, err
	}
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.Task{}
		if err := decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), parsed); err != nil {
			return apiTypes.Task{}, err
		}
	}
	return taskFromGenerated(*parsed), nil
}

func taskFromGenerated(task generated.Task) apiTypes.Task {
	result := apiTypes.Task{
		ID: task.Id, OperationID: task.OperationId, Type: task.Type, Target: task.Target,
		Status: apiTypes.TaskStatus(task.Status),
	}
	if task.RetryOf != nil {
		result.RetryOf = *task.RetryOf
	}
	if task.PlanHash != nil {
		result.PlanHash = *task.PlanHash
	}
	if task.Steps != nil {
		result.Steps = make([]apiTypes.TaskStep, len(*task.Steps))
		for index, step := range *task.Steps {
			result.Steps[index] = apiTypes.TaskStep{
				Name: step.Name, Status: apiTypes.TaskStatus(step.Status),
			}
		}
	}
	return result
}
