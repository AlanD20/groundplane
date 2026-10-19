package apiclient

import (
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

type TaskListOptions struct {
	ResourceKind string
	Scope        string
	Limit        int64
	After        string
	Revision     int64
	Action       string
	Status       string
}

type TaskDetailOptions struct {
	Limit    int64
	After    string
	Revision int64
}

func (c *Client) TaskAttemptEvents(
	ctx context.Context,
	taskID, workID, attemptID string,
	after uint64,
	revision int64,
) (apiTypes.TaskAttemptEvents, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAttemptEvents{}, err
	}
	response, err := client.TaskAttemptEvents(
		ctx,
		taskID,
		workID,
		attemptID,
		&generated.TaskAttemptEventsParams{After: &after, Revision: &revision},
	)
	return readTaskResponse[apiTypes.TaskAttemptEvents](
		ctx,
		"/api/v1/tasks/"+taskID+"/work/"+workID+"/attempts/"+attemptID+"/events",
		response,
		err,
	)
}

func (c *Client) TaskStages(
	ctx context.Context,
	taskID, workID, attemptID string,
	revision int64,
) (apiTypes.TaskStages, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskStages{}, err
	}
	response, err := client.TaskStages(ctx, taskID, workID, attemptID, &generated.TaskStagesParams{Revision: &revision})
	return readTaskResponse[apiTypes.TaskStages](
		ctx,
		"/api/v1/tasks/"+taskID+"/work/"+workID+"/attempts/"+attemptID+"/stages",
		response,
		err,
	)
}

func (c *Client) ListTasks(
	ctx context.Context,
	options TaskListOptions,
) (apiTypes.ExecutionPage[apiTypes.TaskSummary], error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.ExecutionPage[apiTypes.TaskSummary]{}, err
	}
	params := taskListParams(options)
	response, err := client.TaskList(ctx, params)
	return readTaskResponse[apiTypes.ExecutionPage[apiTypes.TaskSummary]](ctx, "/api/v1/tasks", response, err)
}

func (c *Client) ListActivity(
	ctx context.Context,
	options TaskListOptions,
) (apiTypes.ExecutionPage[apiTypes.TaskSummary], error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.ExecutionPage[apiTypes.TaskSummary]{}, err
	}
	base := taskListParams(options)
	response, err := client.ActivityList(ctx, &generated.ActivityListParams{Scope: base.Scope, Limit: base.Limit,
		After: base.After, Revision: base.Revision, Action: base.Action, Status: base.Status, ResourceKind: base.ResourceKind})
	return readTaskResponse[apiTypes.ExecutionPage[apiTypes.TaskSummary]](ctx, "/api/v1/activity", response, err)
}

func taskListParams(options TaskListOptions) *generated.TaskListParams {
	if options.Scope == "" {
		options.Scope = "all"
	}
	if options.Limit == 0 {
		options.Limit = 25
	}
	return &generated.TaskListParams{Scope: &options.Scope, Limit: &options.Limit,
		After: &options.After, Revision: &options.Revision, Action: &options.Action, Status: &options.Status, ResourceKind: &options.ResourceKind}
}

func (c *Client) ShowTask(ctx context.Context, id string, options TaskDetailOptions) (apiTypes.TaskDetails, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskDetails{}, err
	}
	if options.Limit == 0 {
		options.Limit = 25
	}
	response, err := client.TaskShow(
		ctx,
		id,
		&generated.TaskShowParams{Limit: &options.Limit, After: &options.After, Revision: &options.Revision},
	)
	return readTaskResponse[apiTypes.TaskDetails](ctx, "/api/v1/tasks/"+id, response, err)
}

func (c *Client) TaskAttempts(
	ctx context.Context,
	taskID, workID string,
	after uint64,
	limit, revision int64,
) (apiTypes.TaskAttemptPage, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskAttemptPage{}, err
	}
	if limit == 0 {
		limit = 25
	}
	response, err := client.TaskAttempts(
		ctx,
		taskID,
		workID,
		&generated.TaskAttemptsParams{After: &after, Limit: &limit, Revision: &revision},
	)
	return readTaskResponse[apiTypes.TaskAttemptPage](
		ctx,
		"/api/v1/tasks/"+taskID+"/work/"+workID+"/attempts",
		response,
		err,
	)
}

func (c *Client) TaskImage(
	ctx context.Context,
	taskID, workID string,
	revision int64,
) (apiTypes.TaskImageExecution, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.TaskImageExecution{}, err
	}
	response, err := client.TaskImage(ctx, taskID, workID, &generated.TaskImageParams{Revision: &revision})
	return readTaskResponse[apiTypes.TaskImageExecution](
		ctx,
		"/api/v1/tasks/"+taskID+"/work/"+workID+"/image",
		response,
		err,
	)
}

// Decode the wire body once into the public API model. Generated requests own
// paths and parameters; there is no conversion through the retired Task model.
func readTaskResponse[T any](ctx context.Context, path string, response *http.Response, callErr error) (T, error) {
	var result T
	if callErr != nil {
		return result, generatedCallError(ctx, http.MethodGet, path, callErr)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return result, responseProblem(http.MethodGet, path, response)
	}
	err := decodeSingleJSON(http.MethodGet, path, response.Body, &result)
	return result, err
}
