package apiclient

import (
	"bytes"
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func (c *Client) ListAgents(ctx context.Context, limit int, cursor string) (apiTypes.Page[apiTypes.Agent], error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Page[apiTypes.Agent]{}, err
	}
	params := &generated.AgentListParams{}
	if limit != 0 {
		value := int64(limit)
		params.Limit = &value
	}
	if cursor != "" {
		params.Cursor = &cursor
	}
	response, err := client.AgentListWithResponse(ctx, params)
	if err != nil {
		return apiTypes.Page[apiTypes.Agent]{}, generatedCallError(ctx, http.MethodGet, "/api/v1/agents", err)
	}
	if err := generatedResponseError(
		http.MethodGet, "/api/v1/agents", response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.Page[apiTypes.Agent]{}, err
	}
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.PageAgent{}
		if err := decodeSingleJSON(
			http.MethodGet,
			"/api/v1/agents",
			bytes.NewReader(response.Body),
			parsed,
		); err != nil {
			return apiTypes.Page[apiTypes.Agent]{}, err
		}
	}
	items := []generated.Agent(nil)
	if parsed.Items != nil {
		items = *parsed.Items
	}
	page := apiTypes.Page[apiTypes.Agent]{Items: make([]apiTypes.Agent, len(items))}
	if parsed.NextCursor != nil {
		page.NextCursor = *parsed.NextCursor
	}
	for index, agent := range items {
		page.Items[index] = agentFromGenerated(agent)
	}
	return page, nil
}

func (c *Client) ShowAgent(ctx context.Context, id string) (apiTypes.Agent, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Agent{}, err
	}
	path := "/api/v1/agents/" + id
	response, err := client.AgentShowWithResponse(ctx, id)
	if err != nil {
		return apiTypes.Agent{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(
		http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.Agent{}, err
	}
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.Agent{}
		if err := decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), parsed); err != nil {
			return apiTypes.Agent{}, err
		}
	}
	return agentFromGenerated(*parsed), nil
}

func (c *Client) ShowAgentConfig(ctx context.Context, id string) (apiTypes.AgentConfig, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.AgentConfig{}, err
	}
	path := "/api/v1/agents/" + id + "/config"
	response, err := client.AgentConfigShowWithResponse(ctx, id)
	if err != nil {
		return apiTypes.AgentConfig{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(
		http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.AgentConfig{}, err
	}
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.AgentConfig{}
		if err := decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), parsed); err != nil {
			return apiTypes.AgentConfig{}, err
		}
	}
	return apiTypes.AgentConfig{
		PullIntervalSeconds: int(parsed.PullIntervalSeconds),
		MaxConcurrentTasks:  int(parsed.MaxConcurrentTasks),
		Labels:              copyGeneratedAgentLabels(parsed.Labels),
	}, nil
}

func agentFromGenerated(agent generated.Agent) apiTypes.Agent {
	return apiTypes.Agent{
		ID:               agent.Id,
		EnrollmentTaskID: agent.EnrollmentTaskId,
		Host:             agent.Host,
		Status:           apiTypes.AgentStatus(agent.Status),
		Version:          agent.Version,
		Labels:           copyGeneratedAgentLabels(agent.Labels),
		ReadyAt:          agent.ReadyAt,
		LastReportAt:     agent.LastReportAt,
		InFlight:         int(agent.InFlight),
	}
}

func copyGeneratedAgentLabels(labels map[string]string) map[string]string {
	result := make(map[string]string, len(labels))
	for key, value := range labels {
		result[key] = value
	}
	return result
}
