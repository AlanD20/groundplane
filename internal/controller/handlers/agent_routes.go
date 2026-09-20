package handlers

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"strconv"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

const maximumAgentPageSize = 200

type AgentReader interface {
	ListAgents(context.Context) ([]apiTypes.Agent, error)
	GetAgent(context.Context, string) (apiTypes.Agent, error)
	GetAgentConfig(context.Context, string) (apiTypes.AgentConfig, error)
	UpdateAgentConfig(context.Context, string, apiTypes.AgentConfig, string) (etcd.IdempotencyResponse, error)
}

type AgentMutator interface {
	EnrollAgent(context.Context, string) (etcd.IdempotencyResponse, error)
	UpdateAgent(context.Context, string, string, string) (etcd.IdempotencyResponse, error)
	RemoveAgent(context.Context, string, string) (etcd.IdempotencyResponse, error)
}

type agentListInput struct {
	Limit  int    `query:"limit"  required:"false" minimum:"1" maximum:"200"`
	Cursor string `query:"cursor" required:"false"`
}

type agentPageOutput struct {
	Body apiTypes.Page[apiTypes.Agent]
}

type agentIDInput struct {
	ID string `path:"id" pattern:"^agt_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type agentEnrollInput struct {
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
}

type agentRemoveInput struct {
	ID             string `path:"id" pattern:"^agt_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
}

type agentUpdateInput struct {
	ID             string `path:"id" pattern:"^agt_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body           apiTypes.AgentUpdate
}

type agentConfigReplacement struct {
	PullIntervalSeconds int               `json:"pull_interval_seconds" minimum:"1" maximum:"2147483647"`
	MaxConcurrentTasks  int               `json:"max_concurrent_tasks"  minimum:"1" maximum:"2147483647"`
	Labels              map[string]string `json:"labels"`
}

type agentConfigUpdateInput struct {
	ID             string `path:"id" pattern:"^agt_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body           agentConfigReplacement
}

type agentOutput struct {
	Body apiTypes.Agent
}

type agentConfigOutput struct {
	Body apiTypes.AgentConfig
}

type agentMutationOutput struct {
	Status      int
	ContentType string `header:"Content-Type"`
	Body        func(huma.Context)
}

func (s *Server) registerAgents() {
	taskAcceptedSchema := s.API.OpenAPI().Components.Schemas.Schema(
		reflect.TypeFor[apiTypes.TaskAccepted](),
		true,
		"TaskAccepted",
	)
	agentConfigSchema := s.API.OpenAPI().Components.Schemas.Schema(
		reflect.TypeFor[apiTypes.AgentConfig](),
		true,
		"AgentConfig",
	)
	huma.Register(s.API, huma.Operation{
		OperationID: "agent.join", Method: http.MethodPost, Path: "/agents",
		Summary: "Create and start the local Agent", Tags: []string{"Agent"}, DefaultStatus: http.StatusAccepted,
		Middlewares: huma.Middlewares{s.rejectAgentMutationBody, s.rejectAgentQuery},
		Responses:   attachMutationResponses(taskAcceptedSchema),
	}, s.enrollAgent)
	huma.Register(s.API, huma.Operation{
		OperationID: "agent.list", Method: http.MethodGet, Path: "/agents",
		Summary: "List agents", Tags: []string{"Agent"},
		Middlewares: huma.Middlewares{s.validateAgentListQuery},
	}, s.listAgents)
	huma.Register(s.API, huma.Operation{
		OperationID: "agent.show", Method: http.MethodGet, Path: "/agents/{id}",
		Summary: "Show an agent", Tags: []string{"Agent"},
	}, s.showAgent)
	huma.Register(s.API, huma.Operation{
		OperationID: "agent.config.show", Method: http.MethodGet, Path: "/agents/{id}/config",
		Summary: "Show agent config", Tags: []string{"Agent"},
	}, s.showAgentConfig)
	huma.Register(s.API, huma.Operation{
		OperationID: "agent.config.set", Method: http.MethodPut, Path: "/agents/{id}/config",
		Summary: "Replace agent config", Tags: []string{"Agent"}, DefaultStatus: http.StatusOK,
		Middlewares: huma.Middlewares{s.rejectAgentQuery},
		Responses: map[string]*huma.Response{
			strconv.Itoa(http.StatusOK): {
				Description: http.StatusText(http.StatusOK),
				Content: map[string]*huma.MediaType{
					"application/json": {Schema: agentConfigSchema},
				},
			},
		},
	}, s.replaceAgentConfig)
	huma.Register(s.API, huma.Operation{
		OperationID: "agent.update", Method: http.MethodPost, Path: "/agents/{id}/update",
		Summary: "Update the local Agent", Tags: []string{"Agent"}, DefaultStatus: http.StatusAccepted,
		Middlewares: huma.Middlewares{s.rejectAgentQuery},
		Responses:   attachMutationResponses(taskAcceptedSchema),
	}, s.updateAgent)
	huma.Register(s.API, huma.Operation{
		OperationID: "agent.remove", Method: http.MethodDelete, Path: "/agents/{id}",
		Summary: "Remove the local Agent", Tags: []string{"Agent"}, DefaultStatus: http.StatusAccepted,
		Middlewares: huma.Middlewares{s.rejectAgentMutationBody, s.rejectAgentQuery},
		Responses:   attachMutationResponses(taskAcceptedSchema),
	}, s.removeAgent)
	s.setRoutePolicy("PUT /api/v1/agents/{id}/config", routePolicy{body: jsonBody})
	s.setRoutePolicy("POST /api/v1/agents/{id}/update", routePolicy{body: jsonBody})
}

func (s *Server) listAgents(ctx context.Context, request *agentListInput) (*agentPageOutput, error) {
	if s.agents == nil {
		return nil, errs.New(errs.KindInternal, "Agent reader is not configured")
	}
	if request.Cursor != "" {
		return nil, errs.New(errs.KindMalformedRequest, "Agent pagination cursor is invalid")
	}
	limit := request.Limit
	if limit == 0 {
		limit = maximumAgentPageSize
	}
	agents, err := s.agents.ListAgents(ctx)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	if len(agents) > limit {
		agents = agents[:limit]
	}
	return &agentPageOutput{Body: apiTypes.Page[apiTypes.Agent]{Items: agents}}, nil
}

func (s *Server) showAgent(ctx context.Context, request *agentIDInput) (*agentOutput, error) {
	if s.agents == nil {
		return nil, errs.New(errs.KindInternal, "Agent reader is not configured")
	}
	agent, err := s.agents.GetAgent(ctx, request.ID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &agentOutput{Body: agent}, nil
}

func (s *Server) showAgentConfig(ctx context.Context, request *agentIDInput) (*agentConfigOutput, error) {
	if s.agents == nil {
		return nil, errs.New(errs.KindInternal, "Agent reader is not configured")
	}
	config, err := s.agents.GetAgentConfig(ctx, request.ID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &agentConfigOutput{Body: config}, nil
}

func (s *Server) enrollAgent(ctx context.Context, request *agentEnrollInput) (*agentMutationOutput, error) {
	if s.agentMutations == nil {
		return nil, errs.New(errs.KindInternal, "Agent mutation service is not configured")
	}
	response, err := s.agentMutations.EnrollAgent(ctx, request.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return s.agentMutationResponse(response, "enrollment"), nil
}

func (s *Server) removeAgent(ctx context.Context, request *agentRemoveInput) (*agentMutationOutput, error) {
	if s.agentMutations == nil {
		return nil, errs.New(errs.KindInternal, "Agent mutation service is not configured")
	}
	response, err := s.agentMutations.RemoveAgent(ctx, request.ID, request.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return s.agentMutationResponse(response, "removal"), nil
}

func (s *Server) updateAgent(ctx context.Context, request *agentUpdateInput) (*agentMutationOutput, error) {
	if s.agentMutations == nil {
		return nil, errs.New(errs.KindInternal, "Agent mutation service is not configured")
	}
	response, err := s.agentMutations.UpdateAgent(ctx, request.ID, request.Body.Image, request.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return s.agentMutationResponse(response, "update"), nil
}

func (s *Server) replaceAgentConfig(
	ctx context.Context,
	request *agentConfigUpdateInput,
) (*agentMutationOutput, error) {
	if s.agents == nil {
		return nil, errs.New(errs.KindInternal, "Agent reader is not configured")
	}
	labels := make(map[string]string, len(request.Body.Labels))
	for key, value := range request.Body.Labels {
		labels[key] = value
	}
	response, err := s.agents.UpdateAgentConfig(ctx, request.ID, apiTypes.AgentConfig{
		PullIntervalSeconds: request.Body.PullIntervalSeconds,
		MaxConcurrentTasks:  request.Body.MaxConcurrentTasks,
		Labels:              labels,
	}, request.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return s.agentMutationResponse(response, "config replacement"), nil
}

func (s *Server) agentMutationResponse(response etcd.IdempotencyResponse, action string) *agentMutationOutput {
	return &agentMutationOutput{
		Status: response.Status, ContentType: response.ContentKind,
		Body: func(ctx huma.Context) {
			ctx.SetStatus(response.Status)
			if _, err := ctx.BodyWriter().Write(response.Body); err != nil && s.Logger != nil {
				s.Logger.Error(
					"controller: write Agent mutation response",
					slog.String("action", action),
					slog.Any("error", err),
				)
			}
		},
	}
}

func (s *Server) validateAgentListQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	for key, values := range requestURL.Query() {
		if key != "limit" && key != "cursor" {
			s.writeAgentRequestProblem(ctx, "Agent list query is invalid")
			return
		}
		if len(values) != 1 {
			s.writeAgentRequestProblem(ctx, "Agent list query contains duplicate values")
			return
		}
	}
	next(ctx)
}

func (s *Server) writeAgentRequestProblem(ctx huma.Context, detail string) {
	if err := huma.WriteErr(s.API, ctx, http.StatusBadRequest, detail); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write Agent request problem", slog.Any("error", err))
	}
}

func (s *Server) rejectAgentMutationBody(ctx huma.Context, next func(huma.Context)) {
	var probe [1]byte
	count, err := ctx.BodyReader().Read(probe[:])
	if count != 0 || (err != nil && !errors.Is(err, io.EOF)) {
		s.writeAgentRequestProblem(ctx, "Agent mutation body is not allowed")
		return
	}
	next(ctx)
}

func (s *Server) rejectAgentQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	if len(requestURL.Query()) != 0 {
		s.writeAgentRequestProblem(ctx, "Agent request query is invalid")
		return
	}
	next(ctx)
}
