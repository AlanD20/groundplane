package controller

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"

	"github.com/AlanD20/groundplane/internal/common/ids"
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
	UpdateAgentConfig(context.Context, string, apiTypes.AgentConfig) (apiTypes.AgentConfig, error)
}

type AgentMutator interface {
	EnrollAgent(context.Context, string) (etcd.IdempotencyResponse, error)
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

type agentOutput struct {
	Body apiTypes.Agent
}

type agentConfigOutput struct {
	Body apiTypes.AgentConfig
}

func (s *Server) registerAgentReads() {
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

func (s *Server) agentEnroll(w http.ResponseWriter, r *http.Request) {
	if s.agentMutations == nil {
		s.writeProblem(w, errs.New(errs.KindInternal, "Agent mutation service is not configured"))
		return
	}
	if len(r.URL.Query()) != 0 {
		s.writeAgentProblem(w, errs.New(errs.KindMalformedRequest, "Agent enrollment query is invalid"))
		return
	}
	if err := validateBodylessAgentMutation(r); err != nil {
		s.writeAgentProblem(w, err)
		return
	}
	response, err := s.agentMutations.EnrollAgent(r.Context(), r.Header.Get(idempotencyKeyHeader))
	if err != nil {
		s.writeAgentProblem(w, err)
		return
	}
	s.writeAgentMutationResponse(w, response, "enrollment")
}

func (s *Server) agentRemove(w http.ResponseWriter, r *http.Request) {
	if s.agentMutations == nil {
		s.writeProblem(w, errs.New(errs.KindInternal, "Agent mutation service is not configured"))
		return
	}
	id := r.PathValue("id")
	if err := ids.Validate(ids.KindAgent, id); err != nil {
		s.writeAgentProblem(w, errs.New(errs.KindMalformedRequest, "Agent id is invalid"))
		return
	}
	if len(r.URL.Query()) != 0 {
		s.writeAgentProblem(w, errs.New(errs.KindMalformedRequest, "Agent removal query is invalid"))
		return
	}
	if err := validateBodylessAgentMutation(r); err != nil {
		s.writeAgentProblem(w, err)
		return
	}
	response, err := s.agentMutations.RemoveAgent(r.Context(), id, r.Header.Get(idempotencyKeyHeader))
	if err != nil {
		s.writeAgentProblem(w, err)
		return
	}
	s.writeAgentMutationResponse(w, response, "removal")
}

func validateBodylessAgentMutation(r *http.Request) error {
	if r == nil || r.ContentLength > 0 || len(r.TransferEncoding) != 0 ||
		(r.Body != nil && r.Body != http.NoBody) {
		return errs.New(errs.KindMalformedRequest, "Agent mutation body is not allowed")
	}
	return nil
}

func (s *Server) writeAgentMutationResponse(
	w http.ResponseWriter,
	response etcd.IdempotencyResponse,
	action string,
) {
	w.Header().Set("Content-Type", response.ContentKind)
	w.WriteHeader(response.Status)
	if _, err := w.Write(response.Body); err != nil && s.Logger != nil {
		s.Logger.Error(
			"controller: write Agent mutation response",
			slog.String("action", action),
			slog.Any("error", err),
		)
	}
}

func (s *Server) agentConfigUpdate(w http.ResponseWriter, r *http.Request) {
	if s.agents == nil {
		s.writeProblem(w, errs.New(errs.KindInternal, "Agent reader is not configured"))
		return
	}
	id := r.PathValue("id")
	if err := ids.Validate(ids.KindAgent, id); err != nil {
		s.writeAgentProblem(w, err)
		return
	}
	request, err := decodeAgentConfigRequest(r)
	if err != nil {
		s.writeAgentProblem(w, err)
		return
	}
	updated, err := s.agents.UpdateAgentConfig(r.Context(), id, request)
	if err != nil {
		s.writeAgentProblem(w, err)
		return
	}
	s.writeAgentJSON(w, updated)
}

type agentConfigRequest struct {
	PullIntervalSeconds *int               `json:"pull_interval_seconds"`
	MaxConcurrentTasks  *int               `json:"max_concurrent_tasks"`
	Labels              *map[string]string `json:"labels"`
}

func decodeAgentConfigRequest(r *http.Request) (apiTypes.AgentConfig, error) {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var request agentConfigRequest
	if err := decoder.Decode(&request); err != nil {
		return apiTypes.AgentConfig{}, errs.New(errs.KindMalformedRequest, "Agent config body is malformed")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return apiTypes.AgentConfig{}, errs.New(errs.KindMalformedRequest, "Agent config body has trailing data")
	}
	if request.PullIntervalSeconds == nil || request.MaxConcurrentTasks == nil || request.Labels == nil {
		return apiTypes.AgentConfig{}, errs.New(
			errs.KindValidationFailed,
			"Agent config replacement requires every field",
		)
	}
	if *request.PullIntervalSeconds <= 0 || *request.PullIntervalSeconds > math.MaxInt32 ||
		*request.MaxConcurrentTasks <= 0 || *request.MaxConcurrentTasks > math.MaxInt32 {
		return apiTypes.AgentConfig{}, errs.New(errs.KindValidationFailed, "Agent config limits are invalid")
	}
	labels := make(map[string]string, len(*request.Labels))
	for key, value := range *request.Labels {
		labels[key] = value
	}
	return apiTypes.AgentConfig{
		PullIntervalSeconds: *request.PullIntervalSeconds,
		MaxConcurrentTasks:  *request.MaxConcurrentTasks,
		Labels:              labels,
	}, nil
}

func (s *Server) writeAgentProblem(w http.ResponseWriter, err error) {
	var domainError *errs.Error
	if errors.As(err, &domainError) {
		s.writeProblem(w, domainError)
		return
	}
	s.writeProblem(w, errs.Wrap(errs.KindInternal, err))
}

func (s *Server) writeAgentJSON(w http.ResponseWriter, response any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write Agent response", slog.Any("error", err))
	}
}
