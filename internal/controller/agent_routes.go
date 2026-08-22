package controller

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
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
		s.Logger.Error("controller: write Agent mutation response", slog.String("action", action), slog.Any("error", err))
	}
}

func (s *Server) agentList(w http.ResponseWriter, r *http.Request) {
	if s.agents == nil {
		s.writeProblem(w, errs.New(errs.KindInternal, "Agent reader is not configured"))
		return
	}
	limit, err := agentPageLimit(r)
	if err != nil {
		s.writeAgentProblem(w, err)
		return
	}
	agents, err := s.agents.ListAgents(r.Context())
	if err != nil {
		s.writeAgentProblem(w, err)
		return
	}
	if len(agents) > limit {
		agents = agents[:limit]
	}
	s.writeAgentJSON(w, apiTypes.Page[apiTypes.Agent]{Items: agents})
}

func (s *Server) agentShow(w http.ResponseWriter, r *http.Request) {
	if s.agents == nil {
		s.writeProblem(w, errs.New(errs.KindInternal, "Agent reader is not configured"))
		return
	}
	id := r.PathValue("id")
	if err := ids.Validate(ids.KindAgent, id); err != nil {
		s.writeAgentProblem(w, err)
		return
	}
	agent, err := s.agents.GetAgent(r.Context(), id)
	if err != nil {
		s.writeAgentProblem(w, err)
		return
	}
	s.writeAgentJSON(w, agent)
}

func (s *Server) agentConfigShow(w http.ResponseWriter, r *http.Request) {
	if s.agents == nil {
		s.writeProblem(w, errs.New(errs.KindInternal, "Agent reader is not configured"))
		return
	}
	id := r.PathValue("id")
	if err := ids.Validate(ids.KindAgent, id); err != nil {
		s.writeAgentProblem(w, err)
		return
	}
	config, err := s.agents.GetAgentConfig(r.Context(), id)
	if err != nil {
		s.writeAgentProblem(w, err)
		return
	}
	s.writeAgentJSON(w, config)
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
		return apiTypes.AgentConfig{}, errs.New(errs.KindValidationFailed, "Agent config replacement requires every field")
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

func agentPageLimit(r *http.Request) (int, error) {
	query := r.URL.Query()
	for name := range query {
		if name != "limit" && name != "cursor" {
			return 0, errs.New(errs.KindMalformedRequest, "Agent list query is invalid")
		}
	}
	if len(query["limit"]) > 1 || len(query["cursor"]) > 1 || query.Get("cursor") != "" {
		return 0, errs.New(errs.KindMalformedRequest, "Agent pagination query is invalid")
	}
	if raw := query.Get("limit"); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > maximumAgentPageSize {
			return 0, errs.New(errs.KindMalformedRequest, "Agent pagination limit is invalid")
		}
		return limit, nil
	}
	return maximumAgentPageSize, nil
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
