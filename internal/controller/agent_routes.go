package controller

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const maximumAgentPageSize = 200

type AgentReader interface {
	ListAgents(context.Context) ([]apiTypes.Agent, error)
	GetAgent(context.Context, string) (apiTypes.Agent, error)
	GetAgentConfig(context.Context, string) (apiTypes.AgentConfig, error)
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
