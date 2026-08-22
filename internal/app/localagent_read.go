package app

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type localAgentHealthReader interface {
	ListHealth(context.Context) ([]localagent.Health, error)
	Health(context.Context, string) (localagent.Health, error)
	UpdateConfig(context.Context, string, localagent.Config) (localagent.Config, error)
}

type localAgentAssignmentReader interface {
	ListAgentAssignments(context.Context, string, uint64, int32) ([]etcd.TaskAssignment, error)
}

type localAgentReadService struct {
	health      localAgentHealthReader
	assignments localAgentAssignmentReader
	hostname    string
}

func newLocalAgentReadService(
	health localAgentHealthReader,
	assignments localAgentAssignmentReader,
	hostname string,
) (*localAgentReadService, error) {
	if health == nil {
		return nil, errs.New(errs.KindInternal, "local Agent health reader is required")
	}
	if assignments == nil {
		return nil, errs.New(errs.KindInternal, "local Agent assignment reader is required")
	}
	if strings.TrimSpace(hostname) == "" {
		return nil, errs.New(errs.KindInternal, "Controller hostname is required")
	}
	return &localAgentReadService{health: health, assignments: assignments, hostname: hostname}, nil
}

func (service *localAgentReadService) ListAgents(ctx context.Context) ([]apiTypes.Agent, error) {
	health, err := service.health.ListHealth(ctx)
	if err != nil {
		return nil, err
	}
	agents := make([]apiTypes.Agent, len(health))
	for index, current := range health {
		agent, err := service.projectAgent(ctx, current)
		if err != nil {
			return nil, err
		}
		agents[index] = agent
	}
	return agents, nil
}

func (service *localAgentReadService) GetAgent(ctx context.Context, id string) (apiTypes.Agent, error) {
	health, err := service.health.Health(ctx, id)
	if err != nil {
		return apiTypes.Agent{}, err
	}
	return service.projectAgent(ctx, health)
}

func (service *localAgentReadService) GetAgentConfig(ctx context.Context, id string) (apiTypes.AgentConfig, error) {
	health, err := service.health.Health(ctx, id)
	if err != nil {
		return apiTypes.AgentConfig{}, err
	}
	return projectAgentConfig(health.Agent.Config), nil
}

func (service *localAgentReadService) UpdateAgentConfig(
	ctx context.Context,
	id string,
	config apiTypes.AgentConfig,
) (apiTypes.AgentConfig, error) {
	updated, err := service.health.UpdateConfig(ctx, id, localagent.Config{
		PullIntervalSeconds: int32(config.PullIntervalSeconds),
		MaxConcurrentTasks:  int32(config.MaxConcurrentTasks),
		Labels:              copyAgentLabels(config.Labels),
	})
	if err != nil {
		return apiTypes.AgentConfig{}, err
	}
	return projectAgentConfig(updated), nil
}

func (service *localAgentReadService) projectAgent(
	ctx context.Context,
	health localagent.Health,
) (apiTypes.Agent, error) {
	status, err := projectAgentStatus(health)
	if err != nil {
		return apiTypes.Agent{}, err
	}
	assignments, err := service.assignments.ListAgentAssignments(
		ctx,
		health.Agent.ID,
		health.Agent.Generation,
		health.Agent.Config.MaxConcurrentTasks,
	)
	if err != nil {
		return apiTypes.Agent{}, err
	}
	agent := apiTypes.Agent{
		ID:               health.Agent.ID,
		EnrollmentTaskID: health.Agent.EnrollmentTaskID,
		Host:             service.hostname,
		Status:           status,
		Labels:           copyAgentLabels(health.Agent.Config.Labels),
		InFlight:         len(assignments),
	}
	if !health.Agent.ReadyAt.IsZero() {
		readyAt := health.Agent.ReadyAt.UTC()
		agent.ReadyAt = &readyAt
	}
	if health.Version != "" {
		version := health.Version
		agent.Version = &version
	}
	if !health.LastReady.IsZero() {
		lastReportAt := health.LastReady.UTC()
		agent.LastReportAt = &lastReportAt
	}
	return agent, nil
}

func projectAgentStatus(health localagent.Health) (apiTypes.AgentStatus, error) {
	switch health.Agent.Phase {
	case localagent.PhaseProvisioning:
		return apiTypes.AgentPending, nil
	case localagent.PhaseReady:
		if health.Healthy {
			return apiTypes.AgentHealthy, nil
		}
		return apiTypes.AgentDegraded, nil
	case localagent.PhaseDeleting:
		return apiTypes.AgentStopped, nil
	default:
		return "", errs.New(errs.KindInternal, "local Agent has an invalid lifecycle phase")
	}
}

func projectAgentConfig(config localagent.Config) apiTypes.AgentConfig {
	return apiTypes.AgentConfig{
		PullIntervalSeconds: int(config.PullIntervalSeconds),
		MaxConcurrentTasks:  int(config.MaxConcurrentTasks),
		Labels:              copyAgentLabels(config.Labels),
	}
}

func copyAgentLabels(labels map[string]string) map[string]string {
	result := make(map[string]string, len(labels))
	for key, value := range labels {
		result[key] = value
	}
	return result
}
