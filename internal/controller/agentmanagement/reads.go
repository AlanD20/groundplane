package agentmanagement

import (
	"context"
	"strings"

	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type localAgentHealthReader interface {
	ListHealth(context.Context) ([]localagent.Health, error)
	Health(context.Context, string) (localagent.Health, error)
	UpdateConfig(context.Context, string, localagent.Config, string) ([]byte, error)
}

type ReadService struct {
	health   localAgentHealthReader
	hosts    hostbindings.Reader
	queue    *workqueue.Repository
	hostname string
}

func NewReadService(
	health localAgentHealthReader,
	hosts hostbindings.Reader,
	queue *workqueue.Repository,
	hostname string,
) (*ReadService, error) {
	if health == nil {
		return nil, errs.New(errs.KindInternal, "local Agent health reader is required")
	}
	if hosts == nil || queue == nil {
		return nil, errs.New(errs.KindInternal, "Agent host binding and native capacity readers are required")
	}
	if strings.TrimSpace(hostname) == "" {
		return nil, errs.New(errs.KindInternal, "Controller hostname is required")
	}
	return &ReadService{health: health, hosts: hosts, queue: queue, hostname: hostname}, nil
}

func (service *ReadService) ListAgents(ctx context.Context) ([]apiTypes.Agent, error) {
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

func (service *ReadService) GetAgent(ctx context.Context, id string) (apiTypes.Agent, error) {
	health, err := service.health.Health(ctx, id)
	if err != nil {
		return apiTypes.Agent{}, err
	}
	return service.projectAgent(ctx, health)
}

func (service *ReadService) GetAgentConfig(ctx context.Context, id string) (apiTypes.AgentConfig, error) {
	health, err := service.health.Health(ctx, id)
	if err != nil {
		return apiTypes.AgentConfig{}, err
	}
	return projectAgentConfig(health.Agent.Config), nil
}

func (service *ReadService) UpdateAgentConfig(
	ctx context.Context,
	id string,
	config apiTypes.AgentConfig,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	body, err := service.health.UpdateConfig(ctx, id, localagent.Config{
		PullIntervalSeconds: int32(config.PullIntervalSeconds),
		MaxConcurrentTasks:  int32(config.MaxConcurrentTasks),
		Labels:              copyAgentLabels(config.Labels),
	}, idempotencyKey)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	return idempotencyrecord.IdempotencyResponse{
		Status: 200, ContentKind: "application/json", Body: append([]byte(nil), body...),
	}, nil
}

func (service *ReadService) projectAgent(
	ctx context.Context,
	health localagent.Health,
) (apiTypes.Agent, error) {
	status, err := ProjectStatus(health)
	if err != nil {
		return apiTypes.Agent{}, err
	}
	binding, err := hostbindings.Read(ctx, service.hosts, health.Agent.ID, 0)
	if err != nil {
		return apiTypes.Agent{}, err
	}
	inFlight, err := service.queue.OccupiedSlots(ctx, workexecution.Placement{Executor: workexecution.Agent, HostID: binding.Binding.HostID}, binding.ReadRevision)
	if err != nil {
		return apiTypes.Agent{}, err
	}
	agent := apiTypes.Agent{
		ID:               health.Agent.ID,
		EnrollmentTaskID: health.Agent.EnrollmentTaskID,
		Host:             service.hostname,
		Status:           status,
		Labels:           copyAgentLabels(health.Agent.Config.Labels),
		InFlight:         inFlight,
	}
	if !health.Agent.ReadyAt.IsZero() {
		readyAt := health.Agent.ReadyAt.UTC()
		agent.ReadyAt = &readyAt
	}
	// Replacement records name the candidate before it is acknowledged. Only
	// the ready lifecycle phase is authority for an installed image/version.
	if health.Agent.Phase == localagent.PhaseReady {
		image := health.Agent.Image
		agent.InstalledImage = &image
		if health.Agent.Build.Version != "" {
			version := health.Agent.Build.Version
			agent.Version = &version
		}
		if health.Agent.Build.Source.Known() {
			source := health.Agent.Build.Source
			agent.Source = &source
		}
	}
	if !health.LastReady.IsZero() {
		lastReportAt := health.LastReady.UTC()
		agent.LastReportAt = &lastReportAt
	}
	return agent, nil
}

func ProjectStatus(health localagent.Health) (apiTypes.AgentStatus, error) {
	switch health.Agent.Phase {
	case localagent.PhaseProvisioning, localagent.PhaseUpdating:
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
