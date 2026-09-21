package dispatch

import (
	"context"
	"errors"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/agentmanagement"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type controllerTaskLocalAgents interface {
	Enroll(context.Context, localagent.EnrollRequest) (localagent.Agent, error)
	Reconcile(context.Context) error
	Health(context.Context, string) (localagent.Health, error)
	Remove(context.Context, string) error
}

type ResourceHandler struct {
	agents          controllerTaskLocalAgents
	backingZones    backingZoneCascadeExecutor
	runners         controllerTaskRunners
	runnerLifecycle controllerTaskRunnerLifecycle
}

type backingZoneCascadeExecutor interface {
	Execute(context.Context, etcd.TaskRecord) error
}

type controllerTaskRunners interface {
	GetRunner(context.Context, string) (etcdstore.Versioned[runnerrecord.RunnerRecord], error)
}

type controllerTaskRunnerLifecycle interface {
	ExecuteCreate(context.Context, etcd.TaskRecord) error
	ExecuteRemove(context.Context, etcd.TaskRecord) error
}

func NewResourceHandler(
	agents controllerTaskLocalAgents,
	backingZones backingZoneCascadeExecutor,
	runners controllerTaskRunners,
	runnerLifecycle ...controllerTaskRunnerLifecycle,
) (*ResourceHandler, error) {
	if agents == nil || backingZones == nil || runners == nil {
		return nil, errs.New(errs.KindInternal, "Controller Task handlers are not configured")
	}
	var lifecycle controllerTaskRunnerLifecycle
	if len(runnerLifecycle) > 1 {
		return nil, errs.New(errs.KindInternal, "Controller Task Runner lifecycle is ambiguous")
	}
	if len(runnerLifecycle) == 1 {
		lifecycle = runnerLifecycle[0]
	}
	return &ResourceHandler{
		agents: agents, backingZones: backingZones, runners: runners, runnerLifecycle: lifecycle,
	}, nil
}

func (handler *ResourceHandler) Execute(
	ctx context.Context,
	task etcd.TaskRecord,
) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "Controller Task context is required")
	}
	if task.Executor != taskjournal.TaskExecutorController || ids.Validate(ids.KindTask, task.ID) != nil {
		return errs.New(errs.KindValidationFailed, "Controller Task identity is invalid")
	}
	switch task.Params[taskjournal.TaskResourceKindParam] {
	case taskjournal.TaskResourceAgent:
		if ids.Validate(ids.KindAgent, task.Target) != nil {
			return errs.New(errs.KindValidationFailed, "Controller Task Agent target is invalid")
		}
	case taskjournal.TaskResourceSecret:
		if task.Type != taskjournal.TaskRemove || ids.Validate(ids.KindSecret, task.Target) != nil ||
			len(task.Params) != 1 {
			return errs.New(errs.KindValidationFailed, "Controller Task Secret removal is invalid")
		}
		return nil
	case taskjournal.TaskResourceConnector:
		if task.Type != taskjournal.TaskRemove || ids.Validate(ids.KindConnector, task.Target) != nil ||
			len(task.Params) != 3 ||
			ids.Validate(ids.KindEnvironment, task.Params[etcd.TaskConnectorEnvironmentParam]) != nil ||
			task.Params[etcd.TaskConnectorNameParam] == "" {
			return errs.New(errs.KindValidationFailed, "controller Task Connector removal is invalid")
		}
		return nil
	case taskjournal.TaskResourceScript:
		if task.Type != taskjournal.TaskRemove || ids.Validate(ids.KindScript, task.Target) != nil ||
			len(task.Params) != 1 {
			return errs.New(errs.KindValidationFailed, "Controller Task Script removal is invalid")
		}
		return nil
	case taskjournal.TaskResourceEntry:
		if task.Type != taskjournal.TaskRemove || ids.Validate(ids.KindEnvEntry, task.Target) != nil ||
			len(task.Params) != 2 ||
			ids.Validate(ids.KindEnvironment, task.Params[taskjournal.TaskEntryEnvironmentParam]) != nil {
			return errs.New(errs.KindValidationFailed, "Controller Task Entry removal is invalid")
		}
		return nil
	case taskjournal.TaskResourceRoute:
		if (task.Type != taskjournal.TaskCreate && task.Type != taskjournal.TaskUpdate && task.Type != taskjournal.TaskRemove) ||
			ids.Validate(ids.KindRoute, task.Target) != nil ||
			len(task.Params) != 2 ||
			ids.Validate(ids.KindEnvironment, task.Params[taskjournal.TaskRouteEnvironmentParam]) != nil {
			return errs.New(errs.KindValidationFailed, "Controller Task Route mutation is invalid")
		}
		return nil
	case taskjournal.TaskResourceService:
		if (task.Type != taskjournal.TaskStart && task.Type != taskjournal.TaskStop && task.Type != taskjournal.TaskDestroy) ||
			ids.Validate(ids.KindService, task.Target) != nil ||
			len(task.Params) != 2 ||
			ids.Validate(ids.KindEnvironment, task.Params[taskjournal.TaskServiceEnvironmentParam]) != nil {
			return errs.New(errs.KindValidationFailed, "Controller Task Service lifecycle is invalid")
		}
		return nil
	case taskjournal.TaskResourceReleaseGroup:
		if (task.Type != taskjournal.TaskCreate && task.Type != taskjournal.TaskUpdate && task.Type != taskjournal.TaskRemove) ||
			ids.Validate(ids.KindReleaseGroup, task.Target) != nil ||
			len(task.Params) != 1 {
			return errs.New(errs.KindValidationFailed, "controller Task release group mutation is invalid")
		}
		return nil
	case taskjournal.TaskResourceBackingZone:
		return handler.backingZones.Execute(ctx, task)
	case runnerrecord.TaskResourceRunner:
		if task.Type == taskjournal.TaskCreate {
			if handler.runnerLifecycle == nil {
				return errs.New(errs.KindInternal, "Controller Task Runner lifecycle is not configured")
			}
			return handler.runnerLifecycle.ExecuteCreate(ctx, task)
		}
		if task.Type == taskjournal.TaskRemove && handler.runnerLifecycle != nil {
			return handler.runnerLifecycle.ExecuteRemove(ctx, task)
		}
		return handler.executeRunnerRemoval(ctx, task)
	default:
		return errs.New(errs.KindValidationFailed, "Controller Task resource kind is invalid")
	}
	switch task.Type {
	case taskjournal.TaskCreate:
		return handler.executeEnrollment(ctx, task)
	case taskjournal.TaskRemove:
		return handler.executeRemoval(ctx, task)
	case taskjournal.TaskUpdate:
		return errs.New(errs.KindInternal, "agent update requires the recovery-aware native executor")
	default:
		return errs.New(errs.KindValidationFailed, "Controller Task Agent mutation type is invalid")
	}
}

func (handler *ResourceHandler) executeRunnerRemoval(ctx context.Context, task etcd.TaskRecord) error {
	if task.Type != taskjournal.TaskRemove || ids.Validate(ids.KindRunner, task.Target) != nil ||
		len(task.Params) != 6 {
		return errs.New(errs.KindValidationFailed, "Controller Task Runner removal is invalid")
	}
	current, err := handler.runners.GetRunner(ctx, task.Target)
	if err != nil {
		return err
	}
	if current.Record.ProvisioningState != runnerrecord.RunnerProvisioningFailed || current.Record.ContainerID != "" {
		return errs.New(errs.KindStateConflict, "Runner host cleanup requires the Controller lifecycle executor")
	}
	return nil
}

func (handler *ResourceHandler) executeEnrollment(
	ctx context.Context,
	task etcd.TaskRecord,
) error {
	request, err := agentmanagement.DecodeEnrollmentTask(task)
	if err != nil {
		return err
	}
	health, err := handler.agents.Health(ctx, request.AgentID)
	if errors.Is(err, errs.New(errs.KindAgentNotFound, "")) {
		_, enrollErr := handler.agents.Enroll(ctx, request)
		return enrollErr
	}
	if err != nil {
		return err
	}
	if health.Agent.EnrollmentTaskID != request.EnrollmentTaskID {
		return errs.New(errs.KindStateConflict, "local Agent belongs to another enrollment Task")
	}
	if health.Agent.Phase == localagent.PhaseDeleting {
		return errs.New(errs.KindStateConflict, "deleting local Agent cannot resume enrollment")
	}
	if err := handler.agents.Reconcile(ctx); err != nil {
		return err
	}
	health, err = handler.agents.Health(ctx, request.AgentID)
	if err != nil {
		return err
	}
	if health.Agent.EnrollmentTaskID != request.EnrollmentTaskID ||
		health.Agent.Phase != localagent.PhaseReady {
		return errs.New(errs.KindStateConflict, "local Agent enrollment did not reach Ready")
	}
	return nil
}

func (handler *ResourceHandler) executeRemoval(
	ctx context.Context,
	task etcd.TaskRecord,
) error {
	if len(task.Params) != 1 || task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceAgent {
		return errs.New(errs.KindValidationFailed, "Agent removal Task parameters are invalid")
	}
	return handler.agents.Remove(ctx, task.Target)
}
