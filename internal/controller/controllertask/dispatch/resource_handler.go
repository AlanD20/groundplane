package dispatch

import (
	"context"
	"errors"
	"github.com/AlanD20/groundplane/internal/controller/controllertask"
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
	runnerLifecycle controllerTaskRunnerLifecycle
	runnerChecks    Executor
	images          Executor
	connectorChecks Executor
	backingChecks   Executor
	dnsChecks       Executor
	edgeChecks      Executor
}

type backingZoneCascadeExecutor interface {
	Execute(context.Context, etcd.TaskRecord) error
}

type controllerTaskRunnerLifecycle interface {
	ExecuteCreate(context.Context, etcd.TaskRecord) error
	ExecuteRemove(context.Context, etcd.TaskRecord) error
}

func NewResourceHandler(
	agents controllerTaskLocalAgents,
	backingZones backingZoneCascadeExecutor,
	images Executor,
	connectorChecks Executor,
	backingChecks Executor,
	dnsChecks Executor,
	edgeChecks Executor,
	runnerChecks Executor,
	runnerLifecycle controllerTaskRunnerLifecycle,
) (*ResourceHandler, error) {
	if agents == nil || backingZones == nil || images == nil || connectorChecks == nil || backingChecks == nil ||
		dnsChecks == nil ||
		edgeChecks == nil ||
		runnerChecks == nil ||
		runnerLifecycle == nil {
		return nil, errs.New(errs.KindInternal, "Controller Task handlers are not configured")
	}
	return &ResourceHandler{
		agents: agents, backingZones: backingZones, runnerLifecycle: runnerLifecycle, runnerChecks: runnerChecks, images: images,
		connectorChecks: connectorChecks,
		backingChecks:   backingChecks,
		dnsChecks:       dnsChecks,
		edgeChecks:      edgeChecks,
	}, nil
}

func (handler *ResourceHandler) Execute(
	ctx context.Context,
	task etcd.TaskRecord,
	progress *controllertask.Progress,
) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "Controller Task context is required")
	}
	if task.Executor != taskjournal.TaskExecutorController || ids.Validate(ids.KindTask, task.ID) != nil {
		return errs.New(errs.KindValidationFailed, "Controller Task identity is invalid")
	}
	switch task.Params[taskjournal.TaskResourceKindParam] {
	case taskjournal.TaskResourceComponent:
		if task.Type != taskjournal.TaskCheck {
			return errs.New(errs.KindValidationFailed, "Controller Component Task must be a DNS connection check")
		}
		if task.Params[taskjournal.TaskEdgeCheckInputParam] != "" {
			return observeExecution(ctx, task, progress, "connection_check", handler.edgeChecks)
		}
		return observeExecution(ctx, task, progress, "connection_check", handler.dnsChecks)
	case taskjournal.TaskResourceAttach:
		return observeExecution(ctx, task, progress, "connection_check", handler.backingChecks)
	case taskjournal.TaskResourceImage:
		stage := "image_fetch"
		if task.Type == taskjournal.TaskRemove {
			stage = "image_remove"
		}
		return observeExecution(ctx, task, progress, stage, handler.images)
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
		if task.Type == taskjournal.TaskCheck {
			return observeExecution(ctx, task, progress, "connection_check", handler.connectorChecks)
		}
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
		if task.Type == taskjournal.TaskCheck {
			return observeExecution(ctx, task, progress, "connection_check", handler.edgeChecks)
		}
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
		return observeExecution(ctx, task, progress, "backing_zone_remove", handler.backingZones)
	case runnerrecord.TaskResourceRunner:
		if task.Type == taskjournal.TaskCheck {
			return observeExecution(ctx, task, progress, "connection_check", handler.runnerChecks)
		}
		if task.Type == taskjournal.TaskCreate {
			return progress.Run(ctx, "runner_create", func(ctx context.Context) error {
				return handler.runnerLifecycle.ExecuteCreate(ctx, task)
			})
		}
		if task.Type == taskjournal.TaskRemove {
			return progress.Run(ctx, "runner_remove", func(ctx context.Context) error {
				return handler.runnerLifecycle.ExecuteRemove(ctx, task)
			})
		}
		return errs.New(errs.KindValidationFailed, "Controller Runner Task type is invalid")
	default:
		return errs.New(errs.KindValidationFailed, "Controller Task resource kind is invalid")
	}
	switch task.Type {
	case taskjournal.TaskCreate:
		return handler.executeEnrollment(ctx, task, progress)
	case taskjournal.TaskRemove:
		return progress.Run(ctx, "agent_remove", func(ctx context.Context) error {
			return handler.executeRemoval(ctx, task)
		})
	case taskjournal.TaskUpdate:
		return errs.New(errs.KindInternal, "agent update requires the recovery-aware native executor")
	default:
		return errs.New(errs.KindValidationFailed, "Controller Task Agent mutation type is invalid")
	}
}

func (handler *ResourceHandler) executeEnrollment(
	ctx context.Context,
	task etcd.TaskRecord,
	progress *controllertask.Progress,
) error {
	request, err := agentmanagement.DecodeEnrollmentTask(task)
	if err != nil {
		return err
	}
	var health localagent.Health
	var missing bool
	err = progress.Run(ctx, "agent_inspect", func(ctx context.Context) error {
		var inspectErr error
		health, inspectErr = handler.agents.Health(ctx, request.AgentID)
		missing = errors.Is(inspectErr, errs.New(errs.KindAgentNotFound, ""))
		if missing {
			return nil
		}
		return inspectErr
	})
	if err != nil {
		return err
	}
	if missing {
		return progress.Run(ctx, "agent_enroll", func(ctx context.Context) error {
			_, err := handler.agents.Enroll(ctx, request)
			return err
		})
	}
	if health.Agent.EnrollmentTaskID != request.EnrollmentTaskID {
		return errs.New(errs.KindStateConflict, "local Agent belongs to another enrollment Task")
	}
	if health.Agent.Phase == localagent.PhaseDeleting {
		return errs.New(errs.KindStateConflict, "deleting local Agent cannot resume enrollment")
	}
	return progress.Run(ctx, "agent_reconcile", func(ctx context.Context) error {
		if err := handler.agents.Reconcile(ctx); err != nil {
			return err
		}
		health, err = handler.agents.Health(ctx, request.AgentID)
		if err != nil {
			return err
		}
		if health.Agent.EnrollmentTaskID != request.EnrollmentTaskID || health.Agent.Phase != localagent.PhaseReady {
			return errs.New(errs.KindStateConflict, "local Agent enrollment did not reach Ready")
		}
		return nil
	})
}

func observeExecution(ctx context.Context, task etcd.TaskRecord, progress *controllertask.Progress,
	stage string, executor Executor,
) error {
	return progress.Run(ctx, stage, func(ctx context.Context) error { return executor.Execute(ctx, task) })
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
