package dispatch

import (
	"context"
	"errors"
	"github.com/AlanD20/groundplane/internal/controller/controllertask"
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
	agents controllerTaskLocalAgents
}

func NewResourceHandler(
	agents controllerTaskLocalAgents,
) (*ResourceHandler, error) {
	if agents == nil {
		return nil, errs.New(errs.KindInternal, "Controller Task handlers are not configured")
	}
	return &ResourceHandler{
		agents: agents,
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
	case taskjournal.TaskResourceAgent:
		if ids.Validate(ids.KindAgent, task.Target) != nil {
			return errs.New(errs.KindValidationFailed, "Controller Task Agent target is invalid")
		}
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
