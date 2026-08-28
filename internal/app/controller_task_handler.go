package app

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	agentTaskImageKey         = "image"
	agentTaskPullIntervalKey  = "pull_interval_seconds"
	agentTaskMaxConcurrentKey = "max_concurrent_tasks"
	agentTaskLabelPrefix      = "label:"
)

type controllerTaskLocalAgents interface {
	Enroll(context.Context, localagent.EnrollRequest) (localagent.Agent, error)
	Reconcile(context.Context) error
	Update(context.Context, localagent.UpdateRequest) error
	Health(context.Context, string) (localagent.Health, error)
	Remove(context.Context, string) error
}

type controllerTaskHandler struct {
	agents          controllerTaskLocalAgents
	backingZones    backingZoneCascadeExecutor
	runners         controllerTaskRunners
	runnerLifecycle controllerTaskRunnerLifecycle
}

type backingZoneCascadeExecutor interface {
	Execute(context.Context, etcd.TaskRecord) error
}

type controllerTaskRunners interface {
	GetRunner(context.Context, string) (etcd.Versioned[etcd.RunnerRecord], error)
}

type controllerTaskRunnerLifecycle interface {
	ExecuteCreate(context.Context, etcd.TaskRecord) error
	ExecuteRemove(context.Context, etcd.TaskRecord) error
}

func newControllerTaskHandler(
	agents controllerTaskLocalAgents,
	backingZones backingZoneCascadeExecutor,
	runners controllerTaskRunners,
	runnerLifecycle ...controllerTaskRunnerLifecycle,
) (*controllerTaskHandler, error) {
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
	return &controllerTaskHandler{
		agents: agents, backingZones: backingZones, runners: runners, runnerLifecycle: lifecycle,
	}, nil
}

func (handler *controllerTaskHandler) Execute(
	ctx context.Context,
	task etcd.TaskRecord,
) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "Controller Task context is required")
	}
	if task.Executor != etcd.TaskExecutorController || ids.Validate(ids.KindTask, task.ID) != nil {
		return errs.New(errs.KindValidationFailed, "Controller Task identity is invalid")
	}
	switch task.Params[etcd.TaskResourceKindParam] {
	case etcd.TaskResourceAgent:
		if ids.Validate(ids.KindAgent, task.Target) != nil {
			return errs.New(errs.KindValidationFailed, "Controller Task Agent target is invalid")
		}
	case etcd.TaskResourceSecret:
		if task.Type != etcd.TaskRemove || ids.Validate(ids.KindSecret, task.Target) != nil || len(task.Params) != 1 {
			return errs.New(errs.KindValidationFailed, "Controller Task Secret removal is invalid")
		}
		return nil
	case etcd.TaskResourceConnector:
		if task.Type != etcd.TaskRemove || ids.Validate(ids.KindConnector, task.Target) != nil ||
			len(task.Params) != 3 ||
			ids.Validate(ids.KindEnvironment, task.Params[etcd.TaskConnectorEnvironmentParam]) != nil ||
			task.Params[etcd.TaskConnectorNameParam] == "" {
			return errs.New(errs.KindValidationFailed, "controller Task Connector removal is invalid")
		}
		return nil
	case etcd.TaskResourceScript:
		if task.Type != etcd.TaskRemove || ids.Validate(ids.KindScript, task.Target) != nil || len(task.Params) != 1 {
			return errs.New(errs.KindValidationFailed, "Controller Task Script removal is invalid")
		}
		return nil
	case etcd.TaskResourceEntry:
		if task.Type != etcd.TaskRemove || ids.Validate(ids.KindEnvEntry, task.Target) != nil ||
			len(task.Params) != 2 ||
			ids.Validate(ids.KindEnvironment, task.Params[etcd.TaskEntryEnvironmentParam]) != nil {
			return errs.New(errs.KindValidationFailed, "Controller Task Entry removal is invalid")
		}
		return nil
	case etcd.TaskResourceRoute:
		if task.Type != etcd.TaskRemove || ids.Validate(ids.KindRoute, task.Target) != nil || len(task.Params) != 2 ||
			ids.Validate(ids.KindEnvironment, task.Params[etcd.TaskRouteEnvironmentParam]) != nil {
			return errs.New(errs.KindValidationFailed, "Controller Task Route removal is invalid")
		}
		return nil
	case etcd.TaskResourceService:
		if (task.Type != etcd.TaskStart && task.Type != etcd.TaskStop && task.Type != etcd.TaskDestroy) ||
			ids.Validate(ids.KindService, task.Target) != nil || len(task.Params) != 2 ||
			ids.Validate(ids.KindEnvironment, task.Params[etcd.TaskServiceEnvironmentParam]) != nil {
			return errs.New(errs.KindValidationFailed, "Controller Task Service lifecycle is invalid")
		}
		return nil
	case etcd.TaskResourceReleaseGroup:
		if (task.Type != etcd.TaskCreate && task.Type != etcd.TaskUpdate && task.Type != etcd.TaskRemove) ||
			ids.Validate(ids.KindReleaseGroup, task.Target) != nil || len(task.Params) != 1 {
			return errs.New(errs.KindValidationFailed, "controller Task release group mutation is invalid")
		}
		return nil
	case etcd.TaskResourceBackingZone:
		return handler.backingZones.Execute(ctx, task)
	case etcd.TaskResourceRunner:
		if task.Type == etcd.TaskCreate {
			if handler.runnerLifecycle == nil {
				return errs.New(errs.KindInternal, "Controller Task Runner lifecycle is not configured")
			}
			return handler.runnerLifecycle.ExecuteCreate(ctx, task)
		}
		if task.Type == etcd.TaskRemove && handler.runnerLifecycle != nil {
			return handler.runnerLifecycle.ExecuteRemove(ctx, task)
		}
		return handler.executeRunnerRemoval(ctx, task)
	default:
		return errs.New(errs.KindValidationFailed, "Controller Task resource kind is invalid")
	}
	switch task.Type {
	case etcd.TaskCreate:
		return handler.executeEnrollment(ctx, task)
	case etcd.TaskRemove:
		return handler.executeRemoval(ctx, task)
	case etcd.TaskUpdate:
		return handler.executeUpdate(ctx, task)
	default:
		return errs.New(errs.KindValidationFailed, "Controller Task Agent mutation type is invalid")
	}
}

func (handler *controllerTaskHandler) executeRunnerRemoval(ctx context.Context, task etcd.TaskRecord) error {
	if task.Type != etcd.TaskRemove || ids.Validate(ids.KindRunner, task.Target) != nil || len(task.Params) != 6 {
		return errs.New(errs.KindValidationFailed, "Controller Task Runner removal is invalid")
	}
	current, err := handler.runners.GetRunner(ctx, task.Target)
	if err != nil {
		return err
	}
	if current.Record.ProvisioningState != etcd.RunnerProvisioningFailed || current.Record.ContainerID != "" {
		return errs.New(errs.KindStateConflict, "Runner host cleanup requires the Controller lifecycle executor")
	}
	return nil
}

func (handler *controllerTaskHandler) executeUpdate(
	ctx context.Context,
	task etcd.TaskRecord,
) error {
	request, err := decodeAgentUpdateTask(task)
	if err != nil {
		return err
	}
	return handler.agents.Update(ctx, request)
}

func decodeAgentUpdateTask(task etcd.TaskRecord) (localagent.UpdateRequest, error) {
	if len(task.Params) != 4 || task.Params[etcd.TaskResourceKindParam] != etcd.TaskResourceAgent {
		return localagent.UpdateRequest{}, invalidAgentUpdateTask()
	}
	previousImage, hasPrevious := task.Params[agentTaskPreviousImageKey]
	desiredImage, hasDesired := task.Params[agentTaskImageKey]
	generationText, hasGeneration := task.Params[agentTaskStartingGenerationKey]
	if !hasPrevious || !hasDesired || !hasGeneration ||
		!imageref.IsDigestPinned(previousImage) || !imageref.IsDigestPinned(desiredImage) ||
		previousImage == desiredImage {
		return localagent.UpdateRequest{}, invalidAgentUpdateTask()
	}
	generation, err := strconv.ParseUint(generationText, 10, 64)
	if err != nil || generation == 0 || generation == ^uint64(0) ||
		strconv.FormatUint(generation, 10) != generationText {
		return localagent.UpdateRequest{}, invalidAgentUpdateTask()
	}
	return localagent.UpdateRequest{
		AgentID: task.Target, PreviousImage: previousImage, DesiredImage: desiredImage,
		StartingGeneration: generation,
	}, nil
}

func invalidAgentUpdateTask() error {
	return errs.New(errs.KindValidationFailed, "Agent update Task parameters are invalid")
}

func (handler *controllerTaskHandler) executeEnrollment(
	ctx context.Context,
	task etcd.TaskRecord,
) error {
	request, err := decodeAgentEnrollmentTask(task)
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
	if health.Agent.EnrollmentTaskID != task.ID {
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
	if health.Agent.EnrollmentTaskID != task.ID || health.Agent.Phase != localagent.PhaseReady {
		return errs.New(errs.KindStateConflict, "local Agent enrollment did not reach Ready")
	}
	return nil
}

func (handler *controllerTaskHandler) executeRemoval(
	ctx context.Context,
	task etcd.TaskRecord,
) error {
	if len(task.Params) != 1 || task.Params[etcd.TaskResourceKindParam] != etcd.TaskResourceAgent {
		return errs.New(errs.KindValidationFailed, "Agent removal Task parameters are invalid")
	}
	return handler.agents.Remove(ctx, task.Target)
}

func decodeAgentEnrollmentTask(task etcd.TaskRecord) (localagent.EnrollRequest, error) {
	request := localagent.EnrollRequest{
		AgentID: task.Target, EnrollmentTaskID: task.ID,
		Config: localagent.Config{Labels: map[string]string{}},
	}
	required := map[string]bool{
		etcd.TaskResourceKindParam: false, agentTaskImageKey: false,
		agentTaskPullIntervalKey: false, agentTaskMaxConcurrentKey: false,
	}
	for key, value := range task.Params {
		switch key {
		case etcd.TaskResourceKindParam:
			if value != etcd.TaskResourceAgent {
				return localagent.EnrollRequest{}, invalidAgentEnrollmentTask()
			}
			required[key] = true
		case agentTaskImageKey:
			request.Image = value
			required[key] = true
		case agentTaskPullIntervalKey:
			parsed, err := parseCanonicalPositiveInt32(value)
			if err != nil {
				return localagent.EnrollRequest{}, invalidAgentEnrollmentTask()
			}
			request.Config.PullIntervalSeconds = parsed
			required[key] = true
		case agentTaskMaxConcurrentKey:
			parsed, err := parseCanonicalPositiveInt32(value)
			if err != nil {
				return localagent.EnrollRequest{}, invalidAgentEnrollmentTask()
			}
			request.Config.MaxConcurrentTasks = parsed
			required[key] = true
		default:
			if !strings.HasPrefix(key, agentTaskLabelPrefix) {
				return localagent.EnrollRequest{}, invalidAgentEnrollmentTask()
			}
			request.Config.Labels[strings.TrimPrefix(key, agentTaskLabelPrefix)] = value
		}
	}
	for _, present := range required {
		if !present {
			return localagent.EnrollRequest{}, invalidAgentEnrollmentTask()
		}
	}
	return request, nil
}

func agentEnrollmentTaskParams(image string, config localagent.Config) map[string]string {
	params := map[string]string{
		etcd.TaskResourceKindParam: etcd.TaskResourceAgent,
		agentTaskImageKey:          image,
		agentTaskPullIntervalKey:   strconv.FormatInt(int64(config.PullIntervalSeconds), 10),
		agentTaskMaxConcurrentKey:  strconv.FormatInt(int64(config.MaxConcurrentTasks), 10),
	}
	for key, value := range config.Labels {
		params[agentTaskLabelPrefix+key] = value
	}
	return params
}

func parseCanonicalPositiveInt32(value string) (int32, error) {
	parsed, err := strconv.ParseInt(value, 10, 32)
	if err != nil || parsed <= 0 || strconv.FormatInt(parsed, 10) != value {
		return 0, errs.New(errs.KindValidationFailed, "positive canonical int32 is required")
	}
	return int32(parsed), nil
}

func invalidAgentEnrollmentTask() error {
	return errs.New(errs.KindValidationFailed, "Agent enrollment Task parameters are invalid")
}
