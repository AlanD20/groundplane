package taskplanning

import (
	"context"
	"encoding/hex"
	taskplan "github.com/AlanD20/groundplane/internal/controller/taskplan"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// backingServiceCreationPlan is the extra closed procedure carried by the
// first desired revision of a backing Environment. Health is independently
// optional because custom services have no platform-supplied health check.
type backingServiceCreationPlan struct {
	serviceID       string
	volumeDirectory string
	healthServiceID string
	afterStartID    string
	enabled         bool
	hasHealth       bool
	hasAfterStart   bool
}

func resolveBackingServiceCreationPlan(
	task etcd.TaskRecord,
	procedure taskcontract.BlueprintComposeProcedure,
) (backingServiceCreationPlan, error) {
	serviceID, enabled := task.Params[etcd.TaskBackingServiceCreationParam]
	volumeDirectory, hasVolumeDirectory := task.Params[etcd.TaskBackingServiceVolumeDirectoryParam]
	healthServiceID, hasHealth := task.Params[etcd.TaskBackingServiceHealthParam]
	afterStartID, hasAfterStart := task.Params[etcd.TaskBackingServiceAfterStartParam]
	if enabled != hasVolumeDirectory ||
		enabled && procedure != taskcontract.BlueprintComposeProcedureFullReconcile ||
		enabled && (ids.Validate(ids.KindService, serviceID) != nil || volumeDirectory == "") ||
		hasHealth && (!enabled || healthServiceID != serviceID) ||
		hasAfterStart && (!enabled || afterStartID != serviceID) {
		return backingServiceCreationPlan{}, errs.New(errs.KindInternal, "durable Blueprint Task shape is invalid")
	}
	return backingServiceCreationPlan{
		serviceID: serviceID, volumeDirectory: volumeDirectory,
		healthServiceID: healthServiceID, afterStartID: afterStartID,
		enabled: enabled, hasHealth: hasHealth, hasAfterStart: hasAfterStart,
	}, nil
}

func (plan backingServiceCreationPlan) parameterCount() int {
	count := 0
	if plan.enabled {
		count += 2
	}
	if plan.hasHealth {
		count++
	}
	if plan.hasAfterStart {
		count++
	}
	return count
}

func (plan backingServiceCreationPlan) stepCount() int {
	count := 0
	if plan.enabled {
		count++
	}
	if plan.hasHealth {
		count++
	}
	if plan.hasAfterStart {
		count++
	}
	return count
}

func (resolver *TaskPlanResolver) appendBackingServiceCreationFinalSteps(
	ctx context.Context,
	task etcd.TaskRecord,
	creation backingServiceCreationPlan,
	projection projectionrecord.EnvironmentComposeProjection,
	artifactID string,
	steps []*agentpb.ExecutionStep,
	stepIndex int,
) ([]*agentpb.ExecutionStep, int, error) {
	if creation.hasHealth {
		steps = append(steps, &agentpb.ExecutionStep{
			StepId: task.Steps[stepIndex].ID, TimeoutSeconds: uint32(task.TimeoutSeconds),
			Payload: &agentpb.ExecutionStep_WaitHealthy{WaitHealthy: &agentpb.WaitHealthy{
				ArtifactId: artifactID, ServiceIds: []string{creation.healthServiceID},
			}},
		})
		stepIndex++
	}
	if !creation.hasAfterStart {
		return steps, stepIndex, nil
	}
	configuration, err := backingServiceCreationHookConfiguration(projection, creation.afterStartID)
	if err != nil {
		return nil, 0, err
	}
	steps, err = resolver.appendBackingServiceCreationAfterStart(ctx, task, steps, configuration, nil)
	if err != nil {
		return nil, 0, err
	}
	return steps, stepIndex + 1, nil
}

func (resolver *TaskPlanResolver) PrepareBackingServiceCreationTask(
	ctx context.Context,
	task etcd.TaskRecord,
	artifact *agentpb.ComposeArtifact,
	steps []*agentpb.ExecutionStep,
	configuration *backinghook.Configuration,
	inputs *etcd.BackingHookEncryptedInputs,
) (etcd.TaskRecord, error) {
	preparedSteps, err := resolver.appendBackingServiceCreationAfterStart(
		ctx, task, steps, configuration, inputs,
	)
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	if len(preparedSteps) != 0 {
		defer taskplan.ClearBackingHookProcedure(preparedSteps[len(preparedSteps)-1].GetBackingHookProcedure())
	}
	plan, err := taskplan.Build(taskplan.BuildInput{
		VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID,
		RenderGeneration: uint64(task.RenderGeneration),
		Operation:        agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE, TargetID: task.Target,
		Artifacts: []*agentpb.ComposeArtifact{artifact}, Steps: preparedSteps,
	})
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	task.PlanHash = hex.EncodeToString(plan.PlanHash)
	return task, nil
}

func (resolver *TaskPlanResolver) appendBackingServiceCreationAfterStart(
	ctx context.Context,
	task etcd.TaskRecord,
	steps []*agentpb.ExecutionStep,
	configuration *backinghook.Configuration,
	inputs *etcd.BackingHookEncryptedInputs,
) ([]*agentpb.ExecutionStep, error) {
	wantsHook := configuration != nil && configuration.AfterStart != nil
	if !wantsHook {
		if len(task.Steps) != len(steps) {
			return nil, errs.New(errs.KindInternal, "Backing-service creation Task step count changed")
		}
		return steps, nil
	}
	serviceID, declared := task.Params[etcd.TaskBackingServiceAfterStartParam]
	if !declared || serviceID == "" || len(steps) == 0 || len(task.Steps) != len(steps)+1 {
		return nil, errs.New(errs.KindInternal, "Backing-service after-start Task is incomplete")
	}
	inputResolver, ok := resolver.attachIdentities.(serviceLifecycleHookInputResolver)
	if !ok || inputResolver == nil {
		return nil, errs.New(errs.KindInternal, "Backing-service hook input resolver is not configured")
	}
	consume := func(input backinghook.Input) error {
		hook, buildErr := backingHookStep(
			task.Steps[len(steps)], *configuration.AfterStart, input, nil,
		)
		if buildErr != nil {
			return buildErr
		}
		hook.PrerequisiteStepId = steps[len(steps)-1].StepId
		steps = append(steps, hook)
		return nil
	}
	if inputs != nil {
		if err := inputResolver.ResolveDraftLifecycleHookInput(
			ctx, task, inputs, serviceID, backinghook.AfterStart, consume,
		); err != nil {
			return nil, err
		}
	} else if err := inputResolver.ResolveLifecycleHookInput(
		ctx, task, serviceID, backinghook.AfterStart, consume,
	); err != nil {
		return nil, err
	}
	return steps, nil
}

func backingServiceCreationHookConfiguration(
	projection projectionrecord.EnvironmentComposeProjection,
	serviceID string,
) (*backinghook.Configuration, error) {
	for _, service := range projection.DesiredServices {
		if service.Desired.ID != serviceID {
			continue
		}
		if service.Desired.Adapter != "custom" || service.Desired.Hooks == nil ||
			service.Desired.Hooks.AfterStart == nil {
			return nil, errs.New(errs.KindInternal, "Backing-service after-start configuration changed")
		}
		return backinghook.CloneConfiguration(service.Desired.Hooks), nil
	}
	return nil, errs.New(errs.KindInternal, "Backing-service creation Service is missing")
}
