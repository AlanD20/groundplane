package taskplanning

import (
	"context"
	taskplan "github.com/AlanD20/groundplane/internal/controller/taskplan"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	attachrender "github.com/AlanD20/groundplane/internal/infra/etcd/attachrender"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (resolver *TaskPlanResolver) resolveCustomAttachPlan(
	ctx context.Context,
	task etcd.TaskRecord,
	current etcdstore.Versioned[attachrecord.Record],
	renderInput attachrender.AttachTaskRenderInput,
	operation agentpb.PlanOperation,
	artifact *agentpb.ComposeArtifact,
	applyRuntime bool,
) (*agentpb.ExecutionPlan, error) {
	var event backinghook.Event
	var definition *backinghook.Definition
	if task.Type == taskjournal.TaskAttach {
		event = backinghook.Attach
		if renderInput.HookConfiguration != nil {
			definition = renderInput.HookConfiguration.Attach
		}
	} else {
		event = backinghook.Detach
		if renderInput.HookConfiguration != nil {
			definition = renderInput.HookConfiguration.Detach
		}
	}
	if !current.Record.OwnsCredential() || definition == nil {
		if len(task.Steps) != 1 {
			return nil, errs.New(errs.KindInternal, "network-only Custom Attach Task step count is invalid")
		}
		return taskplan.Build(taskplan.BuildInput{
			VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID,
			RenderGeneration: uint64(task.RenderGeneration), Operation: operation,
			TargetID: task.Target, Artifacts: []*agentpb.ComposeArtifact{artifact},
			Steps: []*agentpb.ExecutionStep{
				attachComposeStep(task, 0, artifact, attachRuntimeServiceIDs(current.Record.ServiceID, applyRuntime)),
			},
		})
	}
	if !current.Record.HookBundle || len(task.Steps) != 2 {
		return nil, errs.New(errs.KindInternal, "hooked Custom Attach Task is incomplete")
	}
	hookContext := backinghook.Context{
		Event: event, BackingServiceID: current.Record.BackingServiceID, AttachID: current.Record.ID,
		TenantID: renderInput.TenantID, ProjectID: renderInput.ProjectID,
		EnvironmentID: current.Record.EnvironmentID, ServiceID: current.Record.ServiceID,
	}
	var plan *agentpb.ExecutionPlan
	err := resolver.attachIdentities.ResolveHookInput(
		ctx,
		current,
		task,
		hookContext,
		func(input backinghook.Input) error {
			hookStep, buildErr := backingHookStep(
				task.Steps[0],
				*definition,
				input,
				renderInput.HookConfiguration.Facts,
			)
			if buildErr != nil {
				return buildErr
			}
			defer taskplan.ClearBackingHookProcedure(hookStep.GetBackingHookProcedure())
			plan, buildErr = taskplan.Build(taskplan.BuildInput{
				VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID,
				RenderGeneration: uint64(task.RenderGeneration), Operation: operation,
				TargetID: task.Target, Artifacts: []*agentpb.ComposeArtifact{artifact},
				Steps: []*agentpb.ExecutionStep{
					hookStep,
					attachComposeStep(
						task,
						1,
						artifact,
						attachRuntimeServiceIDs(current.Record.ServiceID, applyRuntime),
					),
				},
			})
			return buildErr
		},
	)
	return plan, err
}

func backingHookStep(
	step taskjournal.TaskStepRecord,
	definition backinghook.Definition,
	input backinghook.Input,
	schema []backinghook.FactDefinition,
) (*agentpb.ExecutionStep, error) {
	event, ok := encodeBackingHookEvent(input.Context.Event)
	if !ok {
		return nil, errs.New(errs.KindInternal, "backing hook event is invalid")
	}
	procedure := &agentpb.BackingHookProcedure{
		Event: event, Command: append([]string(nil), definition.Command...),
		TimeoutSeconds: definition.TimeoutSeconds, BackingServiceId: input.Context.BackingServiceID,
		AttachId: input.Context.AttachID, TenantId: input.Context.TenantID, ProjectId: input.Context.ProjectID,
		EnvironmentId: input.Context.EnvironmentID, ServiceId: input.Context.ServiceID,
	}
	for _, value := range input.Values {
		procedure.Inputs = append(procedure.Inputs, &agentpb.BackingHookValue{
			Key: value.Key, Value: append([]byte(nil), value.Value...),
		})
	}
	for _, value := range input.Facts {
		procedure.Facts = append(procedure.Facts, &agentpb.BackingHookValue{
			Key: value.Key, Value: append([]byte(nil), value.Value...),
		})
	}
	if input.Context.Event == backinghook.Attach {
		for _, value := range schema {
			procedure.FactSchema = append(procedure.FactSchema, &agentpb.BackingHookFactDefinition{
				Key: value.Key, Secret: value.Secret,
			})
		}
	}
	return &agentpb.ExecutionStep{
		StepId: step.ID, TimeoutSeconds: uint32(stepTimeoutSeconds(definition.TimeoutSeconds)),
		Payload: &agentpb.ExecutionStep_BackingHookProcedure{BackingHookProcedure: procedure},
	}, nil
}

func stepTimeoutSeconds(hookTimeout uint32) uint32 {
	return hookTimeout + 15
}

func encodeBackingHookEvent(event backinghook.Event) (agentpb.BackingHookEvent, bool) {
	switch event {
	case backinghook.Attach:
		return agentpb.BackingHookEvent_BACKING_HOOK_EVENT_ATTACH, true
	case backinghook.Detach:
		return agentpb.BackingHookEvent_BACKING_HOOK_EVENT_DETACH, true
	case backinghook.BeforeStop:
		return agentpb.BackingHookEvent_BACKING_HOOK_EVENT_BEFORE_STOP, true
	case backinghook.AfterStart:
		return agentpb.BackingHookEvent_BACKING_HOOK_EVENT_AFTER_START, true
	default:
		return 0, false
	}
}
