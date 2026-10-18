package taskplanning

import (
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

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
