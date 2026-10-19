package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func validateBackingHookProcedure(
	operation agentpb.PlanOperation,
	procedure *agentpb.BackingHookProcedure,
) error {
	definition, input, schema, err := DecodeBackingHookProcedure(procedure)
	if err != nil {
		return err
	}
	if input.Context.Event == backinghook.Attach && operation != agentpb.PlanOperation_PLAN_OPERATION_ATTACH &&
		operation != agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY ||
		input.Context.Event == backinghook.Detach && operation != agentpb.PlanOperation_PLAN_OPERATION_DETACH ||
		input.Context.Event == backinghook.BeforeStop && operation != agentpb.PlanOperation_PLAN_OPERATION_STOP &&
			operation != agentpb.PlanOperation_PLAN_OPERATION_DESTROY ||
		input.Context.Event == backinghook.AfterStart && operation != agentpb.PlanOperation_PLAN_OPERATION_START &&
			operation != agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE {
		return errs.New(errs.KindValidationFailed, "backing hook event does not match the plan operation")
	}
	return backinghook.Validate(definition, input, schema)
}

func DecodeBackingHookProcedure(
	procedure *agentpb.BackingHookProcedure,
) (backinghook.Definition, backinghook.Input, []backinghook.FactDefinition, error) {
	if procedure == nil {
		return backinghook.Definition{}, backinghook.Input{}, nil,
			errs.New(errs.KindValidationFailed, "backing hook procedure is missing")
	}
	event, ok := BackingHookEvent(procedure.GetEvent())
	if !ok {
		return backinghook.Definition{}, backinghook.Input{}, nil,
			errs.New(errs.KindValidationFailed, "backing hook event is invalid")
	}
	input := backinghook.Input{Context: backinghook.Context{
		Event: event, BackingServiceID: procedure.GetBackingServiceId(), AttachID: procedure.GetAttachId(),
		TenantID: procedure.GetTenantId(), ProjectID: procedure.GetProjectId(),
		EnvironmentID: procedure.GetEnvironmentId(), ServiceID: procedure.GetServiceId(),
	}}
	for _, value := range procedure.GetInputs() {
		if value == nil {
			return backinghook.Definition{}, backinghook.Input{}, nil,
				errs.New(errs.KindValidationFailed, "backing hook input is missing")
		}
		input.Values = append(input.Values, backinghook.Value{Key: value.GetKey(), Value: value.GetValue()})
	}
	for _, value := range procedure.GetFacts() {
		if value == nil {
			return backinghook.Definition{}, backinghook.Input{}, nil,
				errs.New(errs.KindValidationFailed, "backing hook fact is missing")
		}
		input.Facts = append(input.Facts, backinghook.Value{Key: value.GetKey(), Value: value.GetValue()})
	}
	schema := make([]backinghook.FactDefinition, 0, len(procedure.GetFactSchema()))
	for _, definition := range procedure.GetFactSchema() {
		if definition == nil {
			return backinghook.Definition{}, backinghook.Input{}, nil,
				errs.New(errs.KindValidationFailed, "backing hook fact schema is missing")
		}
		schema = append(schema, backinghook.FactDefinition{Key: definition.GetKey(), Secret: definition.GetSecret()})
	}
	if event != backinghook.Attach && len(schema) != 0 {
		return backinghook.Definition{}, backinghook.Input{}, nil,
			errs.New(errs.KindValidationFailed, "non-Attach backing hook cannot publish facts")
	}
	return backinghook.Definition{
		Command: append([]string(nil), procedure.GetCommand()...), TimeoutSeconds: procedure.GetTimeoutSeconds(),
	}, input, schema, nil
}

func BackingHookEvent(event agentpb.BackingHookEvent) (backinghook.Event, bool) {
	switch event {
	case agentpb.BackingHookEvent_BACKING_HOOK_EVENT_ATTACH:
		return backinghook.Attach, true
	case agentpb.BackingHookEvent_BACKING_HOOK_EVENT_DETACH:
		return backinghook.Detach, true
	case agentpb.BackingHookEvent_BACKING_HOOK_EVENT_BEFORE_STOP:
		return backinghook.BeforeStop, true
	case agentpb.BackingHookEvent_BACKING_HOOK_EVENT_AFTER_START:
		return backinghook.AfterStart, true
	default:
		return "", false
	}
}
