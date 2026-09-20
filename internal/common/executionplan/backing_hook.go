package executionplan

import (
	"bytes"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
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

func ValidateBackingHookCheckpointRequest(
	request *agentpb.BackingHookCheckpointRequest,
) (*agentpb.BackingHookCheckpointRequest, error) {
	if request == nil || ids.Validate(ids.KindTask, request.GetTaskId()) != nil ||
		ids.Validate(ids.KindOperation, request.GetOperationId()) != nil ||
		ids.Validate(ids.KindAssignment, request.GetAssignmentId()) != nil || request.GetExecutionEpoch() == 0 ||
		ids.Validate(ids.KindStep, request.GetStepId()) != nil || len(request.GetPlanHash()) != sha256.Size {
		return nil, errs.New(errs.KindValidationFailed, "backing hook checkpoint identity is invalid")
	}
	if _, ok := BackingHookEvent(request.GetEvent()); !ok {
		return nil, errs.New(errs.KindValidationFailed, "backing hook checkpoint event is invalid")
	}
	owned := proto.Clone(request).(*agentpb.BackingHookCheckpointRequest)
	switch owned.GetState() {
	case agentpb.BackingHookCheckpointState_BACKING_HOOK_CHECKPOINT_STATE_STARTED:
		if len(owned.GetResultSha256()) != 0 || len(owned.GetFacts()) != 0 {
			return nil, errs.New(errs.KindValidationFailed, "backing hook STARTED checkpoint carries a result")
		}
	case agentpb.BackingHookCheckpointState_BACKING_HOOK_CHECKPOINT_STATE_RESULT:
		digest, err := ComputeBackingHookResultDigest(owned.GetEvent(), owned.GetFacts())
		if err != nil || !bytes.Equal(digest, owned.GetResultSha256()) {
			return nil, errs.New(errs.KindValidationFailed, "backing hook RESULT checkpoint digest is invalid")
		}
	default:
		return nil, errs.New(errs.KindValidationFailed, "backing hook checkpoint state is invalid")
	}
	return owned, nil
}

func ComputeBackingHookResultDigest(
	event agentpb.BackingHookEvent,
	facts []*agentpb.BackingHookValue,
) ([]byte, error) {
	if _, ok := BackingHookEvent(event); !ok {
		return nil, errs.New(errs.KindValidationFailed, "backing hook result event is invalid")
	}
	seen := make(map[string]struct{}, len(facts))
	for _, fact := range facts {
		if fact == nil || !backinghook.ValidKey(fact.GetKey()) || bytes.IndexByte(fact.GetValue(), 0) >= 0 ||
			bytes.IndexByte(fact.GetValue(), '\n') >= 0 {
			return nil, errs.New(errs.KindValidationFailed, "backing hook result fact is invalid")
		}
		if _, duplicate := seen[fact.GetKey()]; duplicate {
			return nil, errs.New(errs.KindValidationFailed, "backing hook result fact is duplicated")
		}
		seen[fact.GetKey()] = struct{}{}
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&agentpb.BackingHookCheckpointRequest{
		Event: event, State: agentpb.BackingHookCheckpointState_BACKING_HOOK_CHECKPOINT_STATE_RESULT,
		Facts: facts,
	})
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(encoded)
	clear(encoded)
	return append([]byte(nil), digest[:]...), nil
}

func ValidateBackingHookCheckpointAck(
	ack *agentpb.BackingHookCheckpointAck,
	request *agentpb.BackingHookCheckpointRequest,
) error {
	if ack == nil || request == nil || ack.GetTaskId() != request.GetTaskId() ||
		ack.GetOperationId() != request.GetOperationId() || ack.GetAssignmentId() != request.GetAssignmentId() ||
		ack.GetExecutionEpoch() != request.GetExecutionEpoch() || ack.GetStepId() != request.GetStepId() ||
		!bytes.Equal(ack.GetPlanHash(), request.GetPlanHash()) || ack.GetEvent() != request.GetEvent() ||
		ack.GetState() != request.GetState() || !bytes.Equal(ack.GetResultSha256(), request.GetResultSha256()) {
		return errs.New(errs.KindStateConflict, "backing hook checkpoint acknowledgement does not match")
	}
	return nil
}
