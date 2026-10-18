package executionplan

import (
	"encoding/hex"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// LifecycleHelperInput binds one host effect to its captured procedure and
// acknowledged boundary. The launcher authenticates the envelope's origin.
type LifecycleHelperInput struct {
	Recovery    *lifecycleexecution.Recovery
	Attempt     workexecution.Attempt
	Procedure   *agentpb.ServiceLifecycleWorkInput
	Progress    lifecycleexecution.Progress
	Stage       lifecycleexecution.Stage
	InspectOnly bool
}

func OpenLifecycleHelperRequest(request *agentpb.ServiceLifecycleHelperRequest) (LifecycleHelperInput, error) {
	if request != nil && request.Assignment != nil && len(request.Assignment.RecoverySource) != 0 {
		return openLifecycleRecoveryHelper(request)
	}
	if request == nil || proto.Size(request) > MaximumPlanBytes || RejectUnknown(request) != nil || request.Assignment == nil ||
		request.Assignment.Attempt == nil || len(request.Assignment.RecoverySource) != 0 || request.Report == nil || request.Acknowledgement == nil {
		return LifecycleHelperInput{}, invalidLifecycleWork()
	}
	claimed := request.Assignment.Attempt
	identity := workexecution.Identity{Executor: workexecution.Agent, HostID: claimed.HostId, AgentID: claimed.AgentId, Generation: claimed.ExecutorGeneration}
	attempt, err := workexecution.DecodeAgentAttempt(claimed, identity)
	if err != nil || attempt.Direction != workexecution.Forward {
		return LifecycleHelperInput{}, invalidLifecycleWork()
	}
	procedure := request.Assignment.Input
	if ValidateServiceLifecycleWorkInput(procedure) != nil || procedure.CapturedSource.InputId != attempt.Input.ID || hex.EncodeToString(procedure.CapturedSource.Sha256) != attempt.Input.SHA256 {
		return LifecycleHelperInput{}, invalidLifecycleWork()
	}
	started, _, err := workexecution.DecodeAgentStartAck(request.Start, attempt)
	if err != nil {
		return LifecycleHelperInput{}, err
	}
	attempt.StartedAt = &started
	reported, err := workexecution.DecodeAgentAttempt(request.Report.Attempt, identity)
	if err != nil || !workexecution.SameAssignment(attempt, reported) {
		return LifecycleHelperInput{}, invalidLifecycleWork()
	}
	progress, err := lifecycleexecution.DecodeProgress(attempt, procedure, request.Report.Progress)
	if err != nil || progress.BeganAt == nil {
		return LifecycleHelperInput{}, invalidLifecycleWork()
	}
	var continuation *lifecycleexecution.Continuation
	if len(request.Assignment.Continuation) != 0 {
		captured, err := lifecycleexecution.DecodeContinuation(procedure, request.Assignment.Continuation)
		if err != nil {
			return LifecycleHelperInput{}, err
		}
		continuation = &captured
	}
	if err := progress.ValidateContinuation(attempt, procedure, continuation); err != nil {
		return LifecycleHelperInput{}, err
	}
	ack := request.Acknowledgement
	acknowledged, err := workexecution.DecodeAgentAttempt(ack.Attempt, identity)
	if err != nil || !workexecution.SameAssignment(attempt, acknowledged) || ack.Evidence == nil || ack.RecordedAt == nil || ack.RecordedAt.CheckValid() != nil {
		return LifecycleHelperInput{}, invalidLifecycleWork()
	}
	checkpoint, err := progress.Checkpoint(attempt, procedure)
	if err != nil {
		return LifecycleHelperInput{}, err
	}
	evidence := workexecution.Input{ID: ack.Evidence.InputId, SHA256: hex.EncodeToString(ack.Evidence.Sha256)}
	if checkpoint.Sequence != ack.Sequence || checkpoint.StageID != ack.StageId || checkpoint.Evidence != evidence || !checkpoint.RecordedAt.Equal(ack.RecordedAt.AsTime()) {
		return LifecycleHelperInput{}, invalidLifecycleWork()
	}
	stages, err := lifecycleexecution.Stages(procedure)
	if err != nil {
		return LifecycleHelperInput{}, err
	}
	input := LifecycleHelperInput{Attempt: attempt, Procedure: procedure, Progress: progress, Stage: stages[len(progress.Completed)], InspectOnly: request.InspectOnly}
	if input.Stage.Expected == lifecycleexecution.HookSucceeded {
		return LifecycleHelperInput{}, invalidLifecycleWork()
	}
	return input, nil
}

func ValidateLifecycleHookDelivery(attempt workexecution.Attempt, procedure *agentpb.ServiceLifecycleWorkInput, delivery *agentpb.ServiceLifecycleHookDelivery) error {
	if procedure == nil || procedure.Hook == nil || delivery == nil || RejectUnknown(delivery) != nil || delivery.Hook == nil ||
		delivery.StageId != procedure.Hook.StageId {
		return invalidLifecycleWork()
	}
	claimed, err := workexecution.DecodeAgentAttempt(delivery.Attempt, attempt.Executor)
	if err != nil || !workexecution.SameAssignment(attempt, claimed) {
		return invalidLifecycleWork()
	}
	hook := delivery.Hook
	if hook.Event != procedure.Hook.Event || !slices.Equal(hook.Command, procedure.Hook.Command) || hook.TimeoutSeconds != procedure.Hook.TimeoutSeconds ||
		hook.BackingServiceId != procedure.ServiceId || len(hook.Facts) != 0 || len(hook.FactSchema) != 0 {
		return invalidLifecycleWork()
	}
	definition, inputs, schema, err := DecodeBackingHookProcedure(hook)
	if err != nil {
		return err
	}
	return backinghook.Validate(definition, inputs, schema)
}

// ReadLifecycleHelperReceipt returns nil for a valid but unsatisfied inspection.
// An effect response must prove its postcondition; only inspection may report
// an incomplete stage without presenting a success receipt.
func ReadLifecycleHelperReceipt(input LifecycleHelperInput, response *agentpb.ServiceLifecycleHelperResponse) (*lifecycleexecution.Receipt, error) {
	if input.Procedure == nil || input.Progress.BeganAt == nil || response == nil || RejectUnknown(response) != nil ||
		response.StageId != input.Stage.ID || response.Member != input.Stage.Member || response.Inspection != input.InspectOnly ||
		response.ObservedAt == nil || response.ObservedAt.CheckValid() != nil || response.ObservedAt.AsTime().Before(*input.Progress.BeganAt) {
		return nil, invalidLifecycleWork()
	}
	if !response.Satisfied {
		if !input.InspectOnly || response.State != "" || response.Evidence != nil {
			return nil, invalidLifecycleWork()
		}
		return nil, nil
	}
	if response.Evidence == nil || response.State != string(input.Stage.Expected) {
		return nil, invalidLifecycleWork()
	}
	receipt := lifecycleexecution.Receipt{StageID: response.StageId, Member: response.Member, State: lifecycleexecution.State(response.State),
		Evidence: workexecution.Input{ID: response.Evidence.InputId, SHA256: hex.EncodeToString(response.Evidence.Sha256)}, ObservedAt: response.ObservedAt.AsTime().UTC()}
	if receipt.Evidence.ID != input.Attempt.Input.ID {
		return nil, invalidLifecycleWork()
	}
	forward := input.Attempt
	if input.Recovery != nil {
		forward = input.Recovery.Forward
		if input.Attempt.StartedAt == nil || receipt.ObservedAt.Before(*input.Attempt.StartedAt) {
			return nil, invalidLifecycleWork()
		}
	}
	if _, err := input.Progress.Confirm(forward, input.Procedure, receipt, receipt.ObservedAt); err != nil {
		return nil, err
	}
	return &receipt, nil
}

func ValidateLifecycleHelperResponse(input LifecycleHelperInput, response *agentpb.ServiceLifecycleHelperResponse) error {
	_, err := ReadLifecycleHelperReceipt(input, response)
	return err
}
