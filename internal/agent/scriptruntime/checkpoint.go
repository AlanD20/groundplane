package scriptruntime

import (
	"context"
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

const scriptCheckpointTimeout = 30 * time.Second

func checkpointStart(
	checkpoint func(context.Context, *agentpb.ScriptCheckpointRequest) error,
	request scriptexecution.Request,
	durable *agentpb.ScriptExecutionCheckpoint,
) error {
	wire := newScriptCheckpointRequest(
		request, durable.State, agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_START_AUTHORIZED,
	)
	wire.Evidence = &agentpb.ScriptCheckpointRequest_StartAuthorized{
		StartAuthorized: &agentpb.ScriptStartAuthorizedCheckpoint{},
	}
	if err := sendScriptCheckpoint(checkpoint, wire); err != nil {
		return err
	}
	durable.State, durable.StartAuthorized = wire.State, true
	return nil
}

func checkpointBody(
	checkpoint func(context.Context, *agentpb.ScriptCheckpointRequest) error,
	request scriptexecution.Request,
	durable *agentpb.ScriptExecutionCheckpoint,
	body scriptexecution.BodyEvidence,
) error {
	wire := newScriptCheckpointRequest(
		request, durable.State, agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_BODY_PREPARED,
	)
	wire.Evidence = &agentpb.ScriptCheckpointRequest_BodyPrepared{
		BodyPrepared: &agentpb.ScriptBodyPreparedCheckpoint{
			BodySha256: append([]byte(nil), body.SHA256...), Uid: body.UID, Gid: body.GID,
			Device: body.Device, Inode: body.Inode, Leaf: body.Leaf,
		},
	}
	if err := sendScriptCheckpoint(checkpoint, wire); err != nil {
		return err
	}
	durable.State = wire.State
	durable.BodyPrepared = proto.Clone(wire.GetBodyPrepared()).(*agentpb.ScriptBodyPreparedCheckpoint)
	return nil
}

func checkpointContainer(
	checkpoint func(context.Context, *agentpb.ScriptCheckpointRequest) error,
	request scriptexecution.Request,
	durable *agentpb.ScriptExecutionCheckpoint,
	evidence scriptexecution.ContainerEvidence,
) error {
	wire := newScriptCheckpointRequest(
		request, durable.State, agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CONTAINER_CREATED,
	)
	wire.Evidence = &agentpb.ScriptCheckpointRequest_ContainerCreated{
		ContainerCreated: &agentpb.ScriptContainerCreatedCheckpoint{
			ContainerId:           evidence.ID,
			OwnershipLabelsSha256: append([]byte(nil), evidence.OwnershipLabelsSHA256...),
		},
	}
	if err := sendScriptCheckpoint(checkpoint, wire); err != nil {
		return err
	}
	durable.State = wire.State
	durable.ContainerCreated = proto.Clone(wire.GetContainerCreated()).(*agentpb.ScriptContainerCreatedCheckpoint)
	return nil
}

func checkpointOutcome(
	checkpoint func(context.Context, *agentpb.ScriptCheckpointRequest) error,
	request scriptexecution.Request,
	durable *agentpb.ScriptExecutionCheckpoint,
	reason agentpb.ScriptOutcomeReason,
	exitCode *int32,
) error {
	wire := newScriptCheckpointRequest(
		request, durable.State, agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_OUTCOME_RECORDED,
	)
	outcome := &agentpb.ScriptOutcomeCheckpoint{Reason: reason, ObservedAt: timestamppb.Now()}
	if exitCode != nil {
		outcome.ExitCode = new(int32)
		*outcome.ExitCode = *exitCode
	}
	wire.Evidence = &agentpb.ScriptCheckpointRequest_Outcome{Outcome: outcome}
	if err := sendScriptCheckpoint(checkpoint, wire); err != nil {
		return err
	}
	durable.State = wire.State
	durable.Outcome = proto.Clone(outcome).(*agentpb.ScriptOutcomeCheckpoint)
	durable.ReconciliationRequired = reason == agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RECOVERY_INVARIANT_FAILURE
	return nil
}

func newScriptCheckpointRequest(
	request scriptexecution.Request,
	expected agentpb.ScriptExecutionState,
	state agentpb.ScriptExecutionState,
) *agentpb.ScriptCheckpointRequest {
	// Input was validated when the invocation was opened. The transport owns
	// the attempt-wide sequence and persists this report before sending it.
	digest, _ := hex.DecodeString(request.Input.SHA256)
	return &agentpb.ScriptCheckpointRequest{
		TaskId: request.TaskID, OperationId: request.OperationID, AttemptId: request.AttemptID,
		WorkItemId: request.WorkItemID, ScriptExecutionId: request.ExecutionID,
		InputId: request.Input.ID, InputSha256: digest, ExpectedState: expected, State: state,
		StageId: ids.DeriveAt(ids.KindStep, request.AssignedAt, request.AttemptID,
			"script/"+request.ExecutionID+"/"+scriptCheckpointStage(state)), RecordedAt: timestamppb.Now(),
	}
}

func scriptCheckpointStage(state agentpb.ScriptExecutionState) string {
	switch state {
	case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_START_AUTHORIZED:
		return "authorize-start"
	case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_BODY_PREPARED:
		return "prepare-body"
	case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CONTAINER_CREATED:
		return "create-container"
	case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_OUTCOME_RECORDED:
		return "record-outcome"
	case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN:
		return "prove-cleanup"
	default:
		return ""
	}
}

func sendScriptCheckpoint(
	checkpoint func(context.Context, *agentpb.ScriptCheckpointRequest) error,
	request *agentpb.ScriptCheckpointRequest,
) error {
	controlCtx, cancel := context.WithTimeout(context.Background(), scriptCheckpointTimeout)
	defer cancel()
	return checkpoint(controlCtx, request)
}
