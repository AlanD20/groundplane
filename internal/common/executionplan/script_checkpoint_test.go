package executionplan

import (
	"bytes"
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestValidateScriptCheckpointRequestAcceptsClosedSequence(t *testing.T) {
	t.Parallel()

	containerID := strings.Repeat("a", 64)
	bodyDigest := bytes.Repeat([]byte{0xb1}, sha256.Size)
	labelsDigest := bytes.Repeat([]byte{0xc2}, sha256.Size)
	exitCode := int32(0)
	cases := []*agentpb.ScriptCheckpointRequest{
		scriptCheckpointRequest(
			agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_NOT_STARTED,
			agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_START_AUTHORIZED,
			&agentpb.ScriptStartAuthorizedCheckpoint{},
		),
		scriptCheckpointRequest(
			agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_START_AUTHORIZED,
			agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_BODY_PREPARED,
			&agentpb.ScriptBodyPreparedCheckpoint{
				BodySha256: bodyDigest, Uid: 1000, Gid: 1000, Device: 11, Inode: 12, Leaf: "body",
			},
		),
		scriptCheckpointRequest(
			agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_BODY_PREPARED,
			agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CONTAINER_CREATED,
			&agentpb.ScriptContainerCreatedCheckpoint{
				ContainerId: containerID, OwnershipLabelsSha256: labelsDigest,
			},
		),
		scriptCheckpointRequest(
			agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CONTAINER_CREATED,
			agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_OUTCOME_RECORDED,
			&agentpb.ScriptOutcomeCheckpoint{
				Reason: agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_NORMAL_EXIT, ExitCode: &exitCode,
				ObservedAt: timestamppb.New(time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)),
			},
		),
		scriptCheckpointRequest(
			agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_OUTCOME_RECORDED,
			agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN,
			&agentpb.ScriptCleanupCheckpoint{
				ContainerId: containerID, BodyDevice: 11, BodyInode: 12, BodyLeaf: "body",
				ContainerAbsent: true, BodyAbsent: true, ExecutionDirectoryAbsent: true,
			},
		),
	}
	for _, request := range cases {
		request.ControlPayloadSha256 = mustScriptCheckpointDigest(t, request)
		if _, err := ValidateScriptCheckpointRequest(request); err != nil {
			t.Fatalf("ValidateScriptCheckpointRequest(%s): %v", request.State, err)
		}
	}
}

func TestValidateScriptCheckpointRequestRejectsSkippedStateAndExitMismatch(t *testing.T) {
	t.Parallel()

	skipped := scriptCheckpointRequest(
		agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_NOT_STARTED,
		agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CONTAINER_CREATED,
		&agentpb.ScriptContainerCreatedCheckpoint{
			ContainerId: strings.Repeat("a", 64), OwnershipLabelsSha256: bytes.Repeat([]byte{1}, sha256.Size),
		},
	)
	if _, err := ComputeScriptCheckpointPayloadDigest(skipped); err == nil {
		t.Fatal("expected skipped state to be rejected")
	}

	exitCode := int32(1)
	failed := scriptCheckpointRequest(
		agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CONTAINER_CREATED,
		agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_OUTCOME_RECORDED,
		&agentpb.ScriptOutcomeCheckpoint{
			Reason: agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RUNTIME_FAILURE, ExitCode: &exitCode,
			ObservedAt: timestamppb.Now(),
		},
	)
	if _, err := ComputeScriptCheckpointPayloadDigest(failed); err == nil {
		t.Fatal("expected non-normal outcome with exit code to be rejected")
	}
}

func TestValidateScriptCheckpointAckRequiresExactDelivery(t *testing.T) {
	t.Parallel()

	request := scriptCheckpointRequest(
		agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_NOT_STARTED,
		agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_START_AUTHORIZED,
		&agentpb.ScriptStartAuthorizedCheckpoint{},
	)
	request.ControlPayloadSha256 = mustScriptCheckpointDigest(t, request)
	ack := &agentpb.ScriptCheckpointAck{
		TaskId: request.TaskId, OperationId: request.OperationId, AssignmentId: request.AssignmentId,
		StepId: request.StepId, ScriptExecutionId: request.ScriptExecutionId, State: request.State,
		ControlPayloadSha256: append([]byte(nil), request.ControlPayloadSha256...),
	}
	if _, err := ValidateScriptCheckpointAck(ack, request); err != nil {
		t.Fatalf("ValidateScriptCheckpointAck(): %v", err)
	}
	ack.AssignmentId = "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	if _, err := ValidateScriptCheckpointAck(ack, request); err == nil {
		t.Fatal("expected mismatched acknowledgement to be rejected")
	}
}

func scriptCheckpointRequest(
	expected agentpb.ScriptExecutionState,
	state agentpb.ScriptExecutionState,
	evidence any,
) *agentpb.ScriptCheckpointRequest {
	request := &agentpb.ScriptCheckpointRequest{
		TaskId: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", OperationId: "op_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		AssignmentId: "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV", StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ScriptExecutionId: testScriptExecutionID, PlanHash: bytes.Repeat([]byte{0xd3}, sha256.Size),
		ExpectedState: expected, State: state,
	}
	switch value := evidence.(type) {
	case *agentpb.ScriptStartAuthorizedCheckpoint:
		request.Evidence = &agentpb.ScriptCheckpointRequest_StartAuthorized{StartAuthorized: value}
	case *agentpb.ScriptBodyPreparedCheckpoint:
		request.Evidence = &agentpb.ScriptCheckpointRequest_BodyPrepared{BodyPrepared: value}
	case *agentpb.ScriptContainerCreatedCheckpoint:
		request.Evidence = &agentpb.ScriptCheckpointRequest_ContainerCreated{ContainerCreated: value}
	case *agentpb.ScriptOutcomeCheckpoint:
		request.Evidence = &agentpb.ScriptCheckpointRequest_Outcome{Outcome: value}
	case *agentpb.ScriptCleanupCheckpoint:
		request.Evidence = &agentpb.ScriptCheckpointRequest_Cleanup{Cleanup: value}
	}
	return request
}

func mustScriptCheckpointDigest(t *testing.T, request *agentpb.ScriptCheckpointRequest) []byte {
	t.Helper()
	digest, err := ComputeScriptCheckpointPayloadDigest(request)
	if err != nil {
		t.Fatalf("ComputeScriptCheckpointPayloadDigest(): %v", err)
	}
	return digest
}
