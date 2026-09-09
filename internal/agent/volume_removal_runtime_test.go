package agent

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	removal "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type volumeLoopHelper struct {
	t         *testing.T
	committed uint64
	calls     uint64
}

func (helper *volumeLoopHelper) Execute(
	_ context.Context,
	request *agentpb.EnvironmentDirectoryHelperRequest,
) (*agentpb.EnvironmentDirectoryHelperResponse, error) {
	helper.t.Helper()
	pending, err := removal.DecodePendingPath(request.VolumeRemovalPendingPath)
	if err != nil || pending.RequestOrdinal != helper.committed || request.TimeoutSeconds > 30 {
		helper.t.Fatalf("helper ran before exact bounded pending intent: %v", err)
	}
	helper.calls++
	completion := removal.Completion{
		OperationID: pending.OperationID, RequestOrdinal: pending.RequestOrdinal, RequestSHA256: pending.RequestSHA256,
		MutationCount: 128, NextCursor: []byte("next"), CompletedAt: time.Now().UTC(),
	}
	if helper.calls == 2 {
		completion.DirectoryAbsent, completion.NextCursor = true, nil
	}
	completion.ResponseSHA256, completion.ResponseBytes = removal.PathResponseDigest(
		completion,
	), removal.PathResponseBytes(
		completion,
	)
	encoded, err := removal.EncodeCompletion(completion)
	if err != nil {
		helper.t.Fatal(err)
	}
	return &agentpb.EnvironmentDirectoryHelperResponse{Schema: 1, MutationCount: completion.MutationCount,
		NextCursor: completion.NextCursor, Complete: completion.DirectoryAbsent, VolumeRemovalCompletion: encoded}, nil
}

func TestVolumeRemovalRuntimeCheckpointsEveryBoundedCall(t *testing.T) {
	assignment := environmentDirectoryAssignment(t)
	step := &agentpb.ExecutionStep{StepId: workerTestStepID, TimeoutSeconds: 21600,
		Payload: &agentpb.ExecutionStep_ManagedVolumeDirectoryRemove{
			ManagedVolumeDirectoryRemove: &agentpb.ManagedVolumeDirectoryRemove{
				VolumeId: ids.New(ids.KindVolume), ComposeKey: "data", IntentSha256: make([]byte, 32),
			},
		}}
	intent := sha256.Sum256([]byte("removal intent"))
	step.GetManagedVolumeDirectoryRemove().IntentSha256 = intent[:]
	helper := &volumeLoopHelper{t: t}
	runtime, err := NewEnvironmentDirectoryRuntime(helper)
	if err != nil {
		t.Fatal(err)
	}
	acknowledged := uint64(0)
	checkpoint := func(_ context.Context, request *agentpb.VolumeRemovalCheckpointRequest) (*agentpb.VolumeRemovalCheckpointAck, error) {
		ack := &agentpb.VolumeRemovalCheckpointAck{RequestId: request.RequestId, TaskId: request.TaskId,
			OperationId: request.OperationId, AssignmentId: request.AssignmentId}
		if len(request.Completion) != 0 {
			completion, err := removal.DecodeCompletion(request.Completion)
			if err != nil || completion.RequestOrdinal != helper.committed ||
				completion.RequestOrdinal != acknowledged+1 {
				t.Fatalf("checkpoint did not acknowledge exact helper result: %v", err)
			}
			acknowledged++
			if completion.DirectoryAbsent {
				ack.DirectoryAbsent = true
				return ack, nil
			}
		}
		pending := removal.PendingPath{
			OperationID:     assignment.OperationID,
			VolumeID:        step.GetManagedVolumeDirectoryRemove().VolumeId,
			Key:             "data",
			IntentSHA256:    intent,
			RequestOrdinal:  acknowledged + 1,
			MutationBudget:  128,
			TaskID:          assignment.TaskID,
			AssignmentID:    assignment.AssignmentID,
			AgentID:         ids.New(ids.KindAgent),
			AgentGeneration: 1,
			CreatedAt:       time.Now().UTC(),
		}
		if acknowledged != 0 {
			pending.Cursor = []byte("next")
		}
		pending.RequestSHA256 = removal.PathRequestDigest(pending)
		ack.PendingPath, err = removal.EncodePendingPath(pending)
		if err != nil {
			t.Fatal(err)
		}
		helper.committed = pending.RequestOrdinal
		return ack, nil
	}
	result, err := runtime.executeVolumeRemoval(context.Background(), assignment, step, checkpoint)
	if err != nil || !result.Complete || helper.calls != 2 || acknowledged != 2 {
		t.Fatalf(
			"bounded removal loop: result=%+v calls=%d acknowledged=%d error=%v",
			result,
			helper.calls,
			acknowledged,
			err,
		)
	}
}
