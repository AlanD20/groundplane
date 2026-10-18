package removalruntime

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/removalexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (input Input) CheckpointRequest(progress removalexecution.Progress) (*agentpb.ServiceRemovalRecoveryCheckpointRequest, error) {
	raw, err := removalexecution.EncodeProgress(input.attempt, input.procedure, input.capture, progress)
	if err != nil {
		return nil, err
	}
	identity, err := workexecution.EncodeAgentAttempt(input.attempt)
	if err != nil {
		return nil, err
	}
	return &agentpb.ServiceRemovalRecoveryCheckpointRequest{Attempt: identity, Progress: raw}, nil
}

// Acknowledged compares the complete grant, not only its sequence. A reply from
// another executor generation or with different evidence cannot authorize work.
func (input Input) Acknowledged(progress removalexecution.Progress, ack *agentpb.ServiceRemovalRecoveryCheckpointAck) (workexecution.Checkpoint, error) {
	if ack == nil || executionplan.RejectUnknown(ack) != nil || ack.Evidence == nil ||
		ack.RecordedAt == nil || ack.RecordedAt.CheckValid() != nil {
		return workexecution.Checkpoint{}, invalidInput()
	}
	identity, err := workexecution.DecodeAgentAttempt(ack.Attempt, input.attempt.Executor)
	if err != nil || !workexecution.SameAssignment(identity, input.attempt) {
		return workexecution.Checkpoint{}, invalidInput()
	}
	expected, err := progress.Checkpoint(input.attempt, input.procedure, input.capture)
	if err != nil {
		return workexecution.Checkpoint{}, err
	}
	received := workexecution.Checkpoint{AttemptID: identity.ID, Sequence: ack.Sequence, StageID: ack.StageId,
		RecordedAt: ack.RecordedAt.AsTime().UTC(), Evidence: workexecution.Input{
			ID: ack.Evidence.InputId, SHA256: hex.EncodeToString(ack.Evidence.Sha256)}}
	if expected != received {
		return workexecution.Checkpoint{}, invalidInput()
	}
	return expected, nil
}

func (input Input) CompletionRequest(progress removalexecution.Progress, ack *agentpb.ServiceRemovalRecoveryCheckpointAck) (*agentpb.ServiceRemovalRecoveryCompletionRequest, error) {
	if progress.Phase != removalexecution.Complete || progress.InFlight {
		return nil, invalidInput()
	}
	checkpoint, err := input.Acknowledged(progress, ack)
	if err != nil {
		return nil, err
	}
	identity, err := workexecution.EncodeAgentAttempt(input.attempt)
	if err != nil {
		return nil, err
	}
	digest, err := hex.DecodeString(checkpoint.Evidence.SHA256)
	if err != nil {
		return nil, err
	}
	return &agentpb.ServiceRemovalRecoveryCompletionRequest{Attempt: identity,
		Evidence: &agentpb.WorkInputReference{InputId: checkpoint.Evidence.ID, Sha256: digest}}, nil
}
