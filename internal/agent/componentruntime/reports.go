package componentruntime

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (input Input) checkpointRequest(
	progress componentexecution.Progress,
) (*agentpb.ComponentWorkCheckpointRequest, error) {
	if input.procedure == nil {
		return nil, invalidInput()
	}
	raw, err := componentexecution.EncodeProgress(input.attempt, input.procedure, progress)
	if err != nil {
		return nil, err
	}
	attempt, err := workexecution.EncodeAgentAttempt(input.attempt)
	if err != nil {
		return nil, err
	}
	return &agentpb.ComponentWorkCheckpointRequest{Attempt: attempt, Progress: raw}, nil
}

func (input Input) checkpointAck(
	progress componentexecution.Progress,
	ack *agentpb.ComponentWorkCheckpointAck,
) (workexecution.Checkpoint, error) {
	if input.procedure == nil || ack == nil || executionplan.RejectUnknown(ack) != nil || ack.Evidence == nil ||
		ack.RecordedAt == nil || ack.RecordedAt.CheckValid() != nil {
		return workexecution.Checkpoint{}, invalidInput()
	}
	attempt, err := workexecution.DecodeAgentAttempt(ack.Attempt, input.attempt.Executor)
	if err != nil || !workexecution.SameAssignment(attempt, input.attempt) {
		return workexecution.Checkpoint{}, invalidInput()
	}
	expected, err := progress.Checkpoint(input.attempt, input.procedure)
	if err != nil {
		return workexecution.Checkpoint{}, err
	}
	evidence := workexecution.Input{ID: ack.Evidence.InputId, SHA256: hex.EncodeToString(ack.Evidence.Sha256)}
	if ack.Sequence != expected.Sequence || ack.StageId != expected.StageID || evidence != expected.Evidence ||
		!ack.RecordedAt.AsTime().Equal(expected.RecordedAt) {
		return workexecution.Checkpoint{}, invalidInput()
	}
	return expected, nil
}

func (input Input) completionRequest(
	progress componentexecution.Progress,
) (*agentpb.ComponentWorkCompletionRequest, error) {
	if input.procedure == nil || progress.Stage != componentexecution.Complete {
		return nil, invalidInput()
	}
	checkpoint, err := progress.Checkpoint(input.attempt, input.procedure)
	if err != nil {
		return nil, err
	}
	attempt, err := workexecution.EncodeAgentAttempt(input.attempt)
	if err != nil {
		return nil, err
	}
	digest, err := hex.DecodeString(checkpoint.Evidence.SHA256)
	if err != nil {
		return nil, err
	}
	return &agentpb.ComponentWorkCompletionRequest{Attempt: attempt, Evidence: &agentpb.WorkInputReference{
		InputId: checkpoint.Evidence.ID, Sha256: digest,
	}}, nil
}

func (input Input) completionAck(
	progress componentexecution.Progress,
	ack *agentpb.ComponentWorkCompletionAck,
) (workexecution.Attempt, error) {
	if input.procedure == nil || ack == nil || executionplan.RejectUnknown(ack) != nil || ack.Evidence == nil ||
		ack.StartedAt == nil || ack.StartedAt.CheckValid() != nil || ack.FinishedAt == nil || ack.FinishedAt.CheckValid() != nil ||
		input.attempt.StartedAt == nil || !input.attempt.StartedAt.Equal(ack.StartedAt.AsTime()) || progress.Stage != componentexecution.Complete {
		return workexecution.Attempt{}, invalidInput()
	}
	attempt, err := workexecution.DecodeAgentAttempt(ack.Attempt, input.attempt.Executor)
	if err != nil || !workexecution.SameAssignment(attempt, input.attempt) {
		return workexecution.Attempt{}, invalidInput()
	}
	checkpoint, err := progress.Checkpoint(input.attempt, input.procedure)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	evidence := workexecution.Input{ID: ack.Evidence.InputId, SHA256: hex.EncodeToString(ack.Evidence.Sha256)}
	finished := ack.FinishedAt.AsTime().UTC()
	if evidence != checkpoint.Evidence || finished.Before(progress.UpdatedAt) {
		return workexecution.Attempt{}, invalidInput()
	}
	terminal := input.attempt
	outcome := workexecution.Outcome(ack.Outcome)
	if outcome != workexecution.Succeeded && outcome != workexecution.Cancelled && outcome != workexecution.Superseded {
		return workexecution.Attempt{}, invalidInput()
	}
	terminal.FinishedAt, terminal.Result, terminal.Outcome = &finished, &evidence, outcome
	return terminal, terminal.Validate()
}
