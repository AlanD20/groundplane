package lifecycleruntime

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (input Input) checkpointRequest(
	progress lifecycleexecution.Progress,
) (*agentpb.ServiceLifecycleCheckpointRequest, error) {
	if input.procedure == nil {
		return nil, invalidInput()
	}
	raw, err := lifecycleexecution.EncodeProgress(input.attempt, input.procedure, progress)
	if err != nil {
		return nil, err
	}
	attempt, err := workexecution.EncodeAgentAttempt(input.attempt)
	if err != nil {
		return nil, err
	}
	return &agentpb.ServiceLifecycleCheckpointRequest{Attempt: attempt, Progress: raw}, nil
}

func (input Input) checkpointAck(
	progress lifecycleexecution.Progress,
	ack *agentpb.ServiceLifecycleCheckpointAck,
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
	progress lifecycleexecution.Progress,
) (*agentpb.ServiceLifecycleCompletionRequest, error) {
	if input.procedure == nil || !input.complete(progress) {
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
	return &agentpb.ServiceLifecycleCompletionRequest{Attempt: attempt, Evidence: &agentpb.WorkInputReference{
		InputId: checkpoint.Evidence.ID, Sha256: digest,
	}}, nil
}

func (input Input) completionAck(
	progress lifecycleexecution.Progress,
	ack *agentpb.ServiceLifecycleCompletionAck,
) (workexecution.Attempt, error) {
	if input.procedure == nil || ack == nil || ack.Stopped != nil || executionplan.RejectUnknown(ack) != nil ||
		ack.Evidence == nil ||
		ack.StartedAt == nil ||
		ack.StartedAt.CheckValid() != nil ||
		ack.FinishedAt == nil ||
		ack.FinishedAt.CheckValid() != nil ||
		input.attempt.StartedAt == nil ||
		!input.attempt.StartedAt.Equal(ack.StartedAt.AsTime()) ||
		!input.complete(progress) {
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

func (input Input) complete(progress lifecycleexecution.Progress) bool {
	stages, err := lifecycleexecution.Stages(input.procedure)
	return err == nil && progress.BeganAt == nil && len(progress.Completed) == len(stages) &&
		progress.Validate(input.attempt, input.procedure) == nil
}
