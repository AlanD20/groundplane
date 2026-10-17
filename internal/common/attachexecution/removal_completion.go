package attachexecution

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func EncodeRemovalCompletion(input RemovalInput, progress RemovalProgress) (*agentpb.AttachRemovalCompletionRequest, error) {
	if !progress.Complete(input) {
		return nil, invalid("Attach removal cannot complete before every captured command succeeds")
	}
	checkpoint, err := (RemovalReport{Progress: progress}).Checkpoint(input)
	if err != nil {
		return nil, err
	}
	attempt, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return nil, err
	}
	digest, err := hex.DecodeString(checkpoint.Evidence.SHA256)
	if err != nil {
		return nil, err
	}
	return &agentpb.AttachRemovalCompletionRequest{Attempt: attempt, CheckpointSequence: checkpoint.Sequence,
		CheckpointEvidence: &agentpb.WorkInputReference{InputId: checkpoint.Evidence.ID, Sha256: digest}}, nil
}

func DecodeRemovalCompletion(input RemovalInput, progress RemovalProgress, ack *agentpb.AttachRemovalCompletionAck) (workexecution.Attempt, error) {
	if ack == nil || executionplan.RejectUnknown(ack) != nil || ack.CheckpointEvidence == nil || ack.FinishedAt == nil ||
		ack.FinishedAt.CheckValid() != nil || !progress.Complete(input) || ack.CheckpointSequence != progress.Sequence {
		return workexecution.Attempt{}, invalid("Attach removal completion does not acknowledge the final command result")
	}
	claimed, err := workexecution.DecodeAgentAttempt(ack.Attempt, input.Attempt.Executor)
	if err != nil || !workexecution.SameAssignment(claimed, input.Attempt) {
		return workexecution.Attempt{}, invalid("Attach completion belongs to another assignment")
	}
	checkpoint, err := (RemovalReport{Progress: progress}).Checkpoint(input)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	evidence := workexecution.Input{ID: ack.CheckpointEvidence.InputId, SHA256: hex.EncodeToString(ack.CheckpointEvidence.Sha256)}
	finished := ack.FinishedAt.AsTime()
	outcome := workexecution.Outcome(ack.Outcome)
	if evidence != checkpoint.Evidence || finished.Before(progress.UpdatedAt) ||
		(outcome != workexecution.Succeeded && outcome != workexecution.Cancelled && outcome != workexecution.Superseded) {
		return workexecution.Attempt{}, invalid("Attach removal completion differs from acknowledged progress")
	}
	terminal := input.Attempt
	terminal.FinishedAt, terminal.Outcome, terminal.Result = &finished, outcome, &evidence
	return terminal, terminal.Validate()
}
