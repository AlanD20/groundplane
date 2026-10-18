package hierarchyexecution

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func EncodeEnvironmentCompletion(input EnvironmentInput, progress EnvironmentProgress) (*agentpb.EnvironmentRemovalCompletionRequest, error) {
	if progress.Phase != EnvironmentRemoved {
		return nil, invalidEnvironmentProgress()
	}
	checkpoint, err := (EnvironmentReport{Progress: progress}).Checkpoint(input)
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
	return &agentpb.EnvironmentRemovalCompletionRequest{Attempt: attempt, CheckpointSequence: checkpoint.Sequence,
		CheckpointEvidence: &agentpb.WorkInputReference{InputId: checkpoint.Evidence.ID, Sha256: digest}}, nil
}

func DecodeEnvironmentCompletion(input EnvironmentInput, progress EnvironmentProgress, ack *agentpb.EnvironmentRemovalCompletionAck) (workexecution.Attempt, error) {
	if ack == nil || executionplan.RejectUnknown(ack) != nil || ack.CheckpointSequence != 4 || ack.CheckpointEvidence == nil ||
		ack.FinishedAt == nil || ack.FinishedAt.CheckValid() != nil || progress.Phase != EnvironmentRemoved {
		return workexecution.Attempt{}, invalidEnvironmentProgress()
	}
	claimed, err := workexecution.DecodeAgentAttempt(ack.Attempt, input.Attempt.Executor)
	if err != nil || !workexecution.SameAssignment(claimed, input.Attempt) {
		return workexecution.Attempt{}, invalidEnvironmentProgress()
	}
	checkpoint, err := (EnvironmentReport{Progress: progress}).Checkpoint(input)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	evidence := workexecution.Input{ID: ack.CheckpointEvidence.InputId, SHA256: hex.EncodeToString(ack.CheckpointEvidence.Sha256)}
	finished := ack.FinishedAt.AsTime()
	if evidence != checkpoint.Evidence || finished.Before(progress.UpdatedAt) {
		return workexecution.Attempt{}, invalidEnvironmentProgress()
	}
	terminal := input.Attempt
	terminal.FinishedAt, terminal.Outcome, terminal.Result = &finished, workexecution.Succeeded, &evidence
	return terminal, terminal.Validate()
}
