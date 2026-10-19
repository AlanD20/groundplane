package attachexecution

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func EncodeProvisionCompletion(
	input ProvisionInput,
	progress ProvisionProgress,
) (*agentpb.AttachProvisionCompletionRequest, error) {
	authority, err := input.Authority()
	if err != nil {
		return nil, err
	}
	if !progress.Complete(input.Attempt, authority) {
		return nil, invalid("Attach provisioning cannot complete before every captured command succeeds")
	}
	checkpoint, err := (ProvisionReport{Progress: progress}).CheckpointAuthority(input.Attempt, authority)
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
	return &agentpb.AttachProvisionCompletionRequest{Attempt: attempt, CheckpointSequence: checkpoint.Sequence,
		CheckpointEvidence: &agentpb.WorkInputReference{InputId: checkpoint.Evidence.ID, Sha256: digest}}, nil
}

func DecodeProvisionCompletion(
	input ProvisionInput,
	progress ProvisionProgress,
	ack *agentpb.AttachProvisionCompletionAck,
) (workexecution.Attempt, error) {
	authority, err := input.Authority()
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if ack == nil || executionplan.RejectUnknown(ack) != nil || ack.CheckpointEvidence == nil ||
		ack.BindingResult == nil ||
		ack.FinishedAt == nil ||
		ack.FinishedAt.CheckValid() != nil ||
		!progress.Complete(input.Attempt, authority) ||
		ack.CheckpointSequence != progress.Sequence {
		return workexecution.Attempt{}, invalid(
			"Attach provisioning completion lacks its final checkpoint or binding result",
		)
	}
	claimed, err := workexecution.DecodeAgentAttempt(ack.Attempt, input.Attempt.Executor)
	if err != nil || !workexecution.SameAssignment(claimed, input.Attempt) {
		return workexecution.Attempt{}, invalid("Attach provisioning completion belongs to another assignment")
	}
	checkpoint, err := (ProvisionReport{Progress: progress}).CheckpointAuthority(input.Attempt, authority)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	evidence := workexecution.Input{
		ID:     ack.CheckpointEvidence.InputId,
		SHA256: hex.EncodeToString(ack.CheckpointEvidence.Sha256),
	}
	binding := workexecution.Input{ID: ack.BindingResult.InputId, SHA256: hex.EncodeToString(ack.BindingResult.Sha256)}
	finished, outcome := ack.FinishedAt.AsTime(), workexecution.Outcome(ack.Outcome)
	if evidence != checkpoint.Evidence || binding.Validate() != nil || finished.Before(progress.UpdatedAt) ||
		(outcome != workexecution.Succeeded && outcome != workexecution.Cancelled && outcome != workexecution.Superseded) {
		return workexecution.Attempt{}, invalid("Attach provisioning completion differs from acknowledged progress")
	}
	terminal := input.Attempt
	terminal.FinishedAt, terminal.Outcome, terminal.Result = &finished, outcome, &binding
	return terminal, terminal.Validate()
}
