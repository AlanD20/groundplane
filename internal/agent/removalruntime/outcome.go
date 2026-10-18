package removalruntime

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/removalexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (input Input) FailureRequest(reason lifecycleexecution.FailureReason) (*agentpb.ServiceRemovalRecoveryFailureRequest, error) {
	if !reason.Valid() || input.capture.ValidateFor(input.attempt, input.procedure) != nil {
		return nil, invalidInput()
	}
	identity, err := workexecution.EncodeAgentAttempt(input.attempt)
	if err != nil {
		return nil, err
	}
	return &agentpb.ServiceRemovalRecoveryFailureRequest{Attempt: identity, Reason: string(reason)}, nil
}

func (input Input) AcknowledgeFailure(reason lifecycleexecution.FailureReason, ack *agentpb.ServiceRemovalRecoveryFailureAck) (workexecution.Attempt, error) {
	if ack == nil || executionplan.RejectUnknown(ack) != nil || ack.Evidence == nil ||
		ack.FinishedAt == nil || ack.FinishedAt.CheckValid() != nil {
		return workexecution.Attempt{}, invalidInput()
	}
	claimed, err := workexecution.DecodeAgentAttempt(ack.Attempt, input.attempt.Executor)
	if err != nil || !workexecution.SameAssignment(claimed, input.attempt) {
		return workexecution.Attempt{}, invalidInput()
	}
	evidence := workexecution.Input{ID: ack.Evidence.InputId, SHA256: hex.EncodeToString(ack.Evidence.Sha256)}
	finished := ack.FinishedAt.AsTime().UTC()
	attempt := input.attempt
	attempt.FinishedAt, attempt.Result, attempt.Outcome = &finished, &evidence, workexecution.Failed
	failure, err := removalexecution.DecodeFailure(attempt, input.procedure, input.capture, ack.Failure, evidence)
	if err != nil || failure.Reason != reason || !failure.RecordedAt.Equal(finished) {
		return workexecution.Attempt{}, invalidInput()
	}
	return attempt, attempt.Validate()
}

func (input Input) AcknowledgeCompletion(progress removalexecution.Progress, ack *agentpb.ServiceRemovalRecoveryCompletionAck) (workexecution.Attempt, error) {
	if ack == nil || executionplan.RejectUnknown(ack) != nil || ack.Evidence == nil ||
		ack.StartedAt == nil || ack.StartedAt.CheckValid() != nil || ack.FinishedAt == nil || ack.FinishedAt.CheckValid() != nil ||
		input.attempt.StartedAt == nil || !ack.StartedAt.AsTime().Equal(*input.attempt.StartedAt) ||
		progress.Phase != removalexecution.Complete || progress.InFlight {
		return workexecution.Attempt{}, invalidInput()
	}
	claimed, err := workexecution.DecodeAgentAttempt(ack.Attempt, input.attempt.Executor)
	if err != nil || !workexecution.SameAssignment(claimed, input.attempt) {
		return workexecution.Attempt{}, invalidInput()
	}
	checkpoint, err := progress.Checkpoint(input.attempt, input.procedure, input.capture)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	evidence := workexecution.Input{ID: ack.Evidence.InputId, SHA256: hex.EncodeToString(ack.Evidence.Sha256)}
	finished := ack.FinishedAt.AsTime().UTC()
	if evidence != checkpoint.Evidence || finished.Before(progress.UpdatedAt) {
		return workexecution.Attempt{}, invalidInput()
	}
	attempt := input.attempt
	attempt.FinishedAt, attempt.Result, attempt.Outcome = &finished, &evidence, workexecution.Succeeded
	return attempt, attempt.Validate()
}
