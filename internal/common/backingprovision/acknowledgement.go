package backingprovision

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/backingruntimefact"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func DecodeCompletionAck(attempt workexecution.Attempt, layout Layout, progress Progress,
	observation backingruntimefact.Observation, ack *agentpb.BackingProvisionCompletionAck,
) (workexecution.Attempt, error) {
	if ack == nil || executionplan.RejectUnknown(ack) != nil || progress.Stage != Complete ||
		progress.Validate(attempt, layout) != nil {
		return workexecution.Attempt{}, invalid("backing creation completion acknowledgement is invalid")
	}
	terminal, err := decodeTerminal(attempt, ack.Attempt, ack.Evidence, ack.Outcome, ack.StartedAt, ack.FinishedAt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	checkpoint, err := progress.Checkpoint(attempt, layout)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	evidence, err := CompletionEvidence(terminal, checkpoint, observation)
	if err != nil || evidence != *terminal.Result || terminal.FinishedAt.Before(progress.UpdatedAt) ||
		observation.ObservedAt.Before(progress.Last.At) || observation.ContainerID != progress.Last.ContainerID ||
		observation.ObservedAt.After(*terminal.FinishedAt) ||
		terminal.Outcome != workexecution.Succeeded && terminal.Outcome != workexecution.Cancelled &&
			terminal.Outcome != workexecution.Superseded {
		return workexecution.Attempt{}, invalid(
			"backing creation completion acknowledgement differs from its saved runtime",
		)
	}
	return terminal, nil
}

func DecodeFailureAck(attempt workexecution.Attempt, layout Layout, reason FailureReason,
	ack *agentpb.BackingProvisionFailureAck,
) (workexecution.Attempt, Failure, error) {
	if ack == nil || executionplan.RejectUnknown(ack) != nil {
		return workexecution.Attempt{}, Failure{}, invalid("backing creation failure acknowledgement is invalid")
	}
	terminal, err := decodeTerminal(attempt, ack.Attempt, ack.Evidence, ack.Outcome, ack.StartedAt, ack.FinishedAt)
	if err != nil {
		return workexecution.Attempt{}, Failure{}, err
	}
	failure, err := DecodeFailure(terminal, layout, ack.Failure, *terminal.Result)
	if err != nil || failure.Reason != reason || !failure.RecordedAt.Equal(*terminal.FinishedAt) ||
		terminal.Outcome != workexecution.Failed && terminal.Outcome != workexecution.Cancelled &&
			terminal.Outcome != workexecution.Superseded {
		return workexecution.Attempt{}, Failure{}, invalid(
			"backing creation failure acknowledgement differs from its saved cause",
		)
	}
	return terminal, failure, nil
}

func decodeTerminal(
	attempt workexecution.Attempt,
	identity *agentpb.WorkAttemptIdentity,
	evidence *agentpb.WorkInputReference,
	outcome string,
	started, finished *timestamppb.Timestamp,
) (workexecution.Attempt, error) {
	if attempt.Validate() != nil || attempt.FinishedAt != nil || evidence == nil || finished == nil ||
		finished.CheckValid() != nil ||
		(attempt.StartedAt == nil) != (started == nil) ||
		started != nil && (started.CheckValid() != nil || !started.AsTime().Equal(*attempt.StartedAt)) {
		return workexecution.Attempt{}, invalid("backing creation terminal receipt has invalid identity or timing")
	}
	claimed, err := workexecution.DecodeAgentAttempt(identity, attempt.Executor)
	if err != nil || !workexecution.SameAssignment(claimed, attempt) {
		return workexecution.Attempt{}, invalid("backing creation terminal receipt belongs to another assignment")
	}
	terminal := attempt
	at, result := finished.AsTime().
		UTC(),
		workexecution.Input{
			ID:     evidence.InputId,
			SHA256: hex.EncodeToString(evidence.Sha256),
		}
	terminal.FinishedAt, terminal.Result, terminal.Outcome = &at, &result, workexecution.Outcome(outcome)
	if terminal.Validate() != nil {
		return workexecution.Attempt{}, invalid("backing creation terminal receipt has no valid result")
	}
	return terminal, nil
}
