package attachexecution

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func EncodeInspectionRequest(
	input RecoveryInput,
	report RemovalInspection,
) (*agentpb.AttachRecoveryCompletionRequest, error) {
	raw, reference, err := EncodeRemovalInspection(input, report)
	if err != nil {
		return nil, err
	}
	identity, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return nil, err
	}
	digest, err := hex.DecodeString(reference.SHA256)
	if err != nil {
		return nil, err
	}
	return &agentpb.AttachRecoveryCompletionRequest{Attempt: identity, Report: raw,
		Evidence: &agentpb.WorkInputReference{InputId: reference.ID, Sha256: digest}}, nil
}

func DecodeInspectionAck(
	input RecoveryInput,
	report RemovalInspection,
	ack *agentpb.AttachRecoveryCompletionAck,
) (workexecution.Attempt, error) {
	if ack == nil || executionplan.RejectUnknown(ack) != nil || ack.Evidence == nil ||
		ack.FinishedAt == nil || ack.FinishedAt.CheckValid() != nil {
		return workexecution.Attempt{}, invalid("Attach inspection acknowledgement is malformed")
	}
	claimed, err := workexecution.DecodeAgentAttempt(ack.Attempt, input.Attempt.Executor)
	if err != nil || !workexecution.SameAssignment(claimed, input.Attempt) {
		return workexecution.Attempt{}, invalid("Attach inspection acknowledgement belongs to another attempt")
	}
	_, reference, err := EncodeRemovalInspection(input, report)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	provided := workexecution.Input{ID: ack.Evidence.InputId, SHA256: hex.EncodeToString(ack.Evidence.Sha256)}
	outcome := workexecution.Failed
	if report.ExecutionStopped() {
		outcome = workexecution.Succeeded
	}
	finished := ack.FinishedAt.AsTime()
	if reference != provided || ack.Outcome != string(outcome) || finished.Before(report.ObservedAt) {
		return workexecution.Attempt{}, invalid("Attach inspection acknowledgement differs from the saved report")
	}
	terminal := input.Attempt
	if ack.StartedAt != nil {
		if ack.StartedAt.CheckValid() != nil {
			return workexecution.Attempt{}, invalid("Attach inspection acknowledgement start time is invalid")
		}
		started := ack.StartedAt.AsTime()
		if terminal.StartedAt != nil && !terminal.StartedAt.Equal(started) {
			return workexecution.Attempt{}, invalid("Attach inspection acknowledgement changed its Start time")
		}
		terminal.StartedAt = &started
	} else if terminal.StartedAt != nil {
		return workexecution.Attempt{}, invalid("Attach inspection acknowledgement lost its Start time")
	}
	terminal.FinishedAt, terminal.Result, terminal.Outcome = &finished, &reference, outcome
	return terminal, terminal.Validate()
}
