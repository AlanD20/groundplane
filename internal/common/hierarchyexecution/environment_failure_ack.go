package hierarchyexecution

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func DecodeEnvironmentFailureAck(attempt workexecution.Attempt, reason FailureReason, ack *agentpb.EnvironmentRemovalFailureAck) (workexecution.Attempt, EnvironmentFailure, error) {
	if ack == nil || executionplan.RejectUnknown(ack) != nil || ack.Evidence == nil || ack.FinishedAt == nil || ack.FinishedAt.CheckValid() != nil ||
		ack.Outcome != string(workexecution.Failed) && ack.Outcome != string(workexecution.Cancelled) && ack.Outcome != string(workexecution.Superseded) {
		return workexecution.Attempt{}, EnvironmentFailure{}, invalidEnvironmentProgress()
	}
	claimed, err := workexecution.DecodeAgentAttempt(ack.Attempt, attempt.Executor)
	if err != nil || !workexecution.SameAssignment(claimed, attempt) {
		return workexecution.Attempt{}, EnvironmentFailure{}, invalidEnvironmentProgress()
	}
	terminal := attempt
	if ack.StartedAt != nil {
		if ack.StartedAt.CheckValid() != nil {
			return workexecution.Attempt{}, EnvironmentFailure{}, invalidEnvironmentProgress()
		}
		started := ack.StartedAt.AsTime()
		if attempt.StartedAt != nil && !attempt.StartedAt.Equal(started) {
			return workexecution.Attempt{}, EnvironmentFailure{}, invalidEnvironmentProgress()
		}
		terminal.StartedAt = &started
	} else if attempt.StartedAt != nil {
		return workexecution.Attempt{}, EnvironmentFailure{}, invalidEnvironmentProgress()
	}
	evidence := workexecution.Input{ID: ack.Evidence.InputId, SHA256: hex.EncodeToString(ack.Evidence.Sha256)}
	failure, err := DecodeEnvironmentFailure(terminal, ack.Failure, evidence)
	if err != nil || failure.Reason != reason || !ack.FinishedAt.AsTime().Equal(failure.RecordedAt) {
		return workexecution.Attempt{}, EnvironmentFailure{}, invalidEnvironmentProgress()
	}
	finished := ack.FinishedAt.AsTime()
	terminal.FinishedAt, terminal.Outcome, terminal.Result = &finished, workexecution.Outcome(ack.Outcome), &evidence
	return terminal, failure, terminal.Validate()
}
