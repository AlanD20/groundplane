package lifecycleexecution

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func DecodeFailureAck(
	attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput,
	reason FailureReason,
	ack *agentpb.ServiceLifecycleFailureAck,
) (workexecution.Attempt, Failure, error) {
	if ack == nil || len(ack.ProtoReflect().GetUnknown()) != 0 || ack.Evidence == nil ||
		len(ack.Evidence.ProtoReflect().GetUnknown()) != 0 ||
		ack.FinishedAt == nil ||
		ack.FinishedAt.CheckValid() != nil ||
		len(ack.FinishedAt.ProtoReflect().GetUnknown()) != 0 ||
		ack.Outcome != string(workexecution.Failed) && ack.Outcome != string(workexecution.Cancelled) &&
			ack.Outcome != string(workexecution.Superseded) {
		return workexecution.Attempt{}, Failure{}, invalid()
	}
	claimed, err := workexecution.DecodeAgentAttempt(ack.Attempt, attempt.Executor)
	if err != nil || !workexecution.SameAssignment(claimed, attempt) {
		return workexecution.Attempt{}, Failure{}, invalid()
	}
	terminal := attempt
	if attempt.StartedAt == nil {
		return workexecution.Attempt{}, Failure{}, invalid()
	}
	evidence := workexecution.Input{ID: ack.Evidence.InputId, SHA256: hex.EncodeToString(ack.Evidence.Sha256)}
	failure, err := DecodeFailure(terminal, input, ack.Failure, evidence)
	if err != nil || failure.Reason != reason || !ack.FinishedAt.AsTime().Equal(failure.RecordedAt) {
		return workexecution.Attempt{}, Failure{}, invalid()
	}
	finished := ack.FinishedAt.AsTime()
	terminal.FinishedAt, terminal.Outcome, terminal.Result = &finished, workexecution.Outcome(ack.Outcome), &evidence
	return terminal, failure, terminal.Validate()
}
