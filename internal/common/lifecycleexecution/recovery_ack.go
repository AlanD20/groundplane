package lifecycleexecution

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func DecodeRecoveryAck(
	attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput,
	source Recovery,
	report RecoveryReport,
	ack *agentpb.ServiceLifecycleRecoveryReportAck,
) (workexecution.Attempt, error) {
	if ack == nil || len(ack.ProtoReflect().GetUnknown()) != 0 || ack.Evidence == nil ||
		len(ack.Evidence.ProtoReflect().GetUnknown()) != 0 ||
		ack.StartedAt == nil ||
		ack.StartedAt.CheckValid() != nil ||
		len(ack.StartedAt.ProtoReflect().GetUnknown()) != 0 ||
		ack.FinishedAt == nil ||
		ack.FinishedAt.CheckValid() != nil ||
		len(ack.FinishedAt.ProtoReflect().GetUnknown()) != 0 ||
		attempt.StartedAt == nil ||
		!ack.StartedAt.AsTime().Equal(*attempt.StartedAt) ||
		ack.FinishedAt.AsTime().Before(report.ObservedAt) ||
		ack.BoundaryCompleted != report.BoundaryCompleted() {
		return workexecution.Attempt{}, invalid()
	}
	claimed, err := workexecution.DecodeAgentAttempt(ack.Attempt, attempt.Executor)
	if err != nil || !workexecution.SameAssignment(attempt, claimed) {
		return workexecution.Attempt{}, invalid()
	}
	_, evidence, err := EncodeRecoveryReport(attempt, input, source, report)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if ack.Evidence.InputId != evidence.ID || hex.EncodeToString(ack.Evidence.Sha256) != evidence.SHA256 {
		return workexecution.Attempt{}, invalid()
	}
	terminal := attempt
	finished := ack.FinishedAt.AsTime().UTC()
	terminal.FinishedAt, terminal.Result, terminal.Outcome = &finished, &evidence, workexecution.Failed
	if ack.BoundaryCompleted {
		terminal.Outcome = workexecution.Succeeded
	}
	return terminal, terminal.Validate()
}
