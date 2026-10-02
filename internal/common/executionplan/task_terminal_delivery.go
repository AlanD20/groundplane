package executionplan

import (
	"bytes"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// SameTaskTerminalReceiptAuthority compares immutable completion evidence,
// excluding only the authenticated process delivering that evidence.
func SameTaskTerminalReceiptAuthority(left, right *agentpb.TaskTerminalReceiptAck) bool {
	return ValidateTaskTerminalReceiptAck(left) == nil && ValidateTaskTerminalReceiptAck(right) == nil &&
		left.TaskId == right.TaskId && left.AssignmentId == right.AssignmentId &&
		left.AssignmentGeneration == right.AssignmentGeneration && left.Terminal == right.Terminal &&
		left.DurableTaskModRevision == right.DurableTaskModRevision && bytes.Equal(left.PlanHash, right.PlanHash) &&
		bytes.Equal(
			left.TaskAckSha256,
			right.TaskAckSha256,
		) && bytes.Equal(left.TerminalReceiptSha256, right.TerminalReceiptSha256)
}

// ValidateTaskTerminalReceiptAck validates the exact delivery tuple. It does not
// decide whether the receipt is committed: only the Controller repository can.
func ValidateTaskTerminalReceiptAck(receipt *agentpb.TaskTerminalReceiptAck) error {
	if receipt == nil || RejectUnknown(receipt) != nil || len(receipt.ProcessGeneration) != 16 ||
		ids.Validate(
			ids.KindTask,
			receipt.TaskId,
		) != nil || ids.Validate(ids.KindAssignment, receipt.AssignmentId) != nil ||
		receipt.AssignmentGeneration == 0 || len(receipt.PlanHash) != sha256.Size ||
		len(receipt.TaskAckSha256) != sha256.Size || len(receipt.TerminalReceiptSha256) != sha256.Size ||
		receipt.DurableTaskModRevision <= 0 || !validTaskTerminal(receipt.Terminal) {
		return errs.New(errs.KindValidationFailed, "Task terminal delivery tuple is invalid")
	}
	return nil
}

func TaskAcknowledgementSHA256(ack *agentpb.TaskAck) ([]byte, error) {
	if ack == nil || RejectUnknown(ack) != nil || ids.Validate(ids.KindTask, ack.TaskId) != nil ||
		ids.Validate(ids.KindAssignment, ack.AssignmentId) != nil || ack.AssignmentGeneration == 0 ||
		len(ack.PlanHash) != sha256.Size || ack.ExecutionEpoch == 0 || !validTaskTerminal(ack.Terminal) ||
		ack.GetBackupResult() == nil || len(ack.ReleaseRecoveryRecordSha256) != 0 {
		return nil, errs.New(errs.KindValidationFailed, "Backup Task acknowledgement is invalid")
	}
	result := ack.GetBackupResult()
	if result.RecoveryRequired == nil ||
		(result.FailedStepId != "" && ids.Validate(ids.KindStep, result.FailedStepId) != nil) ||
		(ack.Terminal == agentpb.TaskTerminal_TASK_TERMINAL_COMPLETED &&
			(ack.ExitCode != 0 || result.FailedStepId != "" || result.GetRecoveryRequired())) {
		return nil, errs.New(errs.KindValidationFailed, "Backup Task acknowledgement result is invalid")
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(ack)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(encoded)
	hash := sha256.New()
	_, _ = hash.Write([]byte("groundplane.task.ack.schema-one.v1\x00"))
	_, _ = hash.Write(encoded)
	return hash.Sum(nil), nil
}

func validTaskTerminal(terminal agentpb.TaskTerminal) bool {
	switch terminal {
	case agentpb.TaskTerminal_TASK_TERMINAL_COMPLETED, agentpb.TaskTerminal_TASK_TERMINAL_FAILED,
		agentpb.TaskTerminal_TASK_TERMINAL_TIMED_OUT, agentpb.TaskTerminal_TASK_TERMINAL_ABORTED:
		return true
	default:
		return false
	}
}

func TaskTerminalReceiptApplied(receipt *agentpb.TaskTerminalReceiptAck) (*agentpb.TaskTerminalReceiptApplied, error) {
	if err := ValidateTaskTerminalReceiptAck(receipt); err != nil {
		return nil, err
	}
	return &agentpb.TaskTerminalReceiptApplied{
		ProcessGeneration: append(
			[]byte(nil),
			receipt.ProcessGeneration...), TaskId: receipt.TaskId, AssignmentId: receipt.AssignmentId,
		AssignmentGeneration: receipt.AssignmentGeneration, PlanHash: append([]byte(nil), receipt.PlanHash...), Terminal: receipt.Terminal,
		TaskAckSha256: append(
			[]byte(nil),
			receipt.TaskAckSha256...), TerminalReceiptSha256: append([]byte(nil), receipt.TerminalReceiptSha256...),
		DurableTaskModRevision: receipt.DurableTaskModRevision,
	}, nil
}

func ReceiptFromTaskTerminalApplied(
	applied *agentpb.TaskTerminalReceiptApplied,
) (*agentpb.TaskTerminalReceiptAck, error) {
	if applied == nil || RejectUnknown(applied) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Task terminal receipt application is invalid")
	}
	receipt := &agentpb.TaskTerminalReceiptAck{
		ProcessGeneration: append(
			[]byte(nil),
			applied.ProcessGeneration...), TaskId: applied.TaskId, AssignmentId: applied.AssignmentId,
		AssignmentGeneration: applied.AssignmentGeneration, PlanHash: append([]byte(nil), applied.PlanHash...), Terminal: applied.Terminal,
		TaskAckSha256: append(
			[]byte(nil),
			applied.TaskAckSha256...), TerminalReceiptSha256: append([]byte(nil), applied.TerminalReceiptSha256...),
		DurableTaskModRevision: applied.DurableTaskModRevision,
	}
	if err := ValidateTaskTerminalReceiptAck(receipt); err != nil {
		return nil, err
	}
	return receipt, nil
}
