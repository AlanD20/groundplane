package etcd

import (
	"bytes"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// The report is evidence of what this assigned worker sent, not a replacement
// for the native terminal Task. Its terminal outcome may differ after a timeout
// races delivery. Identity, execution epoch and exact Ack digest must not differ.
func validateTaskTerminalReport(
	task TaskRecord,
	receipt *agentpb.TaskTerminalReceiptAck,
	report *agentpb.TaskAck,
) error {
	digest, err := executionplan.TaskAcknowledgementSHA256(report)
	if err != nil || task.TerminalAssignment == nil || task.Result == nil || receipt == nil ||
		report.TaskId != task.ID || report.AssignmentId != task.TerminalAssignment.AssignmentID ||
		report.AssignmentGeneration != task.TerminalAssignment.AssignmentGeneration ||
		report.ExecutionEpoch != task.Result.ExecutionEpoch || hex.EncodeToString(report.PlanHash) != task.PlanHash ||
		!bytes.Equal(digest, receipt.TaskAckSha256) {
		return terminalDeliveryConflict()
	}
	failed := report.GetBackupResult().FailedStepId
	if failed != "" {
		found := false
		for _, step := range task.Steps {
			if step.ID == failed {
				found = true
				break
			}
		}
		if !found {
			return terminalDeliveryConflict()
		}
	}
	return nil
}
