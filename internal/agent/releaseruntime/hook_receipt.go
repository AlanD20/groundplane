package releaseruntime

import (
	"bytes"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// HookCleanupReceipt selects the acknowledged Script evidence for the current
// hook. Controller confirmation additionally checks the durable zero-exit outcome;
// cleanup alone cannot turn a failed hook into a successful Release.
func HookCleanupReceipt(
	input releaseexecution.Input,
	report releaseexecution.CheckpointReport,
	record *agentworkjournal.ScriptRecord,
) (workexecution.Input, error) {
	hook, err := report.Progress.ActiveHook(input)
	if err != nil {
		return workexecution.Input{}, err
	}
	if report.Ack == nil || report.ValidateEnvelope(input.Attempt) != nil || record == nil ||
		record.Validate() != nil ||
		record.Ack == nil ||
		!workexecution.SameAssignment(record.Attempt, input.Attempt) {
		return workexecution.Input{}, errs.New(
			errs.KindStateConflict,
			"Release hook has no acknowledged Script cleanup record",
		)
	}
	request := record.Report
	if request.ScriptExecutionId != hook.ExecutionId || request.OperationId != input.Release.OperationId ||
		request.InputId != hook.Input.InputId ||
		!bytes.Equal(request.InputSha256, hook.Input.Sha256) ||
		request.State != agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN ||
		request.Sequence <= report.Sequence ||
		request.RecordedAt.AsTime().Before(report.Progress.UpdatedAt) {
		return workexecution.Input{}, errs.New(
			errs.KindStateConflict,
			"Script cleanup receipt does not match the active Release hook",
		)
	}
	return workexecution.Input{ID: record.Ack.EvidenceId, SHA256: hex.EncodeToString(record.Ack.EvidenceSha256)}, nil
}
