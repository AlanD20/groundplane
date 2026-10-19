package releaseruntime

import (
	"bytes"
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (hooks *Hooks) RunRecovery(
	ctx context.Context,
	input releaseexecution.RecoveryInput,
	report releaseexecution.RecoveryReport,
) (releaseexecution.FailureHookProof, error) {
	hook, err := report.Progress.ActiveHook(input)
	if err != nil || report.Validate(input) != nil || report.Ack == nil {
		return releaseexecution.FailureHookProof{}, errs.New(
			errs.KindStateConflict,
			"failure hook requires its acknowledged post-restoration boundary",
		)
	}
	delivery, err := checkpointdelivery.NewScript(input.Input.Attempt, hooks.journal, hooks.exchange)
	if err != nil {
		return releaseexecution.FailureHookProof{}, err
	}
	if err := delivery.Resume(ctx); err != nil {
		return releaseexecution.FailureHookProof{}, err
	}
	captured, err := hooks.loader.LoadFailureHook(ctx, input.Input, hook)
	if err != nil {
		return releaseexecution.FailureHookProof{}, err
	}
	if captured.InterruptedCleanup != nil {
		return releaseexecution.FailureHookProof{
			ExecutionID:        hook.ExecutionId,
			InterruptedCleanup: captured.InterruptedCleanup,
		}, nil
	}
	if captured.Invocation == nil {
		return releaseexecution.FailureHookProof{}, errs.New(
			errs.KindStateConflict,
			"failure hook has no captured invocation",
		)
	}
	serving := input.HookServingReleaseID()
	mayStart := serving != "" && captured.Invocation.Run != nil && captured.Invocation.Run.ReleaseId == serving
	record, executionErr := hooks.executeInvocation(ctx, input.Input, hook, *captured.Invocation, delivery, mayStart)
	if record == nil || record.Validate() != nil || record.Ack == nil ||
		!workexecution.SameAssignment(record.Attempt, input.Input.Attempt) {
		if executionErr != nil {
			return releaseexecution.FailureHookProof{}, executionErr
		}
		return releaseexecution.FailureHookProof{}, errs.New(
			errs.KindStateConflict,
			"failure hook has no acknowledged cleanup record",
		)
	}
	request := record.Report
	if request.ScriptExecutionId != hook.ExecutionId || request.OperationId != input.Input.Release.OperationId ||
		request.InputId != hook.Input.InputId || !bytes.Equal(request.InputSha256, hook.Input.Sha256) ||
		request.State != agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN ||
		request.Sequence <= report.Sequence || request.RecordedAt.AsTime().Before(report.Progress.UpdatedAt) {
		if executionErr != nil {
			return releaseexecution.FailureHookProof{}, executionErr
		}
		return releaseexecution.FailureHookProof{}, errs.New(
			errs.KindStateConflict,
			"failure hook cleanup does not match its captured execution",
		)
	}
	// Preserve the Script's own failed outcome without replacing the original
	// deployment failure. Only acknowledged cleanup permits recovery to proceed.
	return releaseexecution.FailureHookProof{ExecutionID: hook.ExecutionId,
		Checkpoint: &workexecution.Input{
			ID:     record.Ack.EvidenceId,
			SHA256: hex.EncodeToString(record.Ack.EvidenceSha256),
		}}, nil
}
