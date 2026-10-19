package composehelper

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// VerifyRecoveryRuntime is inspection only. Earlier successful commands or
// checkpoints cannot substitute for final exact ownership, readiness, retained
// runtime and live/restart routing observations. File proof is separate.
func VerifyRecoveryRuntime(
	ctx context.Context,
	taskRunner runner.Runner,
	input releaseexecution.RecoveryInput,
	report releaseexecution.RecoveryReport,
) error {
	if ctx == nil || taskRunner == nil || report.Validate(input) != nil || report.Ack == nil ||
		!report.Progress.InFlight || report.Progress.Phase != releaseexecution.VerifyRestoration {
		return invalidReleaseHelper()
	}
	bounded, cancel := context.WithDeadline(ctx, input.Input.Attempt.Deadline)
	defer cancel()
	if err := bounded.Err(); err != nil {
		return err
	}
	artifacts, inventory, err := recoveryRuntimeInventory(bounded, taskRunner, input)
	if err != nil {
		return err
	}
	if len(inventory.candidates) != 0 || artifacts.Prior == nil && inventory.proxyID != "" {
		return errs.New(errs.KindStateConflict, "failed Release runtime remains during final recovery verification")
	}
	if err := recoveryRetained(bounded, taskRunner, artifacts, input.Input.Release.ServiceId, true); err != nil {
		return err
	}
	return recoveryPredecessorServing(bounded, taskRunner, artifacts, inventory)
}
