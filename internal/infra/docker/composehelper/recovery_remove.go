package composehelper

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RemoveRecoveryCandidate removes only the failed Release's verified container
// IDs. It never invokes Compose down, expands dependencies, or removes Volumes.
// On resumed delivery inspectOnly proves the result without replaying removal.
func RemoveRecoveryCandidate(ctx context.Context, taskRunner runner.Runner, input releaseexecution.RecoveryInput, report releaseexecution.RecoveryReport, inspectOnly bool) error {
	if ctx == nil || taskRunner == nil || report.Validate(input) != nil || report.Ack == nil ||
		!report.Progress.InFlight || report.Progress.Phase != releaseexecution.RemoveFailedCandidate {
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
	if err := recoveryPredecessorServing(bounded, taskRunner, artifacts, inventory); err != nil {
		return err
	}
	selected := slices.Clone(inventory.candidates)
	// A first deployment has no predecessor proxy to preserve. Its exact
	// candidate proxy must also disappear before the absent state is restored.
	if artifacts.Prior == nil && inventory.proxyID != "" {
		selected = append(selected, inventory.proxyID)
	}
	if inspectOnly && len(selected) != 0 {
		return errs.New(errs.KindStateConflict, "failed Release containers remain; candidate removal is incomplete")
	}
	if !inspectOnly && len(selected) != 0 {
		if artifacts.Prior != nil && artifacts.Prior.Proxy != nil {
			if err := waitRestorationDrain(bounded); err != nil {
				return err
			}
		}
		for _, id := range selected {
			currentArtifacts, current, err := recoveryRuntimeInventory(bounded, taskRunner, input)
			if err != nil {
				return err
			}
			if err := recoveryPredecessorServing(bounded, taskRunner, currentArtifacts, current); err != nil {
				return err
			}
			if !slices.Contains(current.candidates, id) && !(currentArtifacts.Prior == nil && current.proxyID == id) {
				return errs.New(errs.KindStateConflict, "failed Release container ownership changed before removal")
			}
			if err := stopRecoveryContainer(bounded, taskRunner, id); err != nil {
				return err
			}
		}
	}
	artifacts, inventory, err = recoveryRuntimeInventory(bounded, taskRunner, input)
	if err != nil {
		return err
	}
	if len(inventory.candidates) != 0 || artifacts.Prior == nil && inventory.proxyID != "" {
		return errs.New(errs.KindStateConflict, "failed Release runtime is still present after removal")
	}
	// The retained inactive slot may share its Compose name with the failed
	// candidate. Restore it only after that exact candidate has been removed.
	if err := recoveryRetained(bounded, taskRunner, artifacts, input.Input.Release.ServiceId, inspectOnly); err != nil {
		return err
	}
	return recoveryPredecessorServing(bounded, taskRunner, artifacts, inventory)
}

func recoveryPredecessorServing(ctx context.Context, taskRunner runner.Runner, artifacts executionplan.ReleaseWorkArtifacts, inventory servingInventory) error {
	if artifacts.Prior == nil {
		if len(inventory.predecessors) != 0 {
			return foreignRestorationRuntime()
		}
		return nil
	}
	prior := artifacts.Prior
	ready, err := servingMemberProven(ctx, taskRunner, prior.Artifact, prior.Workloads, prior.Proxy)
	if err != nil {
		return err
	}
	if !ready {
		return errs.New(errs.KindStateConflict, "captured previous workload is not ready; failed candidate cannot be removed")
	}
	if prior.Proxy != nil {
		if inventory.proxyID == "" {
			return errs.New(errs.KindStateConflict, "captured previous proxy is missing")
		}
		proven, err := restorationProxyProven(ctx, taskRunner, inventory.proxyID, prior.Proxy, false)
		if err != nil {
			return err
		}
		if !proven {
			return errs.New(errs.KindStateConflict, "live and restart routing have not returned to the captured previous workload")
		}
	}
	return nil
}
