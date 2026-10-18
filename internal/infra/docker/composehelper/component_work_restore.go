package composehelper

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/runner"
)

func restoreComponentRuntime(ctx context.Context, taskRunner runner.Runner, input componentexecution.HelperInput) error {
	if input.Recovery == nil {
		return invalidComponentInventory()
	}
	if input.Procedure.Predecessor.Absent {
		return removeComponentRuntime(ctx, taskRunner, input)
	}
	inventory, err := inspectComponentWork(ctx, taskRunner, input)
	if err != nil {
		return err
	}
	prior := input.Procedure.Predecessor.Runtime
	ready := false
	for _, container := range append(inventory.Candidate, inventory.Prior...) {
		if componentWorkMatches(container, prior) {
			ready = componentWorkReady([]componentWorkContainer{container})
			continue
		}
		if err := removeComponentContainer(ctx, taskRunner, container.ID); err != nil {
			return err
		}
	}
	if !ready {
		if err := applySingletonArtifact(ctx, taskRunner, prior); err != nil {
			return err
		}
	}
	return waitComponentRuntime(ctx, taskRunner, input, true)
}

func waitComponentRuntime(ctx context.Context, taskRunner runner.Runner, input componentexecution.HelperInput, predecessor bool) error {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		inventory, err := inspectComponentWork(ctx, taskRunner, input)
		if err != nil {
			return err
		}
		containers := append(inventory.Candidate, inventory.Prior...)
		absent := input.Procedure.Remove
		artifact := input.Procedure.Candidate
		if predecessor {
			absent, artifact = input.Procedure.Predecessor.Absent, input.Procedure.Predecessor.Runtime
		}
		if absent && len(containers) == 0 {
			return nil
		}
		if !absent && len(containers) == 1 && componentWorkMatches(containers[0], artifact) && componentWorkReady(containers) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
