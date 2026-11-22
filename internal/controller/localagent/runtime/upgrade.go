package runtime

import (
	"context"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// RequireUpgradeReady preserves unresolved ownership. Only captured Entry work
// has a supported quiescence proof; other retained operations remain blocking.
// The caller holds assignment admission closed until activation or rejection.
func (owner *localAgentWork) RequireUpgradeReady(
	ctx context.Context,
	agentID string,
	generation uint64,
	maximum int32,
) error {
	if err := validateWorkBound(maximum); err != nil {
		return err
	}
	binding, err := owner.agents.MaintenanceBinding(ctx, agentID, generation)
	if err != nil {
		return err
	}
	ready, retained, err := owner.queue.HostUpgradeWindow(ctx, binding.HostID, binding.ReadRevision)
	if err != nil {
		return err
	}
	if !ready {
		return errs.New(
			errs.KindResourceInUse,
			"Agent update is blocked by active work or unsupported recovery; ownership is retained",
		)
	}
	if !retained {
		return nil
	}
	if err := owner.sessions.CheckExecutionQuiescence(ctx, agentID, generation); err != nil {
		return err
	}
	// Reject a concurrent Retry or binding change after the fresh process proof.
	binding, err = owner.agents.MaintenanceBinding(ctx, agentID, generation)
	if err != nil {
		return err
	}
	ready, _, err = owner.queue.HostUpgradeWindow(ctx, binding.HostID, binding.ReadRevision)
	if err != nil {
		return err
	}
	if !ready {
		return errs.New(
			errs.KindResourceInUse,
			"Agent work changed during upgrade qualification; ownership is retained",
		)
	}
	return nil
}
