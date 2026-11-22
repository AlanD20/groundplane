package runtime

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"math"

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
	identity := workexecution.Identity{
		Executor:   workexecution.Agent,
		HostID:     binding.HostID,
		AgentID:    agentID,
		Generation: generation,
	}
	ready, retained, err := owner.queue.HostUpgradeWindow(ctx, identity, binding.ReadRevision)
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
	if binding.HostID != identity.HostID {
		return errs.New(errs.KindStateConflict, "Agent host binding changed during execution qualification")
	}
	ready, _, err = owner.queue.HostUpgradeWindow(ctx, identity, binding.ReadRevision)
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

// RequireReplacementRollbackReady is only for predecessor recovery authorized by
// the matching native activation journal. That journal proves original execution
// quiescence and preserves closed admission across candidate startup. No command
// was admitted to the candidate, so an offline candidate need not answer a probe.
func (owner *localAgentWork) RequireReplacementRollbackReady(
	ctx context.Context,
	agentID string,
	original, current uint64,
	maximum int32,
) error {
	if err := validateWorkBound(maximum); err != nil {
		return err
	}
	if original == 0 || current <= original || current-original != 1 {
		return errs.New(errs.KindStateConflict, "Agent rollback generation differs from its prepared update")
	}
	if err := owner.sessions.RequireAssignmentPause(ctx, agentID, math.MaxUint64); err != nil {
		return err
	}
	binding, err := owner.agents.MaintenanceBinding(ctx, agentID, current)
	if err != nil {
		return err
	}
	identity := workexecution.Identity{
		Executor:   workexecution.Agent,
		HostID:     binding.HostID,
		AgentID:    agentID,
		Generation: original,
	}
	ready, _, err := owner.queue.HostUpgradeWindow(ctx, identity, binding.ReadRevision)
	if err != nil {
		return err
	}
	if !ready {
		return errs.New(
			errs.KindResourceInUse,
			"Agent rollback is blocked by work outside the original quiescence proof; ownership is retained",
		)
	}
	return owner.sessions.RequireAssignmentPause(ctx, agentID, math.MaxUint64)
}
