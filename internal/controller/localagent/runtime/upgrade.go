package runtime

import (
	"context"
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
	return owner.requireUpgradeReady(ctx, agentID, generation, maximum, "")
}

func (owner *localAgentWork) RequirePreparedReplacementReady(
	ctx context.Context,
	agentID string,
	generation uint64,
	maximum int32,
	taskID string,
) error {
	return owner.requireUpgradeReady(ctx, agentID, generation, maximum, taskID)
}

func (owner *localAgentWork) requireUpgradeReady(
	ctx context.Context,
	agentID string,
	generation uint64,
	maximum int32,
	preparedTaskID string,
) error {
	if err := validateWorkBound(maximum); err != nil {
		return err
	}
	execution, err := owner.agents.MaintenanceExecution(ctx, agentID, generation)
	if err != nil {
		return err
	}
	ready, retained, err := owner.queue.HostUpgradeWindow(ctx, execution.Identity, execution.Binding.ReadRevision)
	if err != nil {
		return err
	}
	if !ready {
		return errs.New(
			errs.KindResourceInUse,
			"Agent update is blocked by active work or unsupported recovery; ownership is retained",
		)
	}
	if retained {
		if err := owner.sessions.CheckExecutionQuiescenceForProcess(
			ctx,
			agentID,
			generation,
			execution.ProcessGeneration,
		); err != nil {
			return err
		}
	}
	// Reject a concurrent process replacement, Retry or binding change after
	// the fresh process proof.
	confirmed, err := owner.agents.MaintenanceExecution(ctx, agentID, generation)
	if err != nil {
		return err
	}
	if execution.Identity != confirmed.Identity || execution.ProcessGeneration != confirmed.ProcessGeneration ||
		execution.Binding.HostID != confirmed.Binding.HostID {
		return errs.New(errs.KindStateConflict, "Agent executor changed during execution qualification")
	}
	ready, _, err = owner.queue.HostUpgradeWindow(ctx, confirmed.Identity, confirmed.Binding.ReadRevision)
	if err != nil {
		return err
	}
	if !ready {
		return errs.New(
			errs.KindResourceInUse,
			"Agent work changed during upgrade qualification; ownership is retained",
		)
	}
	if preparedTaskID != "" {
		if err := owner.sessions.RequireAssignmentPause(ctx, agentID, generation); err != nil {
			return err
		}
		return owner.agents.BindPreparedReplacementExecution(ctx, preparedTaskID, confirmed)
	}
	return nil
}

// RequireReplacementRollbackReady is only for predecessor recovery authorized by
// the matching native activation journal. That journal proves original execution
// quiescence and preserves closed admission across candidate startup. No command
// was admitted to the candidate, so an offline candidate need not answer a probe.
func (owner *localAgentWork) RequireReplacementRollbackReady(
	ctx context.Context,
	taskID string,
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
	execution, err := owner.agents.PreparedReplacementExecution(ctx, taskID, agentID, original, current)
	if err != nil {
		return err
	}
	ready, _, err := owner.queue.HostUpgradeWindow(ctx, execution.Identity, execution.Binding.ReadRevision)
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

func (owner *localAgentWork) ReleasePreparedReplacement(
	ctx context.Context,
	taskID string,
	agentID string,
	original, current uint64,
) error {
	return owner.agents.ReleasePreparedReplacementExecution(ctx, taskID, agentID, original, current)
}
