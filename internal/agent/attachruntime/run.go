package attachruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Checkpoints interface {
	Resume(context.Context) error
	Current(context.Context) (*attachexecution.ProvisionProgress, error)
	Checkpoint(context.Context, attachexecution.ProvisionProgress) error
}

// Host runs only the captured command against a verified managed backing
// container. inspectOnly may return a retained exact result, but must never
// execute an ambiguous command again or infer completion from process absence.
type Host interface {
	InspectProvision(
		context.Context,
		attachexecution.ProvisionRecoveryInput,
	) (attachexecution.ProvisionInspection, error)
	ExecuteProvisionCommand(
		context.Context,
		attachexecution.ProvisionInput,
		attachexecution.ProvisionProgress,
		bool,
		FactStore,
	) (attachexecution.ProvisionCommandResult, error)
}

func Run(ctx context.Context, input attachexecution.ProvisionInput, checkpoints Checkpoints,
	host Host, facts FactStore,
) (*attachexecution.ProvisionProgress, error) {
	if ctx == nil || checkpoints == nil || host == nil || facts == nil || input.Validate() != nil ||
		input.Attempt.StartedAt == nil {
		return nil, errs.New(
			errs.KindInternal,
			"Attach provisioning runtime requires its captured input, checkpoints and host executor",
		)
	}
	authority, err := input.Authority()
	if err != nil {
		return nil, err
	}
	if err := checkpoints.Resume(ctx); err != nil {
		return nil, err
	}
	progress, err := checkpoints.Current(ctx)
	if err != nil {
		return nil, err
	}
	if progress == nil {
		initial, err := attachexecution.NewProvisionProgress(input.Attempt, authority, *input.Attempt.StartedAt)
		if err != nil {
			return nil, err
		}
		progress = &initial
	}
	if err := progress.Validate(input.Attempt, authority); err != nil {
		return nil, err
	}
	// The captured grant count bounds this loop. A failed or ambiguous command
	// returns immediately; no automatic command or execution-attempt retry.
	for remaining := authority.Commands; remaining > 0 && !progress.Complete(input.Attempt, authority); remaining-- {
		if err := ctx.Err(); err != nil {
			return progress, err
		}
		inspectOnly := progress.InFlight
		if !inspectOnly {
			next, err := progress.Begin(input.Attempt, authority, time.Now().UTC())
			if err != nil {
				return progress, err
			}
			if err := checkpoints.Checkpoint(ctx, next); err != nil {
				return progress, err
			}
			progress, err = acknowledgedProgress(ctx, checkpoints, input, next)
			if err != nil {
				return nil, err
			}
		}
		if !time.Now().UTC().Before(input.Attempt.Deadline) {
			inspectOnly = true
		}
		var effectContext context.Context
		var cancel context.CancelFunc
		if !inspectOnly {
			effectContext, cancel = context.WithDeadline(ctx, input.Attempt.Deadline)
		} else {
			// Reconciliation can run after the execution deadline, but a stalled
			// Docker inspection must not occupy the Agent worker indefinitely.
			effectContext, cancel = context.WithTimeout(ctx, 15*time.Second)
		}
		result, effectErr := host.ExecuteProvisionCommand(effectContext, input, *progress, inspectOnly, facts)
		cancel()
		if effectErr != nil {
			return progress, effectErr
		}
		next, err := progress.Confirm(input.Attempt, authority, result, time.Now().UTC())
		if err != nil {
			return progress, err
		}
		if err := checkpoints.Checkpoint(ctx, next); err != nil {
			return progress, err
		}
		progress, err = acknowledgedProgress(ctx, checkpoints, input, next)
		if err != nil {
			return nil, err
		}
	}
	if !progress.Complete(input.Attempt, authority) {
		return progress, errs.New(
			errs.KindStateConflict,
			"Attach provisioning ended before every captured command completed",
		)
	}
	return progress, nil
}

func acknowledgedProgress(ctx context.Context, checkpoints Checkpoints, input attachexecution.ProvisionInput,
	expected attachexecution.ProvisionProgress,
) (*attachexecution.ProvisionProgress, error) {
	authority, err := input.Authority()
	if err != nil {
		return nil, err
	}
	actual, err := checkpoints.Current(ctx)
	if err != nil {
		return nil, err
	}
	if actual == nil {
		return nil, errs.New(errs.KindStateConflict, "Attach provisioning lost its acknowledged command boundary")
	}
	want, err := (attachexecution.ProvisionReport{Progress: expected}).CheckpointAuthority(input.Attempt, authority)
	if err != nil {
		return nil, err
	}
	got, err := (attachexecution.ProvisionReport{Progress: *actual}).CheckpointAuthority(input.Attempt, authority)
	if err != nil || got.Sequence != want.Sequence || got.Evidence != want.Evidence || got.StageID != want.StageID ||
		!got.RecordedAt.Equal(want.RecordedAt) {
		return nil, errs.New(
			errs.KindStateConflict,
			"Attach provisioning journal differs from the acknowledged command boundary",
		)
	}
	return actual, nil
}
