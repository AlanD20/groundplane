package hierarchyruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type AttachCheckpoints interface {
	Resume(context.Context) error
	Current(context.Context) (*attachexecution.RemovalProgress, error)
	Checkpoint(context.Context, attachexecution.RemovalProgress) error
}

// AttachHost runs only the captured command against a verified managed backing
// container. inspectOnly may return a retained exact result, but must never
// execute an ambiguous command again or infer completion from process absence.
type AttachHost interface {
	ExecuteRemovalCommand(
		context.Context,
		attachexecution.RemovalInput,
		attachexecution.RemovalProgress,
		bool,
	) (attachexecution.RemovalCommandResult, error)
	InspectRemoval(context.Context, attachexecution.RecoveryInput) (attachexecution.RemovalInspection, error)
}

func RunAttachRemoval(ctx context.Context, input attachexecution.RemovalInput, checkpoints AttachCheckpoints,
	host AttachHost,
) (*attachexecution.RemovalProgress, error) {
	if ctx == nil || checkpoints == nil || host == nil || input.Validate() != nil || input.Attempt.StartedAt == nil {
		return nil, errs.New(
			errs.KindInternal,
			"Attach removal runtime requires its captured input, checkpoints and host executor",
		)
	}
	if err := checkpoints.Resume(ctx); err != nil {
		return nil, err
	}
	progress, err := checkpoints.Current(ctx)
	if err != nil {
		return nil, err
	}
	if progress == nil {
		initial, err := attachexecution.NewRemovalProgress(input, *input.Attempt.StartedAt)
		if err != nil {
			return nil, err
		}
		progress = &initial
	}
	if err := progress.Validate(input); err != nil {
		return nil, err
	}
	// The captured grant count bounds this loop. A failed or ambiguous command
	// returns immediately; no automatic command or execution-attempt retry.
	for remaining := input.CommandCount(); remaining > 0 && !progress.Complete(input); remaining-- {
		if err := ctx.Err(); err != nil {
			return progress, err
		}
		inspectOnly := progress.InFlight
		if !inspectOnly {
			next, err := progress.Begin(input, time.Now().UTC())
			if err != nil {
				return progress, err
			}
			if err := checkpoints.Checkpoint(ctx, next); err != nil {
				return progress, err
			}
			progress, err = acknowledgedAttachProgress(ctx, checkpoints, input, next)
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
		result, effectErr := host.ExecuteRemovalCommand(effectContext, input, *progress, inspectOnly)
		cancel()
		if effectErr != nil {
			return progress, effectErr
		}
		next, err := progress.Confirm(input, result, time.Now().UTC())
		if err != nil {
			return progress, err
		}
		if err := checkpoints.Checkpoint(ctx, next); err != nil {
			return progress, err
		}
		progress, err = acknowledgedAttachProgress(ctx, checkpoints, input, next)
		if err != nil {
			return nil, err
		}
	}
	if !progress.Complete(input) {
		return progress, errs.New(
			errs.KindStateConflict,
			"Attach removal ended before every captured command completed",
		)
	}
	return progress, nil
}

func acknowledgedAttachProgress(ctx context.Context, checkpoints AttachCheckpoints, input attachexecution.RemovalInput,
	expected attachexecution.RemovalProgress,
) (*attachexecution.RemovalProgress, error) {
	actual, err := checkpoints.Current(ctx)
	if err != nil {
		return nil, err
	}
	if actual == nil {
		return nil, errs.New(errs.KindStateConflict, "Attach removal lost its acknowledged command boundary")
	}
	want, err := (attachexecution.RemovalReport{Progress: expected}).Checkpoint(input)
	if err != nil {
		return nil, err
	}
	got, err := (attachexecution.RemovalReport{Progress: *actual}).Checkpoint(input)
	if err != nil || got.Sequence != want.Sequence || got.Evidence != want.Evidence || got.StageID != want.StageID ||
		!got.RecordedAt.Equal(want.RecordedAt) {
		return nil, errs.New(
			errs.KindStateConflict,
			"Attach removal journal differs from the acknowledged command boundary",
		)
	}
	return actual, nil
}
