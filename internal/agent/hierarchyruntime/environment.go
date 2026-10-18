// Package hierarchyruntime executes captured hierarchy host work without child
// Tasks or the superseded generic step interpreter.
package hierarchyruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/hierarchyexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type EnvironmentCheckpoints interface {
	Resume(context.Context) error
	Current(context.Context) (*hierarchyexecution.EnvironmentProgress, error)
	Checkpoint(context.Context, hierarchyexecution.EnvironmentProgress) error
}

// EnvironmentHost must check the captured owner and configured filesystem root
// before mutation. inspectOnly never repeats an effect after a lost result.
// Success returns independently observed absence, not a subprocess exit code.
type EnvironmentHost interface {
	RemoveRuntime(context.Context, hierarchyexecution.EnvironmentInput, hierarchyexecution.EnvironmentProgress, bool) (hierarchyexecution.EnvironmentObservation, error)
	RemoveDirectory(context.Context, hierarchyexecution.EnvironmentInput, hierarchyexecution.EnvironmentProgress, bool) (hierarchyexecution.EnvironmentObservation, error)
}

// RunEnvironmentRemoval preserves the last acknowledged progress on errors.
// It does not settle the Task or assume that timeout stopped a host mutation.
func RunEnvironmentRemoval(ctx context.Context, input hierarchyexecution.EnvironmentInput, checkpoints EnvironmentCheckpoints,
	host EnvironmentHost,
) (*hierarchyexecution.EnvironmentProgress, error) {
	if ctx == nil || checkpoints == nil || host == nil || input.Attempt.StartedAt == nil {
		return nil, errs.New(errs.KindInternal, "Environment removal runtime is not configured")
	}
	if err := checkpoints.Resume(ctx); err != nil {
		return nil, err
	}
	progress, err := checkpoints.Current(ctx)
	if err != nil {
		return nil, err
	}
	if progress == nil {
		initial, err := hierarchyexecution.NewEnvironmentProgress(input, *input.Attempt.StartedAt)
		if err != nil {
			return nil, err
		}
		progress = &initial
	}
	if err := progress.Validate(input); err != nil {
		return nil, err
	}
	// Two host effects, each with Begin and Confirm; no unbounded retry loop.
	for remaining := 2; remaining > 0 && progress.Phase != hierarchyexecution.EnvironmentRemoved; remaining-- {
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
			progress, err = acknowledgedEnvironmentProgress(ctx, checkpoints, input, next)
			if err != nil {
				return nil, err
			}
		}
		var observation hierarchyexecution.EnvironmentObservation
		// An expired attempt may only inspect its already-started effect. New
		// mutation receives a deadline-bound context; confirmation uses ctx.
		if !time.Now().UTC().Before(input.Attempt.Deadline) {
			inspectOnly = true
		}
		effectContext := ctx
		cancel := func() {}
		if !inspectOnly {
			effectContext, cancel = context.WithDeadline(ctx, input.Attempt.Deadline)
		}
		switch progress.Phase {
		case hierarchyexecution.RemoveRuntime:
			observation, err = host.RemoveRuntime(effectContext, input, *progress, inspectOnly)
		case hierarchyexecution.RemoveDirectory:
			observation, err = host.RemoveDirectory(effectContext, input, *progress, inspectOnly)
		default:
			err = errs.New(errs.KindStateConflict, "Environment removal has an unknown execution stage")
		}
		cancel()
		if err != nil {
			return progress, err
		}
		next, err := progress.Confirm(input, observation, time.Now().UTC())
		if err != nil {
			return progress, err
		}
		if err := checkpoints.Checkpoint(ctx, next); err != nil {
			return progress, err
		}
		progress, err = acknowledgedEnvironmentProgress(ctx, checkpoints, input, next)
		if err != nil {
			return nil, err
		}
	}
	if progress.Phase != hierarchyexecution.EnvironmentRemoved {
		return progress, errs.New(errs.KindStateConflict, "Environment removal ended before both absence checks completed")
	}
	return progress, nil
}

func acknowledgedEnvironmentProgress(ctx context.Context, checkpoints EnvironmentCheckpoints, input hierarchyexecution.EnvironmentInput,
	expected hierarchyexecution.EnvironmentProgress,
) (*hierarchyexecution.EnvironmentProgress, error) {
	saved, err := checkpoints.Current(ctx)
	if err != nil {
		return nil, err
	}
	if saved == nil || saved.Validate(input) != nil || !hierarchyexecution.EqualEnvironmentProgress(*saved, expected) {
		return nil, errs.New(errs.KindStateConflict, "Environment removal checkpoint changed before host execution")
	}
	return saved, nil
}
