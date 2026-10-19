package releaseruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Checkpoints persists before sending and returns only durably acknowledged
// cursors. Script checkpoints share the same attempt sequence underneath it.
type Checkpoints interface {
	Resume(context.Context) error
	Current(context.Context) (*releaseexecution.CheckpointReport, error)
	Checkpoint(context.Context, releaseexecution.Progress) error
}

// Effects is the Release owner's closed phase dispatcher, not a generic step
// engine. Execute may cross a freshly granted boundary; Inspect must account for
// an uncertain existing boundary without blindly repeating commands or hooks.
// Both publish immutable operation evidence before returning its reference.
type Effects interface {
	Execute(context.Context, releaseexecution.Input, releaseexecution.CheckpointReport) (workexecution.Input, error)
	Inspect(context.Context, releaseexecution.Input, releaseexecution.CheckpointReport) (workexecution.Input, error)
}

// Run keeps failure separate from recovery. A failed or uncertain effect leaves
// its in-flight cursor intact and returns to the attempt owner; it cannot turn
// cleanup into success, create a new attempt or retry indefinitely.
func Run(
	ctx context.Context,
	input releaseexecution.Input,
	checkpoints Checkpoints,
	effects Effects,
) (*releaseexecution.CheckpointReport, error) {
	if ctx == nil || checkpoints == nil || effects == nil || input.Attempt.StartedAt == nil || input.Release == nil {
		return nil, errs.New(
			errs.KindInternal,
			"Release execution requires a started input, durable checkpoints and effects",
		)
	}
	bounded, cancel := context.WithDeadline(ctx, input.Attempt.Deadline)
	defer cancel()
	if err := checkpoints.Resume(bounded); err != nil {
		return nil, err
	}
	report, err := checkpoints.Current(bounded)
	if err != nil {
		return nil, err
	}
	for {
		if err := bounded.Err(); err != nil {
			return report, err
		}
		var current releaseexecution.Progress
		if report == nil {
			current, err = releaseexecution.NewProgress(input, time.Now().UTC())
		} else {
			if report.Ack == nil {
				return report, errs.New(errs.KindStateConflict, "Release execution cannot use an unacknowledged cursor")
			}
			current = report.Progress
			err = report.ValidateEnvelope(input.Attempt)
			if err == nil {
				err = current.Validate(input)
			}
		}
		if err != nil {
			return report, err
		}
		if current.Phase == releaseexecution.Complete {
			return report, nil
		}
		inspect := current.InFlight
		if !inspect {
			if current.Rolling == nil {
				current, err = current.Begin(input, time.Now().UTC())
			} else {
				current, err = current.BeginReplica(input, time.Now().UTC())
			}
			if err != nil {
				return report, err
			}
			if err := checkpoints.Checkpoint(bounded, current); err != nil {
				return report, err
			}
			report, err = acknowledgedCursor(bounded, checkpoints, input, current)
			if err != nil {
				return report, err
			}
		}
		var evidence workexecution.Input
		if inspect {
			evidence, err = effects.Inspect(bounded, input, *report)
		} else {
			evidence, err = effects.Execute(bounded, input, *report)
		}
		if err != nil {
			return report, err
		}
		if current.Rolling == nil {
			current, err = current.Confirm(input, evidence, time.Now().UTC())
		} else {
			current, err = current.ConfirmReplica(input, current.Rolling.Phase, evidence, time.Now().UTC())
		}
		if err != nil {
			return report, err
		}
		if err := checkpoints.Checkpoint(bounded, current); err != nil {
			return report, err
		}
		report, err = acknowledgedCursor(bounded, checkpoints, input, current)
		if err != nil {
			return report, err
		}
	}
}

func acknowledgedCursor(
	ctx context.Context,
	checkpoints Checkpoints,
	input releaseexecution.Input,
	expected releaseexecution.Progress,
) (*releaseexecution.CheckpointReport, error) {
	report, err := checkpoints.Current(ctx)
	if err != nil {
		return nil, err
	}
	if report == nil || report.Ack == nil || report.ValidateEnvelope(input.Attempt) != nil ||
		!releaseexecution.EqualProgress(report.Progress, expected) {
		return nil, errs.New(errs.KindStateConflict, "Release checkpoint did not acknowledge the requested cursor")
	}
	return report, nil
}
