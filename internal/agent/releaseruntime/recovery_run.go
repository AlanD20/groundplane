package releaseruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type RecoveryCheckpoints interface {
	Resume(context.Context) error
	Current(context.Context) (*releaseexecution.RecoveryReport, error)
	Checkpoint(context.Context, releaseexecution.RecoveryProgress) error
}

type RecoveryEffects interface {
	ExecuteRecovery(
		context.Context,
		releaseexecution.RecoveryInput,
		releaseexecution.RecoveryReport,
	) (workexecution.Input, error)
	InspectRecovery(
		context.Context,
		releaseexecution.RecoveryInput,
		releaseexecution.RecoveryReport,
	) (workexecution.Input, error)
}

// RunRecovery has no retry loop for failed actions. It advances only after each
// exact checkpoint and effect proof is durably acknowledged. A resumed in-flight
// action is inspection-only; failure retains its cursor for explicit resolution.
func RunRecovery(
	ctx context.Context,
	input releaseexecution.RecoveryInput,
	checkpoints RecoveryCheckpoints,
	effects RecoveryEffects,
) (*releaseexecution.RecoveryReport, error) {
	if ctx == nil || input.Validate() != nil || input.Input.Attempt.StartedAt == nil || checkpoints == nil ||
		effects == nil {
		return nil, errs.New(
			errs.KindInternal,
			"Release recovery requires captured input, a started attempt, checkpoints and effects",
		)
	}
	deliveryContext, finishDelivery := context.WithTimeout(ctx, 15*time.Second)
	if err := checkpoints.Resume(deliveryContext); err != nil {
		finishDelivery()
		return nil, err
	}
	report, err := checkpoints.Current(deliveryContext)
	finishDelivery()
	if err != nil {
		return nil, err
	}
	bounded, cancel := context.WithDeadline(ctx, input.Input.Attempt.Deadline)
	defer cancel()
	for {
		var progress releaseexecution.RecoveryProgress
		if report == nil {
			progress, err = releaseexecution.NewRecoveryProgress(input, time.Now().UTC())
		} else {
			if report.Ack == nil {
				return report, errs.New(errs.KindStateConflict, "Recovery cannot use an unacknowledged checkpoint")
			}
			err = report.Validate(input)
			progress = report.Progress
		}
		if err != nil {
			return report, err
		}
		// Completion replay grants no new host effects and does not need an
		// extended action deadline. Settlement still verifies restoration proof.
		if progress.Phase == releaseexecution.RecoveryComplete {
			return report, nil
		}
		if err := bounded.Err(); err != nil {
			return report, err
		}
		inspect := progress.InFlight
		if !inspect {
			progress, err = progress.Begin(input, time.Now().UTC())
			if err != nil {
				return report, err
			}
			if err := checkpoints.Checkpoint(bounded, progress); err != nil {
				return report, err
			}
			report, err = acknowledgedRecovery(bounded, input, checkpoints, progress)
			if err != nil {
				return report, err
			}
		}
		var evidence workexecution.Input
		if inspect {
			evidence, err = effects.InspectRecovery(bounded, input, *report)
		} else {
			evidence, err = effects.ExecuteRecovery(bounded, input, *report)
		}
		if err != nil {
			return report, err
		}
		progress, err = progress.Confirm(input, evidence, time.Now().UTC())
		if err != nil {
			return report, err
		}
		if err := checkpoints.Checkpoint(bounded, progress); err != nil {
			return report, err
		}
		report, err = acknowledgedRecovery(bounded, input, checkpoints, progress)
		if err != nil {
			return report, err
		}
	}
}

func acknowledgedRecovery(
	ctx context.Context,
	input releaseexecution.RecoveryInput,
	checkpoints RecoveryCheckpoints,
	expected releaseexecution.RecoveryProgress,
) (*releaseexecution.RecoveryReport, error) {
	report, err := checkpoints.Current(ctx)
	if err != nil {
		return nil, err
	}
	if report == nil || report.Ack == nil || report.Validate(input) != nil ||
		!releaseexecution.EqualRecoveryProgress(report.Progress, expected) {
		return nil, errs.New(errs.KindStateConflict, "Recovery checkpoint did not acknowledge the requested action")
	}
	return report, nil
}
