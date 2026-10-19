package releaseruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// WaitDrain consumes the already captured drain window. Reconnect does not
// renew it. This proves only that the window elapsed, not that the old workload
// disappeared or that a connection survived; removal has its own boundary.
func WaitDrain(ctx context.Context, input releaseexecution.Input, report releaseexecution.CheckpointReport) error {
	if ctx == nil || report.Ack == nil || report.ValidateEnvelope(input.Attempt) != nil ||
		report.Progress.Validate(
			input,
		) != nil || !report.Progress.InFlight || report.Progress.Phase != releaseexecution.Rollout ||
		report.Progress.Rolling == nil || report.Progress.Rolling.Phase != rollingrelease.DrainPredecessor {
		return errs.New(errs.KindStateConflict, "Release has no acknowledged rolling drain boundary")
	}
	stage, err := report.Progress.StageID(input)
	if err != nil || stage != report.StageID {
		return errs.New(errs.KindStateConflict, "rolling drain receipt names another action")
	}
	cursor := report.Progress.Rolling
	deadline := input.Attempt.Deadline
	if cursor.ReplicaDeadline.Before(deadline) {
		deadline = cursor.ReplicaDeadline
	}
	bounded, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	if err := bounded.Err(); err != nil {
		return err
	}
	remaining := time.Until(cursor.DrainUntil)
	if remaining > 0 {
		timer := time.NewTimer(remaining)
		defer timer.Stop()
		select {
		case <-bounded.Done():
			return bounded.Err()
		case <-timer.C:
		}
	}
	if err := bounded.Err(); err != nil {
		return err
	}
	if !time.Now().Before(deadline) {
		return errs.New(errs.KindStateConflict, "rolling replica deadline expired before drain completion")
	}
	return nil
}
