package releaseexecution

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// BeginReplica persists the same per-replica deadline across health polling,
// traffic changes and reconnect. In-flight phases cannot obtain another start.
func (progress Progress) BeginReplica(input Input, at time.Time) (Progress, error) {
	if err := progress.Validate(input); err != nil {
		return Progress{}, err
	}
	if progress.Phase != Rollout || progress.Rolling == nil || progress.InFlight ||
		at.Before(progress.UpdatedAt) || progress.Sequence == ^uint64(0) {
		return Progress{}, invalidProgress()
	}
	next, err := progress.Rolling.Begin(*input.Artifacts.Rolling, input.Attempt.Deadline, at)
	if err != nil {
		return Progress{}, err
	}
	started := at.UTC()
	progress.Sequence++
	progress.Rolling, progress.InFlight, progress.BeganAt, progress.UpdatedAt = &next, true, &started, started
	return progress, progress.Validate(input)
}

func (progress Progress) ConfirmReplica(input Input, phase rollingrelease.Phase, evidence workexecution.Input, at time.Time) (Progress, error) {
	if err := progress.Validate(input); err != nil {
		return Progress{}, err
	}
	if progress.Phase != Rollout || progress.Rolling == nil || !progress.InFlight || evidence.Validate() != nil ||
		at.Before(progress.UpdatedAt) || progress.Sequence == ^uint64(0) {
		return Progress{}, invalidProgress()
	}
	next, err := progress.Rolling.Confirm(*input.Artifacts.Rolling, input.Attempt.Deadline, phase, at)
	if err != nil {
		return Progress{}, err
	}
	progress.Sequence++
	progress.Rolling, progress.InFlight, progress.BeganAt = &next, false, nil
	progress.UpdatedAt, progress.Evidence = at.UTC(), &evidence
	if next.Phase == "" {
		progress.Rolling, progress.Action = nil, VerifyServing
	}
	return progress, progress.Validate(input)
}
