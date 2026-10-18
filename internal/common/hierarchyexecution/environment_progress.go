package hierarchyexecution

import (
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type EnvironmentPhase string

const (
	RemoveRuntime      EnvironmentPhase = "remove_runtime"
	RemoveDirectory    EnvironmentPhase = "remove_directory"
	EnvironmentRemoved EnvironmentPhase = "complete"
)

// EnvironmentObservation is produced by host inspection, not inferred from a
// successful subprocess exit. Both absence checks are required at completion.
type EnvironmentObservation struct {
	EnvironmentID   string              `json:"environment_id"`
	Source          workexecution.Input `json:"source"`
	RuntimeAbsent   bool                `json:"runtime_absent"`
	DirectoryAbsent bool                `json:"directory_absent"`
	At              time.Time           `json:"at"`
}

type EnvironmentProgress struct {
	AttemptID   string                  `json:"attempt_id"`
	Sequence    uint64                  `json:"sequence"`
	Phase       EnvironmentPhase        `json:"phase"`
	InFlight    bool                    `json:"in_flight"`
	BeganAt     *time.Time              `json:"began_at,omitempty"`
	UpdatedAt   time.Time               `json:"updated_at"`
	Observation *EnvironmentObservation `json:"observation,omitempty"`
}

func NewEnvironmentProgress(input EnvironmentInput, at time.Time) (EnvironmentProgress, error) {
	progress := EnvironmentProgress{AttemptID: input.Attempt.ID, Phase: RemoveRuntime, UpdatedAt: at.UTC()}
	if !at.Before(input.Attempt.Deadline) {
		return EnvironmentProgress{}, invalidEnvironmentProgress()
	}
	return progress, progress.Validate(input)
}

func (progress EnvironmentProgress) Validate(input EnvironmentInput) error {
	if input.Validate() != nil || input.Attempt.StartedAt == nil ||
		executionplan.ValidateEnvironmentRemovalInput(input.Removal) != nil || progress.AttemptID != input.Attempt.ID ||
		progress.Sequence > 4 || progress.UpdatedAt.IsZero() || progress.UpdatedAt.Before(*input.Attempt.StartedAt) ||
		progress.InFlight != (progress.Sequence%2 == 1) || progress.InFlight != (progress.BeganAt != nil) {
		return invalidEnvironmentProgress()
	}
	if progress.BeganAt != nil && (progress.BeganAt.Before(*input.Attempt.StartedAt) || !progress.BeganAt.Before(input.Attempt.Deadline) || progress.BeganAt.After(progress.UpdatedAt)) {
		return invalidEnvironmentProgress()
	}
	expected := RemoveRuntime
	if progress.Sequence >= 2 {
		expected = RemoveDirectory
	}
	if progress.Sequence == 4 {
		expected = EnvironmentRemoved
	}
	if progress.Phase != expected || (progress.Observation != nil) != (progress.Sequence >= 2) {
		return invalidEnvironmentProgress()
	}
	if progress.Observation != nil {
		observation := progress.Observation
		if observation.EnvironmentID != input.Removal.EnvironmentId || observation.Source.Validate() != nil ||
			observation.Source.ID != input.Removal.CapturedSource.InputId || observation.Source.SHA256 != hex.EncodeToString(input.Removal.CapturedSource.Sha256) ||
			!observation.RuntimeAbsent || progress.Sequence == 4 && !observation.DirectoryAbsent ||
			observation.At.IsZero() || observation.At.Before(*input.Attempt.StartedAt) || observation.At.After(progress.UpdatedAt) {
			return invalidEnvironmentProgress()
		}
	}
	return nil
}

// Begin must be durably acknowledged before its mutation. Replaying an in-flight
// cursor requires inspection; it is not permission to begin the effect again.
func (progress EnvironmentProgress) Begin(input EnvironmentInput, at time.Time) (EnvironmentProgress, error) {
	if progress.Validate(input) != nil || progress.InFlight || progress.Phase == EnvironmentRemoved ||
		at.Before(progress.UpdatedAt) || !at.Before(input.Attempt.Deadline) {
		return EnvironmentProgress{}, invalidEnvironmentProgress()
	}
	next := progress
	began := at.UTC()
	next.Sequence++
	next.InFlight, next.BeganAt, next.UpdatedAt = true, &began, began
	return next, next.Validate(input)
}

func (progress EnvironmentProgress) Confirm(input EnvironmentInput, observation EnvironmentObservation, at time.Time) (EnvironmentProgress, error) {
	if progress.Validate(input) != nil || !progress.InFlight || at.Before(progress.UpdatedAt) ||
		observation.At.Before(*progress.BeganAt) || observation.At.After(at) {
		return EnvironmentProgress{}, invalidEnvironmentProgress()
	}
	next := progress
	next.Sequence++
	next.InFlight, next.BeganAt, next.UpdatedAt = false, nil, at.UTC()
	next.Observation = &observation
	if progress.Phase == RemoveRuntime {
		next.Phase = RemoveDirectory
	} else {
		next.Phase = EnvironmentRemoved
	}
	return next, next.Validate(input)
}

func invalidEnvironmentProgress() error {
	return errs.New(errs.KindStateConflict, "Environment removal progress does not prove its required execution and absence checks")
}
