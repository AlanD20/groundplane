package componentexecution

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Stage string

const (
	WriteConfiguration    Stage = "write_configuration"
	ReconcileRuntime      Stage = "reconcile_runtime"
	ActivateConfiguration Stage = "activate_configuration"
	RemoveRuntime         Stage = "remove_runtime"
	VerifyRuntime         Stage = "verify_runtime"
	Complete              Stage = "complete"
)

// Observation is produced by the ownership-checking Component executor. It
// confirms one named boundary, not arbitrary caller-provided execution steps.
// Runtime verification distinguishes an exact running candidate from absence.
type Observation struct {
	ComponentID            string              `json:"component_id"`
	Input                  workexecution.Input `json:"input"`
	Stage                  Stage               `json:"stage"`
	ConfigurationWritten   bool                `json:"configuration_written"`
	RuntimeReconciled      bool                `json:"runtime_reconciled"`
	ConfigurationActivated bool                `json:"configuration_activated"`
	RuntimeAbsent          bool                `json:"runtime_absent"`
	RuntimeReady           bool                `json:"runtime_ready"`
	At                     time.Time           `json:"at"`
}

type Progress struct {
	AttemptID   string       `json:"attempt_id"`
	Sequence    uint64       `json:"sequence"`
	Stage       Stage        `json:"stage"`
	InFlight    bool         `json:"in_flight"`
	BeganAt     *time.Time   `json:"began_at,omitempty"`
	UpdatedAt   time.Time    `json:"updated_at"`
	Observation *Observation `json:"observation,omitempty"`
}

func stages(input *agentpb.ComponentWorkInput) []Stage {
	if input == nil {
		return nil
	}
	return (Layout{Remove: input.Remove, WriteFiles: len(input.Files) != 0,
		Activate: input.ConfigurationAction != nil}).stages()
}

func FinalSequence(input *agentpb.ComponentWorkInput) (uint64, error) {
	if err := executionplan.ValidateComponentWorkInput(input); err != nil {
		return 0, err
	}
	return uint64(2 * len(stages(input))), nil
}

func NewProgress(attempt workexecution.Attempt, input *agentpb.ComponentWorkInput, at time.Time) (Progress, error) {
	steps := stages(input)
	if len(steps) == 0 || !at.Before(attempt.Deadline) {
		return Progress{}, invalidProgress()
	}
	progress := Progress{AttemptID: attempt.ID, Stage: steps[0], UpdatedAt: at.UTC()}
	return progress, progress.Validate(attempt, input)
}

func (progress Progress) Validate(attempt workexecution.Attempt, input *agentpb.ComponentWorkInput) error {
	layout, err := CaptureLayout(attempt, input)
	if err != nil {
		return err
	}
	return progress.ValidateLayout(attempt, layout)
}

// ValidateLayout checks retained journal state without loading large artifacts.
// Execution additionally requires Layout.Matches against the assigned input.
func (progress Progress) ValidateLayout(attempt workexecution.Attempt, layout Layout) error {
	// Retained progress remains readable after settlement; only the transient
	// assignment opener requires an unfinished attempt.
	if attempt.Validate() != nil || attempt.Direction != workexecution.Forward || attempt.StartedAt == nil {
		return invalidProgress()
	}
	if layout.Validate(attempt) != nil {
		return invalidProgress()
	}
	steps := layout.stages()
	if progress.AttemptID != attempt.ID || progress.Sequence > uint64(2*len(steps)) ||
		progress.InFlight != (progress.Sequence%2 == 1) || progress.InFlight != (progress.BeganAt != nil) ||
		progress.UpdatedAt.IsZero() || progress.UpdatedAt.Location() != time.UTC || progress.UpdatedAt.Before(*attempt.StartedAt) {
		return invalidProgress()
	}
	if progress.BeganAt != nil && (progress.BeganAt.Before(*attempt.StartedAt) ||
		!progress.BeganAt.Before(attempt.Deadline) || progress.BeganAt.After(progress.UpdatedAt)) {
		return invalidProgress()
	}
	index := progress.Sequence / 2
	expected := Complete
	if index < uint64(len(steps)) {
		expected = steps[index]
	}
	if progress.Stage != expected || (progress.Observation != nil) != (index > 0) {
		return invalidProgress()
	}
	if observed := progress.Observation; observed != nil {
		if observed.ComponentID != layout.ComponentID || observed.Input != attempt.Input || observed.At.IsZero() ||
			observed.At.Before(*attempt.StartedAt) || observed.At.After(progress.UpdatedAt) ||
			!observed.Satisfies(steps[index-1], layout.Remove) {
			return invalidProgress()
		}
	}
	return nil
}

func (progress Progress) Begin(
	attempt workexecution.Attempt,
	input *agentpb.ComponentWorkInput,
	at time.Time,
) (Progress, error) {
	if progress.Validate(attempt, input) != nil || progress.InFlight || progress.Stage == Complete ||
		at.Before(progress.UpdatedAt) || !at.Before(attempt.Deadline) {
		return Progress{}, invalidProgress()
	}
	next, began := progress, at.UTC()
	next.Sequence++
	next.InFlight, next.BeganAt, next.UpdatedAt = true, &began, began
	return next, next.Validate(attempt, input)
}

func (progress Progress) Confirm(
	attempt workexecution.Attempt,
	input *agentpb.ComponentWorkInput,
	observation Observation,
	at time.Time,
) (Progress, error) {
	if progress.Validate(attempt, input) != nil || !progress.InFlight || at.Before(progress.UpdatedAt) ||
		observation.At.Before(
			*progress.BeganAt,
		) || observation.At.After(at) || !observation.Satisfies(progress.Stage, input.Remove) {
		return Progress{}, invalidProgress()
	}
	next := progress
	next.Sequence++
	next.InFlight, next.BeganAt, next.UpdatedAt = false, nil, at.UTC()
	next.Observation, next.Stage = &observation, Complete
	if steps := stages(input); next.Sequence/2 < uint64(len(steps)) {
		next.Stage = steps[next.Sequence/2]
	}
	return next, next.Validate(attempt, input)
}

func (observation Observation) Satisfies(stage Stage, removal bool) bool {
	if observation.Stage != stage {
		return false
	}
	// Each report carries only its stage's proof; contradictory or extraneous
	// facts cannot accidentally qualify another stage.
	expected := Observation{
		ComponentID: observation.ComponentID,
		Input:       observation.Input,
		Stage:       stage,
		At:          observation.At,
	}
	switch stage {
	case WriteConfiguration:
		expected.ConfigurationWritten = true
	case ReconcileRuntime:
		expected.RuntimeReconciled = true
	case ActivateConfiguration:
		expected.ConfigurationActivated = true
	case RemoveRuntime:
		expected.RuntimeAbsent = true
	case VerifyRuntime:
		expected.RuntimeAbsent, expected.RuntimeReady = removal, !removal
	default:
		return false
	}
	return observation == expected
}

func invalidProgress() error {
	return errs.New(
		errs.KindStateConflict,
		"Component progress does not confirm its captured configuration and runtime",
	)
}
