package entryexecution

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Observation confirms only the named boundary for the complete captured set.
// File verification includes content, ownership, mode and required absences;
// consumer verification includes exact workload identity, not broad liveness.
type Observation struct {
	EnvironmentID       string              `json:"environment_id"`
	Input               workexecution.Input `json:"input"`
	Stage               Stage               `json:"stage"`
	FilesWritten        bool                `json:"files_written"`
	FilesVerified       bool                `json:"files_verified"`
	ConsumersReconciled bool                `json:"consumers_reconciled"`
	ConsumersVerified   bool                `json:"consumers_verified"`
	At                  time.Time           `json:"at"`
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

func FinalSequence(attempt workexecution.Attempt, input *agentpb.EntryConfigurationWorkInput) (uint64, error) {
	layout, err := CaptureLayout(attempt, input)
	if err != nil {
		return 0, err
	}
	return uint64(2 * len(layout.stages())), nil
}

func NewProgress(
	attempt workexecution.Attempt,
	input *agentpb.EntryConfigurationWorkInput,
	at time.Time,
) (Progress, error) {
	layout, err := CaptureLayout(attempt, input)
	if err != nil || !at.Before(attempt.Deadline) {
		return Progress{}, invalidLayout()
	}
	stage := Complete
	if stages := layout.stages(); len(stages) != 0 {
		stage = stages[0]
	}
	progress := Progress{AttemptID: attempt.ID, Stage: stage, UpdatedAt: at.UTC()}
	return progress, progress.ValidateLayout(attempt, layout)
}

func (progress Progress) Validate(attempt workexecution.Attempt, input *agentpb.EntryConfigurationWorkInput) error {
	layout, err := CaptureLayout(attempt, input)
	if err != nil {
		return err
	}
	return progress.ValidateLayout(attempt, layout)
}

func (progress Progress) ValidateLayout(attempt workexecution.Attempt, layout Layout) error {
	if layout.Validate(attempt) != nil || attempt.Direction != workexecution.Forward || attempt.StartedAt == nil {
		return invalidLayout()
	}
	stages := layout.stages()
	if progress.AttemptID != attempt.ID || progress.Sequence > uint64(2*len(stages)) ||
		progress.InFlight != (progress.Sequence%2 == 1) || progress.InFlight != (progress.BeganAt != nil) ||
		progress.UpdatedAt.IsZero() || progress.UpdatedAt.Location() != time.UTC ||
		progress.UpdatedAt.Before(*attempt.StartedAt) {
		return invalidLayout()
	}
	if began := progress.BeganAt; began != nil && (began.Location() != time.UTC ||
		began.Before(*attempt.StartedAt) || !began.Before(attempt.Deadline) || began.After(progress.UpdatedAt)) {
		return invalidLayout()
	}
	index := progress.Sequence / 2
	stage := Complete
	if index < uint64(len(stages)) {
		stage = stages[index]
	}
	if progress.Stage != stage || (progress.Observation != nil) != (index > 0) {
		return invalidLayout()
	}
	if observed := progress.Observation; observed != nil {
		if observed.EnvironmentID != layout.EnvironmentID || observed.Input != attempt.Input ||
			observed.At.IsZero() || observed.At.Location() != time.UTC ||
			observed.At.Before(*attempt.StartedAt) || observed.At.After(progress.UpdatedAt) ||
			!observed.Satisfies(stages[index-1]) {
			return invalidLayout()
		}
	}
	return nil
}

func (progress Progress) Begin(
	attempt workexecution.Attempt,
	input *agentpb.EntryConfigurationWorkInput,
	at time.Time,
) (Progress, error) {
	if progress.Validate(attempt, input) != nil || progress.InFlight || progress.Stage == Complete ||
		at.Before(progress.UpdatedAt) || !at.Before(attempt.Deadline) {
		return Progress{}, invalidLayout()
	}
	next, began := progress, at.UTC()
	next.Sequence++
	next.InFlight, next.BeganAt, next.UpdatedAt = true, &began, began
	return next, next.Validate(attempt, input)
}

func (progress Progress) Confirm(
	attempt workexecution.Attempt,
	input *agentpb.EntryConfigurationWorkInput,
	observed Observation,
	at time.Time,
) (Progress, error) {
	if progress.Validate(attempt, input) != nil || !progress.InFlight || at.Before(progress.UpdatedAt) ||
		observed.At.Before(*progress.BeganAt) || observed.At.After(at) || !observed.Satisfies(progress.Stage) {
		return Progress{}, invalidLayout()
	}
	layout, err := CaptureLayout(attempt, input)
	if err != nil {
		return Progress{}, err
	}
	next := progress
	next.Sequence++
	next.InFlight, next.BeganAt, next.UpdatedAt = false, nil, at.UTC()
	next.Stage, next.Observation = Complete, &observed
	if stages := layout.stages(); next.Sequence/2 < uint64(len(stages)) {
		next.Stage = stages[next.Sequence/2]
	}
	return next, next.ValidateLayout(attempt, layout)
}

func (observed Observation) Satisfies(stage Stage) bool {
	expected := Observation{EnvironmentID: observed.EnvironmentID, Input: observed.Input, Stage: stage, At: observed.At}
	switch stage {
	case WriteConfiguration:
		expected.FilesWritten = true
	case VerifyConfiguration:
		expected.FilesVerified = true
	case ReconcileConsumers:
		expected.ConsumersReconciled = true
	case VerifyConsumers:
		expected.ConsumersVerified = true
	default:
		return false
	}
	return observed == expected
}
