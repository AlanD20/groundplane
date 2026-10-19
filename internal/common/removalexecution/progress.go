package removalexecution

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Phase string

const (
	QuiesceExecutors     Phase = "quiesce_executors"
	InspectRuntime       Phase = "inspect_runtime"
	RestoreConfiguration Phase = "restore_configuration"
	RestoreRuntime       Phase = "restore_runtime"
	VerifyRestoration    Phase = "verify_restoration"
	Complete             Phase = "complete"
)

// Progress contains bounded recovery evidence only. Configuration and Compose
// bytes remain in the immutable input. Every effect needs a persisted Begin and
// every advance needs its own receipt, including stopping prior executors.
type Progress struct {
	AttemptID string               `json:"attempt_id"`
	Phase     Phase                `json:"phase"`
	Sequence  uint64               `json:"sequence"`
	InFlight  bool                 `json:"in_flight"`
	UpdatedAt time.Time            `json:"updated_at"`
	Evidence  *workexecution.Input `json:"evidence,omitempty"`
}

func phaseIndex(phase Phase) (uint64, bool) {
	switch phase {
	case QuiesceExecutors:
		return 0, true
	case InspectRuntime:
		return 1, true
	case RestoreConfiguration:
		return 2, true
	case RestoreRuntime:
		return 3, true
	case VerifyRestoration:
		return 4, true
	case Complete:
		return 5, true
	default:
		return 0, false
	}
}

func NewProgress(
	attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput,
	capture RecoveryCapture,
	at time.Time,
) (Progress, error) {
	progress := Progress{AttemptID: attempt.ID, Phase: QuiesceExecutors, UpdatedAt: at.UTC()}
	if attempt.FinishedAt != nil || !at.Before(attempt.Deadline) {
		return Progress{}, invalid()
	}
	return progress, progress.Validate(attempt, input, capture)
}

func (progress Progress) Validate(
	attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput,
	capture RecoveryCapture,
) error {
	if err := capture.ValidateFor(attempt, input); err != nil {
		return err
	}
	return progress.ValidateEnvelope(attempt)
}

// ValidateEnvelope checks journal metadata without retaining runtime artifacts.
// Execution still requires Validate against the captured removal input.
func (progress Progress) ValidateEnvelope(attempt workexecution.Attempt) error {
	if attempt.Validate() != nil || attempt.Direction != workexecution.Recovery || attempt.StartedAt == nil ||
		progress.AttemptID != attempt.ID || progress.UpdatedAt.IsZero() || progress.UpdatedAt.Location() != time.UTC ||
		progress.UpdatedAt.Before(*attempt.StartedAt) ||
		attempt.FinishedAt != nil && progress.UpdatedAt.After(*attempt.FinishedAt) {
		return invalid()
	}
	index, valid := phaseIndex(progress.Phase)
	sequence := index * 2
	if progress.InFlight {
		sequence++
	}
	if !valid || progress.Sequence != sequence || progress.Phase == Complete && progress.InFlight ||
		(index > 0) != (progress.Evidence != nil) || progress.Evidence != nil && progress.Evidence.Validate() != nil {
		return invalid()
	}
	return nil
}

func (progress Progress) Begin(
	attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput,
	capture RecoveryCapture,
	at time.Time,
) (Progress, error) {
	if progress.Validate(attempt, input, capture) != nil || attempt.FinishedAt != nil || progress.InFlight ||
		progress.Phase == Complete || at.Before(progress.UpdatedAt) || !at.Before(attempt.Deadline) {
		return Progress{}, invalid()
	}
	progress.Sequence++
	progress.InFlight, progress.UpdatedAt = true, at.UTC()
	return progress, nil
}

func (progress Progress) Confirm(
	attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput,
	capture RecoveryCapture,
	evidence workexecution.Input,
	at time.Time,
) (Progress, error) {
	if progress.Validate(attempt, input, capture) != nil || attempt.FinishedAt != nil || !progress.InFlight ||
		evidence.Validate() != nil || at.Before(progress.UpdatedAt) {
		return Progress{}, invalid()
	}
	// Late observations account for already-granted effects; the next Begin
	// still refuses an expired deadline. Timeout never proves effects stopped.
	switch progress.Phase {
	case QuiesceExecutors:
		progress.Phase = InspectRuntime
	case InspectRuntime:
		progress.Phase = RestoreConfiguration
	case RestoreConfiguration:
		progress.Phase = RestoreRuntime
	case RestoreRuntime:
		progress.Phase = VerifyRestoration
	case VerifyRestoration:
		progress.Phase = Complete
	default:
		return Progress{}, invalid()
	}
	progress.Sequence++
	progress.InFlight, progress.UpdatedAt, progress.Evidence = false, at.UTC(), &evidence
	return progress, nil
}

func EqualProgress(a, b Progress) bool {
	return a.AttemptID == b.AttemptID && a.Phase == b.Phase && a.Sequence == b.Sequence &&
		a.InFlight == b.InFlight && a.UpdatedAt.Equal(b.UpdatedAt) &&
		(a.Evidence == nil && b.Evidence == nil || a.Evidence != nil && b.Evidence != nil && *a.Evidence == *b.Evidence)
}

// ValidateTransition admits exact replay or one grant/receipt boundary, never a
// jump straight to restoration or a replacement of previously recorded proof.
func ValidateTransition(
	attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput,
	capture RecoveryCapture,
	previous *Progress,
	next Progress,
) error {
	if next.Validate(attempt, input, capture) != nil {
		return invalid()
	}
	var current Progress
	var err error
	if previous == nil {
		current, err = NewProgress(attempt, input, capture, *attempt.StartedAt)
	} else {
		current = *previous
		err = current.Validate(attempt, input, capture)
	}
	if err != nil {
		return err
	}
	if previous != nil && EqualProgress(current, next) {
		return nil
	}
	var expected Progress
	if current.InFlight {
		if next.Evidence == nil {
			return invalid()
		}
		expected, err = current.Confirm(attempt, input, capture, *next.Evidence, next.UpdatedAt)
	} else {
		expected, err = current.Begin(attempt, input, capture, next.UpdatedAt)
	}
	if err != nil || !EqualProgress(expected, next) {
		return invalid()
	}
	return nil
}
