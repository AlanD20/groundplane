package releaseexecution

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Progress is the Release handler's durable cursor, not disposable UI history.
// An in-flight action must be inspected after interruption, never replayed on
// the assumption that a lost acknowledgement means the effect did not occur.
type Progress struct {
	AttemptID string                   `json:"attempt_id"`
	Sequence  uint64                   `json:"sequence"`
	Phase     Phase                    `json:"phase"`
	Hook      uint32                   `json:"hook,omitempty"`
	InFlight  bool                     `json:"in_flight"`
	BeganAt   *time.Time               `json:"began_at,omitempty"`
	UpdatedAt time.Time                `json:"updated_at"`
	Evidence  *workexecution.Input     `json:"evidence,omitempty"`
	Rolling   *rollingrelease.Progress `json:"rolling,omitempty"`
	Action    RolloutAction            `json:"action,omitempty"`
}

type Phase string

const (
	Configuration Phase = "configuration"
	PreHooks      Phase = "pre_hooks"
	Rollout       Phase = "rollout"
	PostHooks     Phase = "post_hooks"
	Complete      Phase = "complete"
)

func NewProgress(input Input, at time.Time) (Progress, error) {
	if input.Attempt.Validate() != nil || input.Attempt.StartedAt == nil ||
		input.Attempt.Direction != workexecution.Forward ||
		input.Release == nil ||
		at.IsZero() ||
		at.Before(*input.Attempt.StartedAt) ||
		!at.Before(input.Attempt.Deadline) {
		return Progress{}, invalidProgress()
	}
	return Progress{AttemptID: input.Attempt.ID, Phase: Configuration, UpdatedAt: at.UTC()}, nil
}

func (progress Progress) Validate(input Input) error {
	if input.Attempt.Validate() != nil || input.Attempt.StartedAt == nil || input.Release == nil ||
		progress.AttemptID != input.Attempt.ID ||
		progress.UpdatedAt.IsZero() ||
		progress.UpdatedAt.Before(*input.Attempt.StartedAt) ||
		progress.InFlight != (progress.BeganAt != nil) ||
		progress.InFlight != (progress.Sequence%2 == 1) ||
		progress.BeganAt != nil &&
			(progress.BeganAt.Before(*input.Attempt.StartedAt) || progress.BeganAt.After(progress.UpdatedAt)) ||
		progress.Evidence != nil && progress.Evidence.Validate() != nil {
		return invalidProgress()
	}
	if progress.Sequence == 0 && (progress.Phase != Configuration || progress.Evidence != nil) ||
		progress.Sequence >= 2 && progress.Evidence == nil {
		return invalidProgress()
	}
	if progress.Phase != Rollout && progress.Action != "" {
		return invalidProgress()
	}
	switch progress.Phase {
	case Configuration:
		if progress.Hook != 0 || progress.Rolling != nil {
			return invalidProgress()
		}
	case PreHooks, PostHooks:
		if progress.Hook >= uint32(len(releaseHooks(input, progress.Phase))) || progress.Rolling != nil {
			return invalidProgress()
		}
	case Rollout:
		finalRollingCheck := input.Artifacts.Rolling != nil && progress.Rolling == nil &&
			progress.Action == VerifyServing
		if progress.Hook != 0 || !finalRollingCheck && (input.Artifacts.Rolling != nil) != (progress.Rolling != nil) {
			return invalidProgress()
		}
		if progress.Rolling != nil {
			if progress.Action != "" {
				return invalidProgress()
			}
			if progress.InFlight != progress.Rolling.InFlight {
				return invalidProgress()
			}
			if err := progress.Rolling.Validate(*input.Artifacts.Rolling, input.Attempt.Deadline); err != nil {
				return err
			}
		} else if !validRolloutAction(input, progress.Action) {
			return invalidProgress()
		}
	case Complete:
		if progress.InFlight || progress.Hook != 0 || progress.Rolling != nil || progress.Evidence == nil {
			return invalidProgress()
		}
	default:
		return invalidProgress()
	}
	return nil
}

// Begin must be persisted and acknowledged before crossing the phase boundary.
// A phase already in flight requires the owner's reconciliation path.
func (progress Progress) Begin(input Input, at time.Time) (Progress, error) {
	if err := progress.Validate(input); err != nil {
		return Progress{}, err
	}
	if progress.InFlight || progress.Phase == Complete || at.Before(progress.UpdatedAt) ||
		!at.Before(input.Attempt.Deadline) ||
		progress.Sequence == ^uint64(0) {
		return Progress{}, invalidProgress()
	}
	if progress.Rolling != nil {
		return Progress{}, errs.New(
			errs.KindStateConflict,
			"rolling Release must use its captured per-replica phase cursor",
		)
	}
	started := at.UTC()
	progress.Sequence++
	progress.InFlight, progress.BeganAt, progress.UpdatedAt = true, &started, started
	return progress, nil
}

// Confirm consumes operation-verified evidence, not a process exit alone. It
// does not clear an uncertain phase on failure or skip to the next operation.
func (progress Progress) Confirm(input Input, evidence workexecution.Input, at time.Time) (Progress, error) {
	if err := progress.Validate(input); err != nil {
		return Progress{}, err
	}
	if !progress.InFlight || progress.Rolling != nil || evidence.Validate() != nil || at.Before(progress.UpdatedAt) ||
		progress.Sequence == ^uint64(0) {
		return Progress{}, invalidProgress()
	}
	progress.Sequence++
	progress.InFlight, progress.BeganAt, progress.UpdatedAt = false, nil, at.UTC()
	progress.Evidence = &evidence
	return progress.advance(input)
}

func (progress Progress) advance(input Input) (Progress, error) {
	switch progress.Phase {
	case Configuration:
		progress.Phase = PreHooks
	case PreHooks, PostHooks:
		progress.Hook++
	case Rollout:
		if progress.Rolling == nil {
			// Post hooks may initialize the candidate before it can satisfy its
			// healthcheck. They must finish before readiness or traffic changes.
			if progress.Action == StartCandidate && len(releaseHooks(input, PostHooks)) > 0 {
				progress.Phase, progress.Action = PostHooks, ""
				return progress, progress.Validate(input)
			}
			next, err := nextRolloutAction(input, progress.Action)
			if err != nil {
				return Progress{}, err
			}
			progress.Action = next
			if next != "" {
				return progress, progress.Validate(input)
			}
		}
		progress.Phase, progress.Rolling = Complete, nil
		if input.Artifacts.Rolling != nil {
			progress.Phase = PostHooks
		}
	default:
		return Progress{}, invalidProgress()
	}
	if progress.Phase == PreHooks && progress.Hook == uint32(len(releaseHooks(input, PreHooks))) {
		progress.Phase, progress.Hook = Rollout, 0
		if input.Artifacts.Rolling != nil {
			cursor, err := rollingrelease.NewProgress(*input.Artifacts.Rolling)
			if err != nil {
				return Progress{}, err
			}
			progress.Rolling = &cursor
		} else {
			action, err := firstRolloutAction(input)
			if err != nil {
				return Progress{}, err
			}
			progress.Action = action
		}
	}
	if progress.Phase == PostHooks && progress.Hook == uint32(len(releaseHooks(input, PostHooks))) {
		progress.Phase, progress.Hook = Complete, 0
		if input.Artifacts.Rolling == nil {
			progress.Phase, progress.Action = Rollout, WaitCandidate
		}
	}
	return progress, progress.Validate(input)
}

func invalidProgress() error {
	return errs.New(errs.KindStateConflict, "Release progress does not match its captured phase, attempt or deadline")
}
