// Package removalexecution owns Service removal restoration boundaries. Cleanup
// is a separate attempt, not a replay of the removal or a forward Start action.
package removalexecution

import (
	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// RecoveryCapture preserves the original failure and its inherited boundary,
// including a retry interrupted before acknowledging that inherited prefix.
type RecoveryCapture struct {
	Source       lifecycleexecution.Recovery      `json:"source"`
	Continuation *lifecycleexecution.Continuation `json:"continuation,omitempty"`
}

func (capture RecoveryCapture) Validate(input *agentpb.ServiceLifecycleWorkInput) error {
	if input == nil || input.Action != agentpb.ServiceLifecycleAction_SERVICE_LIFECYCLE_ACTION_DESTROY ||
		input.Hook != nil {
		return invalid()
	}
	if err := capture.Source.Validate(input); err != nil {
		return err
	}
	if capture.Continuation == nil {
		if capture.Source.Forward.Number != 1 {
			return invalid()
		}
	} else if err := capture.Continuation.ValidateFor(capture.Source.Forward, input); err != nil {
		return err
	}
	if progress := capture.Source.Progress; progress != nil {
		return progress.ValidateContinuation(capture.Source.Forward, input, capture.Continuation)
	}
	return nil
}

func (capture RecoveryCapture) ValidateFor(
	attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput,
) error {
	if err := capture.Validate(input); err != nil {
		return err
	}
	if err := capture.Source.ValidateFor(attempt, input); err != nil {
		return err
	}
	if attempt.AssignedAt.Before(*capture.Source.Forward.FinishedAt) {
		return invalid()
	}
	return nil
}

func invalid() error {
	return errs.New(
		errs.KindStateConflict,
		"Service removal restoration does not match its captured failure or acknowledged recovery step",
	)
}
