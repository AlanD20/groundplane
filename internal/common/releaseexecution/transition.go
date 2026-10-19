package releaseexecution

import (
	"fmt"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// ValidateTransition derives the permitted next state. A valid-looking cursor
// alone is insufficient: callers cannot jump over an unobserved phase.
func ValidateTransition(input Input, previous *Progress, next Progress) error {
	if err := next.Validate(input); err != nil {
		return err
	}
	var current Progress
	var err error
	if previous == nil {
		current, err = NewProgress(input, *input.Attempt.StartedAt)
	} else {
		current = *previous
		err = current.Validate(input)
	}
	if err != nil {
		return err
	}
	if previous != nil && EqualProgress(current, next) {
		return nil
	}
	var expected Progress
	if !current.InFlight {
		if current.Rolling == nil {
			expected, err = current.Begin(input, next.UpdatedAt)
		} else {
			expected, err = current.BeginReplica(input, next.UpdatedAt)
		}
	} else {
		if next.Evidence == nil {
			return invalidProgress()
		}
		if current.Rolling == nil {
			expected, err = current.Confirm(input, *next.Evidence, next.UpdatedAt)
		} else {
			expected, err = current.ConfirmReplica(input, current.Rolling.Phase, *next.Evidence, next.UpdatedAt)
		}
	}
	if err != nil {
		return err
	}
	if !EqualProgress(expected, next) {
		return invalidProgress()
	}
	return nil
}

func EqualProgress(left, right Progress) bool {
	if left.AttemptID != right.AttemptID || left.Sequence != right.Sequence || left.Phase != right.Phase ||
		left.Hook != right.Hook ||
		left.Action != right.Action ||
		left.InFlight != right.InFlight ||
		!left.UpdatedAt.Equal(right.UpdatedAt) ||
		(left.BeganAt == nil) != (right.BeganAt == nil) ||
		(left.Evidence == nil) != (right.Evidence == nil) ||
		(left.Rolling == nil) != (right.Rolling == nil) {
		return false
	}
	if left.BeganAt != nil && !left.BeganAt.Equal(*right.BeganAt) {
		return false
	}
	if left.Evidence != nil && *left.Evidence != *right.Evidence {
		return false
	}
	if left.Rolling != nil {
		a, b := *left.Rolling, *right.Rolling
		return a.Change == b.Change && a.Phase == b.Phase && a.InFlight == b.InFlight &&
			a.ReplicaStartedAt.Equal(
				b.ReplicaStartedAt,
			) && a.ReplicaDeadline.Equal(b.ReplicaDeadline) && a.DrainUntil.Equal(b.DrainUntil)
	}
	return true
}

// StageID names the action being acknowledged, not the cursor after completion.
// It is stable across attempts of the same captured input.
func (progress Progress) StageID(input Input) (string, error) {
	if err := progress.Validate(input); err != nil {
		return "", err
	}
	if progress.Phase == Configuration {
		return ConfigurationStageID(input.Attempt.Input)
	}
	at, err := ids.Timestamp(ids.KindPlan, input.Attempt.Input.ID)
	if err != nil {
		return "", err
	}
	purpose := fmt.Sprintf("release/%s/%d", progress.Phase, progress.Hook)
	if progress.Action != "" {
		purpose = fmt.Sprintf("release/rollout/%s", progress.Action)
	}
	if progress.Rolling != nil {
		purpose = fmt.Sprintf("release/rolling/%d/%s", progress.Rolling.Change, progress.Rolling.Phase)
	}
	return ids.DeriveAt(ids.KindStep, at, input.Attempt.Input.SHA256, purpose), nil
}

// ConfigurationStageID also identifies orphan file helpers after the forward
// attempt ended. Computing an identity grants no permission to execute it.
func ConfigurationStageID(reference workexecution.Input) (string, error) {
	if err := reference.Validate(); err != nil {
		return "", err
	}
	at, err := ids.Timestamp(ids.KindPlan, reference.ID)
	if err != nil {
		return "", err
	}
	return ids.DeriveAt(ids.KindStep, at, reference.SHA256, "release/configuration/0"), nil
}
