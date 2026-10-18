package releaseexecution

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// ValidateRecoveryTransition permits only exact replay or the next acknowledged
// boundary. A plausible phase name cannot skip quiescence or restoration proof.
func ValidateRecoveryTransition(input RecoveryInput, previous *RecoveryProgress, next RecoveryProgress) error {
	if next.Validate(input) != nil {
		return invalidProgress()
	}
	var current RecoveryProgress
	var err error
	if previous == nil {
		current, err = NewRecoveryProgress(input, *input.Input.Attempt.StartedAt)
	} else {
		current = *previous
		err = current.Validate(input)
	}
	if err != nil {
		return err
	}
	if previous != nil && EqualRecoveryProgress(current, next) {
		return nil
	}
	var expected RecoveryProgress
	if current.InFlight {
		if next.Evidence == nil {
			return invalidProgress()
		}
		expected, err = current.Confirm(input, *next.Evidence, next.UpdatedAt)
	} else {
		expected, err = current.Begin(input, next.UpdatedAt)
	}
	if err != nil || !EqualRecoveryProgress(expected, next) {
		return invalidProgress()
	}
	return nil
}

func EqualRecoveryProgress(a, b RecoveryProgress) bool {
	return a.AttemptID == b.AttemptID && a.Phase == b.Phase && a.Sequence == b.Sequence && a.InFlight == b.InFlight &&
		a.UpdatedAt.Equal(b.UpdatedAt) && (a.Evidence == nil && b.Evidence == nil || a.Evidence != nil && b.Evidence != nil && *a.Evidence == *b.Evidence)
}

func (progress RecoveryProgress) StageID(input RecoveryInput) (string, error) {
	if err := progress.Validate(input); err != nil {
		return "", err
	}
	return RecoveryStageID(input.Input.Attempt.Input, progress.Phase)
}

// RecoveryStageID names a captured boundary even when reconciling an orphan.
// It is an identity calculation, not a grant to run that phase.
func RecoveryStageID(reference workexecution.Input, phase RecoveryPhase) (string, error) {
	if reference.Validate() != nil {
		return "", invalidProgress()
	}
	if _, valid := recoveryPhaseIndex(phase); !valid {
		return "", invalidProgress()
	}
	at, err := ids.Timestamp(ids.KindPlan, reference.ID)
	if err != nil {
		return "", err
	}
	return ids.DeriveAt(ids.KindStep, at, reference.SHA256, "release/recovery/"+string(phase)), nil
}
