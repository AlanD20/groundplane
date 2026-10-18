package lifecycleexecution

import (
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ResumeProgress carries the bounded completed prefix, not the history of all
// earlier attempts. Its receipts retain the original observation timestamps.
func ResumeProgress(attempt workexecution.Attempt, input *agentpb.ServiceLifecycleWorkInput, continuation *Continuation, at time.Time) (Progress, error) {
	progress, err := NewProgress(attempt, input, at)
	if err != nil || continuation == nil {
		return progress, err
	}
	if err := continuation.ValidateFor(attempt, input); err != nil {
		return Progress{}, err
	}
	progress.Completed, err = continuation.Completed(input)
	if err != nil {
		return Progress{}, err
	}
	progress.Inherited = len(progress.Completed)
	progress.Sequence = progress.expectedSequence()
	return progress, progress.Validate(attempt, input)
}

// ValidateContinuation binds inherited receipts to Controller-captured retry
// authority. Structural progress validation alone cannot authorize a skipped step.
func (progress Progress) ValidateContinuation(attempt workexecution.Attempt, input *agentpb.ServiceLifecycleWorkInput, continuation *Continuation) error {
	if progress.Validate(attempt, input) != nil {
		return invalid()
	}
	if continuation == nil {
		if progress.Inherited != 0 || attempt.Number != 1 {
			return invalid()
		}
		return nil
	}
	if err := continuation.ValidateFor(attempt, input); err != nil {
		return err
	}
	completed, err := continuation.Completed(input)
	if err != nil || progress.Inherited != len(completed) || !slices.Equal(progress.Completed[:progress.Inherited], completed) {
		return invalid()
	}
	return nil
}
