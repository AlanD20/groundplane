package releaseexecution

import "github.com/AlanD20/groundplane/internal/common/workexecution"

// RestorationInterruption is a real failed attempt of the restoration work,
// distinct from the genuinely successful Group source. Explicit Retry keeps
// that source/cause and the same operation input; it does not invent a failure.
type RestorationInterruption struct {
	Attempt workexecution.Attempt `json:"attempt"`
	Failure Failure               `json:"failure"`
}

func (interruption RestorationInterruption) ValidateFor(current, source workexecution.Attempt, authority GroupRestorationAuthority) error {
	previous := interruption.Attempt
	if interruption.ValidatePrevious(source, authority) != nil || current.Validate() != nil ||
		current.Direction != workexecution.Recovery || current.FinishedAt != nil ||
		previous.TaskID != current.TaskID || previous.WorkItemID != current.WorkItemID || previous.Input != current.Input ||
		previous.Number == ^uint64(0) || previous.Number+1 != current.Number ||
		current.Executor.Executor != workexecution.Agent || previous.Executor.HostID != current.Executor.HostID ||
		current.AssignedAt.Before(*previous.FinishedAt) {
		return invalidProgress()
	}
	return nil
}

// ValidatePrevious permits admission before a new attempt exists. The caller
// joins this actual previous attempt/result with the native explicit-Retry CAS.
func (interruption RestorationInterruption) ValidatePrevious(source workexecution.Attempt, authority GroupRestorationAuthority) error {
	previous := interruption.Attempt
	if authority.ValidateSource(source) != nil || previous.Validate() != nil || previous.FinishedAt == nil || previous.Result == nil ||
		previous.Outcome != workexecution.Failed || previous.Executor.Executor != workexecution.Agent ||
		previous.TaskID != source.TaskID || previous.WorkItemID == source.WorkItemID || previous.WorkItemID == authority.FailedWorkID ||
		previous.Executor.HostID != source.Executor.HostID || previous.Input.ID != authority.SourceInput.ID || previous.Input == authority.SourceInput ||
		previous.AssignedAt.Before(authority.FailedAt) ||
		!interruption.Failure.RecordedAt.Equal(*previous.FinishedAt) {
		return invalidProgress()
	}
	_, reference, err := EncodeFailure(previous, interruption.Failure)
	if err != nil || reference != *previous.Result {
		return invalidProgress()
	}
	return nil
}

func CloneRestorationInterruption(input *RestorationInterruption) *RestorationInterruption {
	if input == nil {
		return nil
	}
	copy := *input
	if input.Attempt.StartedAt != nil {
		at := *input.Attempt.StartedAt
		copy.Attempt.StartedAt = &at
	}
	if input.Attempt.FinishedAt != nil {
		at := *input.Attempt.FinishedAt
		copy.Attempt.FinishedAt = &at
	}
	if input.Attempt.Result != nil {
		result := *input.Attempt.Result
		copy.Attempt.Result = &result
	}
	if input.Failure.Checkpoint != nil {
		checkpoint := *input.Failure.Checkpoint
		copy.Failure.Checkpoint = &checkpoint
	}
	return &copy
}

func EqualRestorationInterruption(left, right *RestorationInterruption) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	l, r := left.Attempt, right.Attempt
	if !workexecution.SameAssignment(l, r) || l.Outcome != r.Outcome ||
		(l.StartedAt == nil) != (r.StartedAt == nil) || (l.FinishedAt == nil) != (r.FinishedAt == nil) ||
		(l.Result == nil) != (r.Result == nil) || l.StartedAt != nil && !l.StartedAt.Equal(*r.StartedAt) ||
		l.FinishedAt != nil && !l.FinishedAt.Equal(*r.FinishedAt) || l.Result != nil && *l.Result != *r.Result {
		return false
	}
	lf, rf := left.Failure, right.Failure
	return lf.AttemptID == rf.AttemptID && lf.Input == rf.Input && lf.Reason == rf.Reason && lf.RecordedAt.Equal(rf.RecordedAt) &&
		(lf.Checkpoint == nil && rf.Checkpoint == nil || lf.Checkpoint != nil && rf.Checkpoint != nil && EqualCheckpoint(*lf.Checkpoint, *rf.Checkpoint))
}
