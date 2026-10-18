package hierarchyexecution

func ValidateEnvironmentTransition(input EnvironmentInput, previous *EnvironmentProgress, next EnvironmentProgress) error {
	if err := next.Validate(input); err != nil {
		return err
	}
	var current EnvironmentProgress
	var err error
	if previous == nil {
		if input.Attempt.StartedAt == nil {
			return invalidEnvironmentProgress()
		}
		current, err = NewEnvironmentProgress(input, *input.Attempt.StartedAt)
	} else {
		current = *previous
		err = current.Validate(input)
	}
	if err != nil {
		return err
	}
	if previous != nil && EqualEnvironmentProgress(current, next) {
		return nil
	}
	var expected EnvironmentProgress
	if current.InFlight {
		if next.Observation == nil {
			return invalidEnvironmentProgress()
		}
		expected, err = current.Confirm(input, *next.Observation, next.UpdatedAt)
	} else {
		expected, err = current.Begin(input, next.UpdatedAt)
	}
	if err != nil {
		return err
	}
	if !EqualEnvironmentProgress(expected, next) {
		return invalidEnvironmentProgress()
	}
	return nil
}

func EqualEnvironmentProgress(left, right EnvironmentProgress) bool {
	if left.AttemptID != right.AttemptID || left.Sequence != right.Sequence || left.Phase != right.Phase || left.InFlight != right.InFlight ||
		!left.UpdatedAt.Equal(right.UpdatedAt) || (left.BeganAt == nil) != (right.BeganAt == nil) || (left.Observation == nil) != (right.Observation == nil) {
		return false
	}
	if left.BeganAt != nil && !left.BeganAt.Equal(*right.BeganAt) {
		return false
	}
	if left.Observation != nil {
		a, b := *left.Observation, *right.Observation
		return a.EnvironmentID == b.EnvironmentID && a.Source == b.Source && a.RuntimeAbsent == b.RuntimeAbsent &&
			a.DirectoryAbsent == b.DirectoryAbsent && a.At.Equal(b.At)
	}
	return true
}
