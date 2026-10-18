package taskinspection

import "github.com/AlanD20/groundplane/internal/common/workexecution"

// ActionAvailability is an explanation, never a mutation grant. Abort still
// compares current execution state atomically when the operator invokes it.
type ActionAvailability struct {
	Allowed bool
	Reason  string
}

func cancellation(task workexecution.Task) ActionAvailability {
	switch {
	case task.CollectionStartedAt != nil:
		return ActionAvailability{Reason: "Task history has expired"}
	case task.FinishedAt != nil:
		return ActionAvailability{Reason: "Task has finished"}
	case task.Stop != nil:
		if task.Stop.Outcome == workexecution.Superseded {
			return ActionAvailability{Reason: "Newer work has superseded this Task"}
		}
		return ActionAvailability{Reason: "Cancellation is already requested; running effects must settle"}
	}
	if reason := workexecution.CancellationRestriction(task); reason != "" {
		return ActionAvailability{Reason: reason}
	}
	return ActionAvailability{Allowed: true}
}
