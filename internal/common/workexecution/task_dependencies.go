package workexecution

import (
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

const MaximumTaskDependencies = 16

// TaskCompletion is the small settled fact needed by another Task. It carries
// neither execution inputs nor diagnostics, and therefore can outlive collection
// of the predecessor's history. FinishedAt means operation-owned cleanup and
// aggregate settlement have finished, not just that its last command exited.
type TaskCompletion struct {
	TaskID     string    `json:"task_id"`
	Outcome    Outcome   `json:"outcome"`
	FinishedAt time.Time `json:"finished_at"`
}

func (completion TaskCompletion) Validate() error {
	if ids.Validate(ids.KindTask, completion.TaskID) != nil || !validOutcome(completion.Outcome) ||
		!validTime(completion.FinishedAt) {
		return invalid("Task dependency requires a settled predecessor outcome")
	}
	return nil
}

// TaskDependencies belongs to accepted operator intent, before an operation
// captures runtime/image inputs. After is immutable. Facts are copied once while
// their producer revision is fenced; later Retry cannot rewrite that decision.
type TaskDependencies struct {
	TaskID     string                 `json:"task_id"`
	Intent     Input                  `json:"intent"`
	After      []string               `json:"after"`
	Facts      []TaskCompletion       `json:"facts,omitempty"`
	Failure    *TaskDependencyFailure `json:"failure,omitempty"`
	CreatedAt  time.Time              `json:"created_at"`
	UpdatedAt  time.Time              `json:"updated_at"`
	ReleasedAt *time.Time             `json:"released_at,omitempty"`
}

type TaskDependencyDecision struct {
	Ready   bool
	Waiting []string
	Failed  *TaskDependencyFailure
}

// Failure records why a dependent cannot run. It does not pretend the failed
// predecessor's recovery/cleanup is complete; only success needs that barrier.
type TaskDependencyFailure struct {
	TaskID     string    `json:"task_id"`
	Outcome    Outcome   `json:"outcome"`
	ObservedAt time.Time `json:"observed_at"`
}

func (failure TaskDependencyFailure) Validate() error {
	if ids.Validate(ids.KindTask, failure.TaskID) != nil ||
		failure.Outcome != Failed && failure.Outcome != Cancelled && failure.Outcome != Superseded ||
		!validTime(failure.ObservedAt) {
		return invalid("Task dependency failure requires a failed, cancelled or superseded predecessor")
	}
	return nil
}

func ValidateTaskDependencies(taskID string, after []string) error {
	if ids.Validate(ids.KindTask, taskID) != nil || len(after) == 0 || len(after) > MaximumTaskDependencies {
		return invalid("Run after requires from one through sixteen existing Tasks")
	}
	for index, id := range after {
		if ids.Validate(ids.KindTask, id) != nil || id == taskID || slices.Contains(after[:index], id) {
			return invalid("Run after Tasks must be valid, unique and different from this Task")
		}
	}
	return nil
}

func (dependencies TaskDependencies) Validate() error {
	if err := ValidateTaskDependencies(dependencies.TaskID, dependencies.After); err != nil {
		return err
	}
	if dependencies.Intent.Validate() != nil || !validTime(dependencies.CreatedAt) ||
		!validTime(dependencies.UpdatedAt) || dependencies.UpdatedAt.Before(dependencies.CreatedAt) ||
		len(dependencies.Facts) > len(dependencies.After) {
		return invalid("Task dependency intent, facts or timing is invalid")
	}
	seen := make(map[string]bool, len(dependencies.Facts))
	if dependencies.Failure != nil && (dependencies.Failure.Validate() != nil ||
		!slices.Contains(dependencies.After, dependencies.Failure.TaskID) || dependencies.Failure.ObservedAt.After(dependencies.UpdatedAt)) {
		return invalid("Task dependency failure does not match its accepted predecessor")
	}
	for _, fact := range dependencies.Facts {
		if fact.Validate() != nil || !slices.Contains(dependencies.After, fact.TaskID) || seen[fact.TaskID] ||
			fact.FinishedAt.After(dependencies.UpdatedAt) {
			return invalid("Task dependency facts must match distinct captured predecessors")
		}
		seen[fact.TaskID] = true
	}
	if dependencies.ReleasedAt != nil && (!validTime(*dependencies.ReleasedAt) ||
		dependencies.ReleasedAt.Before(dependencies.CreatedAt) || dependencies.ReleasedAt.After(dependencies.UpdatedAt)) {
		return invalid("Task dependency release time is invalid")
	}
	return nil
}

// Decision considers complete Tasks, never their member operations. Failure wins
// over another pending predecessor, so a dependent cannot wait indefinitely for
// an action it is no longer allowed to run after.
func (dependencies TaskDependencies) Decision() (TaskDependencyDecision, error) {
	if err := dependencies.Validate(); err != nil {
		return TaskDependencyDecision{}, err
	}
	decision := TaskDependencyDecision{}
	if dependencies.Failure != nil {
		copy := *dependencies.Failure
		decision.Failed = &copy
	}
	for _, id := range dependencies.After {
		found := decision.Failed != nil && decision.Failed.TaskID == id
		for _, fact := range dependencies.Facts {
			if fact.TaskID != id {
				continue
			}
			found = true
			if fact.Outcome != Succeeded && decision.Failed == nil {
				decision.Failed = &TaskDependencyFailure{
					TaskID:     fact.TaskID,
					Outcome:    fact.Outcome,
					ObservedAt: fact.FinishedAt,
				}
			}
			break
		}
		if !found {
			decision.Waiting = append(decision.Waiting, id)
		}
	}
	decision.Ready = len(decision.Waiting) == 0 && decision.Failed == nil
	return decision, nil
}

func CloneTaskDependencies(dependencies TaskDependencies) TaskDependencies {
	copy := dependencies
	copy.After = slices.Clone(dependencies.After)
	copy.Facts = slices.Clone(dependencies.Facts)
	if dependencies.Failure != nil {
		failure := *dependencies.Failure
		copy.Failure = &failure
	}
	if dependencies.ReleasedAt != nil {
		at := *dependencies.ReleasedAt
		copy.ReleasedAt = &at
	}
	return copy
}
