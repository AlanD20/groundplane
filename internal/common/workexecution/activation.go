package workexecution

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ActivationGrant records the irreversible software handoff boundary. The
// operation owner stores its exact activation input in the same transaction.
// This receipt is not permission to execute that input twice after a restart.
type ActivationGrant struct {
	WorkItemID string    `json:"work_item_id"`
	AttemptID  string    `json:"attempt_id"`
	Input      Input     `json:"input"`
	GrantedAt  time.Time `json:"granted_at"`
}

func (grant ActivationGrant) Validate() error {
	if ids.Validate(ids.KindWorkItem, grant.WorkItemID) != nil ||
		ids.Validate(ids.KindAttempt, grant.AttemptID) != nil || !validTime(grant.GrantedAt) {
		return invalid("software activation identity or time is invalid")
	}
	return grant.Input.Validate()
}

func AuthorizeActivation(task Task, item Item, attempt Attempt, input Input, at time.Time) (Task, error) {
	if err := task.Validate(); err != nil {
		return Task{}, err
	}
	if err := matchActive(item, attempt); err != nil {
		return Task{}, err
	}
	if item.TaskID != task.ID || item.Operation != ControllerSoftware && item.Operation != AgentSoftware ||
		item.Phase != Running || attempt.Direction != Forward || attempt.StartedAt == nil ||
		item.ForwardRevoked || task.Stop != nil || task.FinishedAt != nil || task.Activation != nil ||
		!at.Before(
			attempt.Deadline,
		) || !at.Before(task.Deadline) || at.Before(item.UpdatedAt) || at.Before(task.UpdatedAt) {
		return Task{}, conflict("software activation is no longer permitted; inspect its saved outcome")
	}
	grant := ActivationGrant{WorkItemID: item.ID, AttemptID: attempt.ID, Input: input, GrantedAt: at}
	if err := grant.Validate(); err != nil {
		return Task{}, err
	}
	next := CloneTask(task)
	next.Activation, next.UpdatedAt = &grant, at
	return next, nil
}

// CancellationFailure supplies the same restriction to mutations and viewers.
// Completion is checked by the caller so an already-cancelled Task stays replayable.
func CancellationFailure(task Task) error {
	if reason := CancellationRestriction(task); reason != "" {
		return errs.New(errs.KindTaskNotAbortable, reason)
	}
	return nil
}

func CancellationRestriction(task Task) string {
	if task.Action == ActionBackupPrune {
		return "backup retention cleanup cannot be cancelled"
	}
	if task.Activation != nil {
		return "software activation has started; wait for its outcome before another update"
	}
	return ""
}
