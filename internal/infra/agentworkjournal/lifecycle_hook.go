package agentworkjournal

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// LifecycleHookRecord stores one exec identity and its permission boundary.
// No command, environment value or output is retained here.
type LifecycleHookRecord struct {
	Boundary    lifecycleexecution.Progress `json:"boundary"`
	ContainerID string                      `json:"container_id"`
	ExecID      string                      `json:"exec_id"`
	CreatedAt   time.Time                   `json:"created_at"`
	ReadyAt     *time.Time                  `json:"ready_at,omitempty"`
	ExitedAt    *time.Time                  `json:"exited_at,omitempty"`
	ExitCode    *int                        `json:"exit_code,omitempty"`
	Result      *lifecycleexecution.Receipt `json:"result,omitempty"`
}

func (hook LifecycleHookRecord) validate(attempt workexecution.Attempt, stages []lifecycleexecution.Stage, progress lifecycleexecution.Progress) error {
	boundary := hook.Boundary
	if boundary.ValidateStages(attempt, stages) != nil || boundary.BeganAt == nil || boundary.Sequence > progress.Sequence ||
		stages[len(boundary.Completed)].Expected != lifecycleexecution.HookSucceeded ||
		len(hook.ContainerID) != 64 || !ids.ValidContainerID(hook.ContainerID) || len(hook.ExecID) != 64 || !ids.ValidContainerID(hook.ExecID) ||
		hook.CreatedAt.Location() != time.UTC || hook.CreatedAt.Before(*boundary.BeganAt) || !hook.CreatedAt.Before(attempt.Deadline) ||
		(hook.ExitedAt == nil) != (hook.ExitCode == nil) {
		return invalid()
	}
	if hook.ReadyAt != nil && (hook.ReadyAt.Location() != time.UTC || hook.ReadyAt.Before(hook.CreatedAt) || !hook.ReadyAt.Before(attempt.Deadline)) {
		return invalid()
	}
	if hook.ExitedAt != nil && (hook.ReadyAt == nil || hook.ExitedAt.Location() != time.UTC || hook.ExitedAt.Before(*hook.ReadyAt)) {
		return invalid()
	}
	if boundary.Sequence == progress.Sequence && !lifecycleexecution.EqualProgress(boundary, progress) {
		return invalid()
	}
	if result := hook.Result; result != nil {
		stage := stages[len(boundary.Completed)]
		if hook.ExitedAt == nil || *hook.ExitCode != 0 || result.StageID != stage.ID || result.Member != "" || result.State != lifecycleexecution.HookSucceeded ||
			result.Evidence.Validate() != nil || result.Evidence.ID != attempt.Input.ID || result.ObservedAt.Location() != time.UTC || result.ObservedAt.Before(*hook.ExitedAt) {
			return invalid()
		}
	}
	if progress.Sequence > boundary.Sequence {
		if hook.Result == nil || len(progress.Completed) <= len(boundary.Completed) {
			return invalid()
		}
		actual := progress.Completed[len(boundary.Completed)]
		if actual != *hook.Result {
			return invalid()
		}
	}
	return nil
}
