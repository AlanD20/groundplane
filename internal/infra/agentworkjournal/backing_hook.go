package agentworkjournal

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// BackingHookRecord retains the one authorized process, never its command,
// resolved inputs or output. An interrupted process is inspected, not replayed.
type BackingHookRecord struct {
	Boundary    backingprovision.Progress     `json:"boundary"`
	ContainerID string                        `json:"container_id"`
	ExecID      string                        `json:"exec_id"`
	CreatedAt   time.Time                     `json:"created_at"`
	ReadyAt     *time.Time                    `json:"ready_at,omitempty"`
	ExitedAt    *time.Time                    `json:"exited_at,omitempty"`
	ExitCode    *int                          `json:"exit_code,omitempty"`
	Result      *backingprovision.Observation `json:"result,omitempty"`
}

func (hook BackingHookRecord) validate(
	attempt workexecution.Attempt,
	layout backingprovision.Layout,
	progress *backingprovision.Progress,
) error {
	boundary := hook.Boundary
	if progress == nil || boundary.Validate(attempt, layout) != nil || boundary.Stage != backingprovision.AfterStart ||
		!boundary.InFlight ||
		!layout.Hook ||
		boundary.Sequence > progress.Sequence ||
		boundary.Last == nil ||
		boundary.Last.ContainerID != hook.ContainerID ||
		len(hook.ContainerID) != 64 ||
		!ids.ValidContainerID(hook.ContainerID) ||
		len(hook.ExecID) != 64 ||
		!ids.ValidContainerID(hook.ExecID) ||
		hook.CreatedAt.Location() != time.UTC ||
		hook.CreatedAt.Before(*boundary.BeganAt) ||
		!hook.CreatedAt.Before(attempt.Deadline) ||
		(hook.ExitedAt == nil) != (hook.ExitCode == nil) {
		return invalid()
	}
	if hook.ReadyAt != nil &&
		(hook.ReadyAt.Location() != time.UTC || hook.ReadyAt.Before(hook.CreatedAt) || !hook.ReadyAt.Before(attempt.Deadline)) {
		return invalid()
	}
	if hook.ExitedAt != nil &&
		(hook.ReadyAt == nil || hook.ExitedAt.Location() != time.UTC || hook.ExitedAt.Before(*hook.ReadyAt)) {
		return invalid()
	}
	if boundary.Sequence == progress.Sequence && !backingprovision.EqualProgress(boundary, *progress) {
		return invalid()
	}
	if result := hook.Result; result != nil {
		evidence, err := backingprovision.HookSuccessEvidence(attempt, layout, hook.ContainerID)
		if err != nil || hook.ExitedAt == nil || *hook.ExitCode != 0 ||
			!result.Satisfies(backingprovision.AfterStart, layout) ||
			result.ContainerID != hook.ContainerID ||
			result.HookResult != evidence ||
			result.At.Location() != time.UTC ||
			result.At.Before(*hook.ExitedAt) ||
			!result.At.Before(attempt.Deadline) {
			return invalid()
		}
	}
	if progress.Sequence > boundary.Sequence && hook.Result == nil {
		return invalid()
	}
	if progress.Sequence == boundary.Sequence+1 && (progress.Last == nil || *progress.Last != *hook.Result) {
		return invalid()
	}
	return nil
}
