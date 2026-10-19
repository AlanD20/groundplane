package agentworkjournal

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

// AttachExec records only process identity and the input gate, never commands,
// credentials, stdin or output. One adapter procedure may contain 16 commands.
// ReadyAt is saved after the wrapper's readiness message and BEFORE permission
// is sent on stdin. Without that durable boundary, a delayed Docker start cannot
// receive permission to execute the captured command.
type AttachExec struct {
	ContainerID string     `json:"container_id"`
	ExecID      string     `json:"exec_id"`
	CreatedAt   time.Time  `json:"created_at"`
	ReadyAt     *time.Time `json:"ready_at,omitempty"`
	ExitedAt    *time.Time `json:"exited_at,omitempty"`
	ExitCode    *int       `json:"exit_code,omitempty"`
}

const maximumAttachExecs = 16

func (execution AttachExec) validate() error {
	if len(execution.ContainerID) != 64 || len(execution.ExecID) != 64 ||
		!ids.ValidContainerID(
			execution.ContainerID,
		) || !ids.ValidContainerID(execution.ExecID) || execution.CreatedAt.IsZero() ||
		(execution.ExitedAt == nil) != (execution.ExitCode == nil) {
		return invalid()
	}
	if execution.ReadyAt != nil && execution.ReadyAt.Before(execution.CreatedAt) {
		return invalid()
	}
	// Running=false is not evidence that an unstarted exec cannot start later.
	// An exit observation is usable only after the wrapper was seen running.
	if execution.ExitedAt != nil && (execution.ReadyAt == nil || execution.ExitedAt.Before(*execution.ReadyAt)) {
		return invalid()
	}
	return nil
}

func (removal AttachRemovalRecord) validateExecs() error {
	var finished *time.Time
	if result := removal.CommandResult; result != nil {
		finished = &result.FinishedAt
	}
	return validateAttachExecs(removal.Executions, removal.Acknowledged,
		removal.Report.Progress.InFlight, removal.Report.Progress.BeganAt, finished)
}

func validateAttachExecs(executions []AttachExec, acknowledged, inFlight bool, began, finished *time.Time) error {
	if len(executions) > maximumAttachExecs {
		return invalid()
	}
	for index, execution := range executions {
		if !acknowledged || !inFlight || began == nil ||
			execution.validate() != nil || execution.CreatedAt.Before(*began) {
			return invalid()
		}
		if index > 0 {
			previous := executions[index-1]
			if previous.ExitedAt == nil || *previous.ExitCode != 0 || execution.CreatedAt.Before(*previous.ExitedAt) ||
				execution.ContainerID != previous.ContainerID {
				return invalid()
			}
		}
		for _, previous := range executions[:index] {
			if previous.ExecID == execution.ExecID {
				return invalid()
			}
		}
		if finished != nil {
			if execution.ExitedAt == nil || *execution.ExitCode != 0 || execution.ExitedAt.After(*finished) {
				return invalid()
			}
		}
	}
	return nil
}
