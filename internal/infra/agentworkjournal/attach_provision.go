package agentworkjournal

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// AttachProvisionRecord contains no credential or hook output. A successful
// command is saved before its result report is sent, so reconnect can resend
// that report without running the command again.
type AttachProvisionRecord struct {
	Authority     attachexecution.ProvisionAuthority      `json:"authority"`
	Report        attachexecution.ProvisionReport         `json:"report"`
	Acknowledged  bool                                    `json:"acknowledged"`
	CommandResult *attachexecution.ProvisionCommandResult `json:"command_result,omitempty"`
	Executions    []AttachExec                            `json:"executions,omitempty"`
	Completion    *workexecution.Attempt                  `json:"completion,omitempty"`
}

func (record attemptRecord) validateAttachProvision() error {
	if record.AttachProvision == nil || record.AttachProvisionFailure != nil || record.EntryRecovery != nil || record.EntryConfiguration != nil ||
		record.ComponentRecovery != nil || record.Component != nil || record.NetworkRecovery != nil || record.Network != nil ||
		record.VolumeRecovery != nil || record.Volume != nil || record.AttachRecovery != nil || record.AttachFailure != nil ||
		record.AttachRemoval != nil || record.EnvironmentFailure != nil || record.EnvironmentRemoval != nil ||
		record.ScriptFailure != nil || record.ScriptRecovery != nil || len(record.Script) != 0 || record.Release != nil ||
		record.Effect != nil || record.Completion != nil || record.Failure != nil || record.Recovery != nil || record.ScriptCompletion != nil {
		return invalid()
	}
	saved := record.AttachProvision
	var finished *time.Time
	if result := saved.CommandResult; result != nil {
		finished = &result.FinishedAt
	}
	if err := validateAttachExecs(saved.Executions, saved.Acknowledged, saved.Report.Progress.InFlight,
		saved.Report.Progress.BeganAt, finished); err != nil {
		return err
	}
	if _, err := saved.Report.CheckpointAuthority(record.Attempt, saved.Authority); err != nil {
		return err
	}
	if saved.Report.Progress.Sequence != record.Sequence || !saved.Report.Progress.UpdatedAt.Equal(record.UpdatedAt) {
		return invalid()
	}
	if result := saved.CommandResult; result != nil {
		if !saved.Acknowledged {
			return invalid()
		}
		if _, err := saved.Report.Progress.Confirm(record.Attempt, saved.Authority, *result, result.FinishedAt); err != nil {
			return err
		}
	}
	if terminal := saved.Completion; terminal != nil {
		if !saved.Acknowledged || !saved.Report.Progress.Complete(record.Attempt, saved.Authority) ||
			terminal.Validate() != nil || !workexecution.SameAssignment(*terminal, record.Attempt) ||
			terminal.StartedAt == nil || record.Attempt.StartedAt == nil || !terminal.StartedAt.Equal(*record.Attempt.StartedAt) ||
			terminal.FinishedAt == nil || terminal.FinishedAt.Before(record.UpdatedAt) || terminal.Result == nil ||
			(terminal.Outcome != workexecution.Succeeded && terminal.Outcome != workexecution.Cancelled && terminal.Outcome != workexecution.Superseded) {
			return invalid()
		}
	}
	return nil
}
