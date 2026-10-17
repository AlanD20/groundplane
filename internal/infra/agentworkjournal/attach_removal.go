package agentworkjournal

import (
	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

type AttachRemovalRecord struct {
	Authority     attachexecution.RemovalAuthority      `json:"authority"`
	Report        attachexecution.RemovalReport         `json:"report"`
	Acknowledged  bool                                  `json:"acknowledged"`
	CommandResult *attachexecution.RemovalCommandResult `json:"command_result,omitempty"`
	Executions    []AttachExec                          `json:"executions,omitempty"`
	Completion    *workexecution.Attempt                `json:"completion,omitempty"`
}

func (record AttachRemovalRecord) matches(input attachexecution.RemovalInput) bool {
	authority, err := input.Authority()
	return err == nil && authority == record.Authority
}

func (record attemptRecord) validateAttachRemoval() error {
	if record.AttachRemoval == nil || record.AttachFailure != nil || record.EnvironmentFailure != nil || record.EnvironmentRemoval != nil ||
		record.ScriptFailure != nil || record.ScriptRecovery != nil || record.ScriptCompletion != nil ||
		record.Release != nil || record.Effect != nil || record.Completion != nil || record.Failure != nil ||
		record.Recovery != nil || len(record.Script) != 0 {
		return invalid()
	}
	removal := record.AttachRemoval
	if err := removal.validateExecs(); err != nil {
		return err
	}
	checkpoint, err := removal.Report.CheckpointAuthority(record.Attempt, removal.Authority)
	if err != nil {
		return err
	}
	if removal.Report.Progress.Sequence != record.Sequence || !removal.Report.Progress.UpdatedAt.Equal(record.UpdatedAt) {
		return invalid()
	}
	if result := removal.CommandResult; result != nil {
		progress := removal.Report.Progress
		if !removal.Acknowledged {
			return invalid()
		}
		if _, err := progress.ConfirmAuthority(record.Attempt, removal.Authority, *result, result.FinishedAt); err != nil {
			return invalid()
		}
	}
	if terminal := removal.Completion; terminal != nil {
		if !removal.Acknowledged || removal.Report.Progress.InFlight || removal.Report.Progress.Completed != removal.Authority.Commands ||
			terminal.Validate() != nil || !workexecution.SameAssignment(*terminal, record.Attempt) ||
			terminal.StartedAt == nil || record.Attempt.StartedAt == nil || !terminal.StartedAt.Equal(*record.Attempt.StartedAt) ||
			terminal.FinishedAt == nil || terminal.FinishedAt.Before(record.UpdatedAt) || terminal.Outcome != workexecution.Succeeded ||
			terminal.Result == nil || *terminal.Result != checkpoint.Evidence {
			return invalid()
		}
	}
	return nil
}
