package agentworkjournal

import (
	"bytes"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/hierarchyexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// EnvironmentRemovalRecord retains only the cursor and source identity. Compose
// bytes stay in the captured assignment, not every journal update.
type EnvironmentRemovalRecord struct {
	Recovery      *hierarchyexecution.EnvironmentRecovery `json:"recovery,omitempty"`
	EnvironmentID string                                  `json:"environment_id"`
	Directory     string                                  `json:"directory"`
	Source        workexecution.Input                     `json:"source"`
	Report        hierarchyexecution.EnvironmentReport    `json:"report"`
	Acknowledged  bool                                    `json:"acknowledged"`
	Completion    *workexecution.Attempt                  `json:"completion,omitempty"`
}

func (record EnvironmentRemovalRecord) input(attempt workexecution.Attempt) (hierarchyexecution.EnvironmentInput, error) {
	if record.Source.Validate() != nil {
		return hierarchyexecution.EnvironmentInput{}, invalid()
	}
	digest, err := hex.DecodeString(record.Source.SHA256)
	if err != nil {
		return hierarchyexecution.EnvironmentInput{}, invalid()
	}
	return hierarchyexecution.EnvironmentInput{Attempt: attempt, Recovery: record.Recovery, Removal: &agentpb.EnvironmentRemovalInput{
		CapturedSource: &agentpb.WorkInputReference{InputId: record.Source.ID, Sha256: digest},
		EnvironmentId:  record.EnvironmentID, ExpectedVolumeDir: record.Directory,
	}}, nil
}

func (record EnvironmentRemovalRecord) matches(input hierarchyexecution.EnvironmentInput) bool {
	if (record.Recovery == nil) != (input.Recovery == nil) {
		return false
	}
	if record.Recovery != nil {
		saved, err := hierarchyexecution.EncodeEnvironmentRecovery(*record.Recovery)
		if err != nil {
			return false
		}
		requested, err := hierarchyexecution.EncodeEnvironmentRecovery(*input.Recovery)
		if err != nil || !bytes.Equal(saved, requested) {
			return false
		}
	}
	return input.Removal != nil && input.Removal.CapturedSource != nil && record.EnvironmentID == input.Removal.EnvironmentId &&
		record.Directory == input.Removal.ExpectedVolumeDir && record.Source.ID == input.Removal.CapturedSource.InputId &&
		record.Source.SHA256 == hex.EncodeToString(input.Removal.CapturedSource.Sha256)
}

func (record attemptRecord) validateEnvironmentRemoval() error {
	if record.EnvironmentRemoval == nil || record.ScriptFailure != nil || record.ScriptRecovery != nil ||
		record.ScriptCompletion != nil || record.Release != nil || record.Effect != nil || record.Completion != nil ||
		record.Failure != nil || record.Recovery != nil || len(record.Script) != 0 {
		return invalid()
	}
	removal := record.EnvironmentRemoval
	input, err := removal.input(record.Attempt)
	if err != nil {
		return err
	}
	if _, err := removal.Report.Checkpoint(input); err != nil {
		return err
	}
	if removal.Report.Progress.Sequence != record.Sequence || !removal.Report.Progress.UpdatedAt.Equal(record.UpdatedAt) {
		return invalid()
	}
	if terminal := removal.Completion; terminal != nil {
		checkpoint, err := removal.Report.Checkpoint(input)
		if err != nil || !removal.Acknowledged || removal.Report.Progress.Phase != hierarchyexecution.EnvironmentRemoved ||
			terminal.Validate() != nil || !workexecution.SameAssignment(*terminal, record.Attempt) ||
			terminal.StartedAt == nil || record.Attempt.StartedAt == nil || !terminal.StartedAt.Equal(*record.Attempt.StartedAt) ||
			terminal.FinishedAt == nil || terminal.FinishedAt.Before(record.UpdatedAt) || terminal.Outcome != workexecution.Succeeded ||
			terminal.Result == nil || *terminal.Result != checkpoint.Evidence {
			return invalid()
		}
	}
	return nil
}
