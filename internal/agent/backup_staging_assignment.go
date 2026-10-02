package agent

import (
	"bytes"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// A prepared descriptor lease belongs to the assignment resume sealed in the
// disposition, not merely another Task with the same point or file names.
func (state *backupStagingState) verifyPreparedAssignment(assignment *agentpb.TaskAssignment) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.plan == nil {
		return invalidAgentStaging()
	}
	for _, prepared := range state.prepared {
		if prepared.IDs.Task != assignment.TaskId {
			continue
		}
		if assignment.BackupAuthority == nil {
			return invalidAgentStaging()
		}
		resumeSHA, err := executionplan.BackupStagingAssignmentResumeSHA256(
			assignment.BackupAuthority,
			assignment.BackupResume,
		)
		if err != nil {
			return err
		}
		recoverySHA, err := executionplan.BackupStagingRecoveryKey(
			prepared.IDs.Task,
			prepared.IDs.Step,
			prepared.IDs.Point,
		)
		if err != nil {
			return err
		}
		matched := false
		for _, disposition := range state.plan.Dispositions {
			if bytes.Equal(disposition.RecoveryKeySha256, recoverySHA) && disposition.GetResumePrepared() != nil &&
				bytes.Equal(disposition.GetResumePrepared().AssignmentResumeSha256, resumeSHA) {
				matched = true
				break
			}
		}
		if !matched {
			return invalidAgentStaging()
		}
	}
	return nil
}
