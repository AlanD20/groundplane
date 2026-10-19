package backupruntime

import (
	"bytes"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (state *Staging) AuthorizeSourceResume(
	assignment *agentpb.BackupWorkAssignment,
	identity workexecution.Identity,
) error {
	owned, attempt, err := executionplan.OpenBackupWorkAssignment(assignment, identity)
	if err != nil {
		return err
	}
	if state == nil {
		return errs.New(errs.KindStateConflict, "Backup staging recovery is unavailable")
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closed {
		return errs.New(errs.KindStateConflict, "Backup staging session is closed")
	}
	var digest []byte
	for _, step := range owned.Input.Steps {
		point := step.GetCapture().GetPointId()
		if point == "" {
			point = step.GetRestore().GetPointId()
		}
		if point == "" {
			continue
		}
		key, err := executionplan.BackupSourceStagingKey(attempt.TaskID, step.ExecutionId, point)
		if err != nil {
			return err
		}
		disposition := state.sourceRecovery[string(key)]
		if disposition == nil || disposition.GetDiscardRecovered() != nil {
			continue
		}
		resume := disposition.GetResumePrepared()
		if resume == nil {
			return errs.New(errs.KindStateConflict, "Backup source files are held for recovery and cannot be executed")
		}
		if digest == nil {
			digest, err = executionplan.BackupWorkStagingResumeSHA256(owned, identity)
			if err != nil {
				return err
			}
		}
		if !bytes.Equal(digest, resume.AssignmentResumeSha256) {
			return errs.New(
				errs.KindStateConflict,
				"Backup source resume differs from the acknowledged startup decision",
			)
		}
	}
	return nil
}
