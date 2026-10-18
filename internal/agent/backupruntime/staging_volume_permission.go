package backupruntime

import (
	"bytes"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type volumeRecoveryPermission struct {
	entry       *agentpb.BackupRecoveredVolumeRestore
	disposition *agentpb.BackupVolumeRestoreRecoveryDisposition
}

func volumeRecoveryPermissions(inventory *agentpb.BackupStagingInventory, plan *agentpb.BackupStagingRecoveryPlan) ([]volumeRecoveryPermission, error) {
	if err := executionplan.ValidateBackupStagingRecoveryPlan(inventory, plan); err != nil {
		return nil, err
	}
	permissions := make([]volumeRecoveryPermission, len(inventory.VolumeRestores))
	for index, entry := range inventory.VolumeRestores {
		permissions[index] = volumeRecoveryPermission{entry: proto.CloneOf(entry), disposition: proto.CloneOf(plan.VolumeDispositions[index])}
	}
	return permissions, nil
}

// AuthorizeVolumeResume checks the startup decision before obtaining Start or
// opening retained mutation journals. A held source never becomes executable
// merely because its files are present or another assignment names the Point.
func (state *Staging) AuthorizeVolumeResume(assignment *agentpb.BackupWorkAssignment, identity workexecution.Identity) error {
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
	var resumeDigest []byte
	for _, permission := range state.volumeRecovery {
		entry := permission.entry
		if entry.Attempt.WorkItemId != attempt.WorkItemID {
			continue
		}
		if !proto.Equal(entry.Attempt, owned.Attempt) || uint64(entry.SourceOrdinal) >= uint64(len(owned.Input.Steps)) {
			return errs.New(errs.KindStateConflict, "Recovered Volume belongs to a different execution attempt")
		}
		step := owned.Input.Steps[entry.SourceOrdinal]
		if step.GetRestore().GetVolume() == nil || step.ExecutionId != entry.RestoreGenerationId ||
			step.GetRestore().PointId != entry.PointId || !bytes.Equal(step.StepDigest, entry.AuthoritySha256) {
			return errs.New(errs.KindStateConflict, "Recovered Volume differs from the captured Restore source")
		}
		if permission.disposition.GetCleanup() != nil {
			// Startup already performed and acknowledged the exact cleanup.
			// Source execution must still prove completion from its own cursor.
			continue
		}
		resume := permission.disposition.GetResume()
		if resume == nil {
			return errs.New(errs.KindStateConflict, "Recovered Volume is retained pending recovery, not authorized to resume")
		}
		if resumeDigest == nil {
			resumeDigest, err = executionplan.BackupWorkStagingResumeSHA256(owned, identity)
			if err != nil {
				return err
			}
		}
		if !bytes.Equal(resumeDigest, resume.AssignmentResumeSha256) {
			return errs.New(errs.KindStateConflict, "Volume resume differs from the acknowledged startup decision")
		}
	}
	return nil
}
