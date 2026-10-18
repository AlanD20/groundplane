package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// BackupWorkStagingResumeSHA256 pins the exact native input and acknowledged
// source cursors used by a startup disposition. It is not an execution grant.
func BackupWorkStagingResumeSHA256(assignment *agentpb.BackupWorkAssignment, identity workexecution.Identity) ([]byte, error) {
	owned, _, err := OpenBackupWorkAssignment(assignment, identity)
	if err != nil {
		return nil, err
	}
	return backupStagingDigest("groundplane.backup.work-staging-resume.v1\x00", owned)
}
