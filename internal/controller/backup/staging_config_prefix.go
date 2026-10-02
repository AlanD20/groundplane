package backup

import (
	"context"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ResolveBackupConfigStagingPrefix derives retained-file evidence from the
// sealed snapshot selected by the live assignment, never from Agent claims.
func (service *BackupCheckpointService) ResolveBackupConfigStagingPrefix(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	authority *agentpb.BackupTaskAuthority,
	stepID string,
	size uint64,
) (*agentpb.BackupRecoveredFile, error) {
	snapshot, err := service.OpenBackupConfigCapture(ctx, agentID, agentGeneration, authority, stepID)
	if err != nil {
		return nil, err
	}
	evidence, err := snapshot.PrefixEvidence(ctx, size)
	if err != nil {
		return nil, err
	}
	return &agentpb.BackupRecoveredFile{
		Role:      agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_SOURCE_PLAINTEXT,
		SizeBytes: evidence.SizeBytes,
		Sha256:    append([]byte(nil), evidence.SHA256[:]...),
	}, nil
}
