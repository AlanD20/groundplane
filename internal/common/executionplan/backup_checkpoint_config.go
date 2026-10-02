package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func validBackupConfigCheckpoint(value *agentpb.BackupConfigCheckpoint) bool {
	if value == nil {
		return false
	}
	switch checkpoint := value.Checkpoint.(type) {
	case *agentpb.BackupConfigCheckpoint_ValueProgress:
		return checkpoint != nil && checkpoint.ValueProgress != nil && checkpoint.ValueProgress.NextOrdinal > 0 &&
			checkpoint.ValueProgress.NextOrdinal <= backupconfig.MaxEntries+1 && backupCheckpointDigest(checkpoint.ValueProgress.ChainSha256)
	case *agentpb.BackupConfigCheckpoint_TransferCompleted:
		return checkpoint != nil && validBackupConfigTransferCompleted(checkpoint.TransferCompleted)
	case *agentpb.BackupConfigCheckpoint_MaterializationVerified:
		if checkpoint == nil {
			return false
		}
		progress := checkpoint.MaterializationVerified
		return progress != nil && ids.Validate(ids.KindConfig, progress.RestoreGenerationId) == nil &&
			progress.MaterializedEntryCount <= backupconfig.MaxEntries && backupCheckpointDigest(progress.MaterializationSha256)
	default:
		return false
	}
}

func validBackupConfigTransferCompleted(value *agentpb.BackupConfigTransferCompleted) bool {
	return value != nil && ((value.RestoreGenerationId == "" && value.RenderGeneration == 0) ||
		(ids.Validate(ids.KindConfig, value.RestoreGenerationId) == nil && value.RenderGeneration > 0)) &&
		validBackupCheckpointConfigContent(value.Content) && value.CommittedRecordCount > 0 &&
		backupCheckpointDigest(
			value.ValueChainSha256,
		) && backupCheckpointDigest(value.TransferTranscriptSha256)
}
