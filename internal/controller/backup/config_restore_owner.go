package backup

import (
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// The stream and its ordinary checkpoint use the same generation authority.
func configRestoreTransferOwner(transfer backupconfiguration.ConfigTransferOwner,
	restore *agentpb.BackupRestoreAuthority,
) (backupconfiguration.ConfigRestoreTransferOwner, error) {
	var zero backupconfiguration.ConfigRestoreTransferOwner
	config := restore.GetConfig()
	if config == nil || transfer.Binding.Direction != agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE {
		return zero, configSnapshotInvalid()
	}
	contentSHA, err := backupconfigtransfer.ContentSHA256(config.GetExpectedArchive().GetContent())
	if err != nil {
		return zero, err
	}
	evidence := restore.GetExpectedEvidence()
	owner := backupconfiguration.ConfigRestoreTransferOwner{Transfer: transfer,
		GenerationID: config.RestoreGenerationId, RenderGeneration: config.RenderGeneration,
		EnvironmentID: config.DestinationEnvironmentId, BaselineRevisionID: config.BaselineRevisionId,
		BaselineHeadRevision: config.BaselineHeadRevision, ContentSHA256: hex.EncodeToString(contentSHA),
		SourceSizeBytes: evidence.GetSourceSizeBytes(), SourceSHA256: hex.EncodeToString(evidence.GetSourceSha256())}
	if err := backupconfiguration.ValidateConfigRestoreTransferOwner(owner); err != nil {
		return zero, err
	}
	return owner, nil
}
