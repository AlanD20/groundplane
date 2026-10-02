package backup

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// CommitBackupConfigCredit is called only after the channel's transfer owner
// matches a credit against actual sent/received records. Native storage rechecks
// the sealed source, live assignment, deadline and Environment operation lock.
func (service *BackupCheckpointService) CommitBackupConfigCredit(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	authority *agentpb.BackupTaskAuthority,
	binding backupconfigtransfer.Binding,
	credit *agentpb.BackupConfigCredit,
) error {
	if service == nil || ctx == nil {
		return configSnapshotInvalid()
	}
	if _, err := executionplan.BackupTaskAuthorityDigest(authority); err != nil {
		return err
	}
	if binding.Validate() != nil ||
		binding.Direction != agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_CAPTURE ||
		binding.TaskID != authority.TaskId ||
		binding.AssignmentID != authority.AssignmentId {
		return configSnapshotGuardConflict()
	}
	for _, step := range authority.Steps {
		if step.StepId != binding.StepID {
			continue
		}
		if step.ExecutionId != binding.ExecutionID || step.GetCapture().GetConfig() == nil {
			return configSnapshotGuardConflict()
		}
		_, err := service.repository.CommitConfigTransferCredit(ctx, backupconfiguration.ConfigTransferOwner{
			Binding: binding, AgentID: agentID, AgentGeneration: agentGeneration,
			AssignmentGeneration: authority.AssignmentGeneration, AuthoritySHA256: hex.EncodeToString(step.StepDigest),
		}, credit)
		return err
	}
	return configSnapshotGuardConflict()
}
