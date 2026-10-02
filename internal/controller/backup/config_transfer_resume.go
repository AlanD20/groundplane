package backup

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (service *BackupCheckpointService) configTransferResumeEvidence(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	authority *agentpb.BackupTaskAuthority,
	step *agentpb.BackupStepAuthority,
	revision int64,
) (*executionplan.BackupConfigTransferEvidence, error) {
	capture, restore := step.GetCapture().GetConfig(), step.GetRestore().GetConfig()
	if capture == nil && restore == nil {
		return nil, nil
	}
	cursor, found, err := service.repository.ReadConfigTransferCursor(ctx, authority.TaskId, authority.AssignmentId,
		step.StepId, step.ExecutionId, revision)
	if err != nil || !found {
		return nil, err
	}
	owner := cursor.Record.Owner
	direction := agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_CAPTURE
	content := capture.GetContent()
	if restore != nil {
		direction, content = agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE, restore.GetExpectedArchive().
			GetContent()
	}
	if owner.AgentID != agentID || owner.AgentGeneration != agentGeneration ||
		owner.AssignmentGeneration != authority.AssignmentGeneration ||
		owner.AuthoritySHA256 != hex.EncodeToString(step.StepDigest) ||
		owner.Binding.Direction != direction {
		return nil, configSnapshotGuardConflict()
	}
	evidence := &executionplan.BackupConfigTransferEvidence{}
	err = service.repository.VisitConfigTransferCredits(ctx, cursor, func(credit *agentpb.BackupConfigCredit) error {
		if credit.NextOrdinal > content.EntryCount+1 {
			return configSnapshotGuardConflict()
		}
		if credit.GetMetadataAccepted() != nil {
			evidence.MetadataAccepted = credit
		} else if credit.GetValueCredit() != nil {
			evidence.CommittedCredits = append(evidence.CommittedCredits, credit)
		}
		return nil
	})
	return evidence, err
}
