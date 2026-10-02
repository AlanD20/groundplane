package backup

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ResolveBackupStagingPrepared retains the actual Prepared receipt across later
// Upload checkpoints; the last checkpoint alone omits staging-final evidence.
func (service *BackupCheckpointService) ResolveBackupStagingPrepared(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	authority *agentpb.BackupTaskAuthority,
	stepID string,
) (*agentpb.BackupArtifactPrepared, error) {
	digest, err := executionplan.BackupTaskAuthorityDigest(authority)
	if err != nil {
		return nil, err
	}
	claim, err := service.repository.GetBackupCheckpointAssignment(ctx, authority.TaskId)
	if err != nil {
		return nil, err
	}
	assignment := claim.Record
	if claim.ReadRevision <= 0 || assignment.AgentID != agentID || assignment.AgentGeneration != agentGeneration ||
		assignment.AssignmentID != authority.AssignmentId || assignment.BackupAuthorityFence == nil ||
		assignment.BackupAuthorityFence.AssignmentGeneration != authority.AssignmentGeneration ||
		assignment.BackupAuthorityFence.AuthoritySHA256 != hex.EncodeToString(digest) {
		return nil, errs.New(errs.KindStateConflict, "Backup staging assignment authority changed")
	}
	for _, step := range authority.Steps {
		if step.StepId != stepID {
			continue
		}
		var prepared *agentpb.BackupArtifactPrepared
		err := service.repository.VisitBackupStepCheckpoints(
			ctx,
			authority,
			step,
			claim.ReadRevision,
			func(receipt backupruntime.BackupCommittedCheckpoint) error {
				if value := receipt.Request.GetArtifactPrepared(); value != nil {
					prepared = proto.CloneOf(value)
				}
				return nil
			},
		)
		return prepared, err
	}
	return nil, errs.New(errs.KindStateConflict, "Backup staging step is outside its assignment")
}
