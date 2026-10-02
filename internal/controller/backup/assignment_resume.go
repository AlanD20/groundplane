package backup

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ResolveBackupTaskResume requires a fixed-revision, contiguous receipt history
// under the durable assignment fence. A last checkpoint is not cumulative state.
func (service *BackupCheckpointService) ResolveBackupTaskResume(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	authority *agentpb.BackupTaskAuthority,
) (*agentpb.BackupTaskResume, error) {
	if ctx == nil || service == nil || service.repository == nil {
		return nil, errs.New(errs.KindInternal, "backup resume service is not configured")
	}
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
		assignment.AssignmentID != authority.AssignmentId ||
		assignment.BackupAuthorityFence == nil ||
		assignment.BackupAuthorityFence.AssignmentGeneration != authority.AssignmentGeneration ||
		assignment.BackupAuthorityFence.AuthoritySHA256 != hex.EncodeToString(digest) {
		return nil, errs.New(errs.KindStateConflict, "backup assignment resume authority changed")
	}
	if err := service.captureAssignmentConfig(ctx, authority); err != nil {
		return nil, err
	}
	resume := &agentpb.BackupTaskResume{
		AssignmentId:         authority.AssignmentId,
		AssignmentGeneration: authority.AssignmentGeneration,
	}
	for _, step := range authority.Steps {
		configEvidence, err := service.configTransferResumeEvidence(
			ctx,
			agentID,
			agentGeneration,
			authority,
			step,
			claim.ReadRevision,
		)
		if err != nil {
			return nil, err
		}
		replay, err := executionplan.NewBackupStepResumeReplay(authority, step, configEvidence)
		if err != nil {
			return nil, err
		}
		err = service.repository.VisitBackupStepCheckpoints(ctx, authority, step, claim.ReadRevision,
			func(checkpoint backupruntime.BackupCommittedCheckpoint) error {
				return replay.Accept(
					executionplan.BackupCheckpointReplayEntry{Request: checkpoint.Request, Fence: checkpoint.Fence},
				)
			})
		if err != nil {
			return nil, err
		}
		state, err := replay.Resume()
		if err != nil {
			return nil, err
		}
		resume.Steps = append(resume.Steps, state)
	}
	return executionplan.ValidateBackupTaskResume(resume, authority)
}
