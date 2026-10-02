package backup

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// StreamBackupConfigRestore receives the selected archive's authenticated
// values, not live Entry mutations. Bulk reception runs outside Connect so
// transactions and replay cannot block heartbeat, abort or readiness messages.
func (service *BackupCheckpointService) StreamBackupConfigRestore(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	authority *agentpb.BackupTaskAuthority,
	stepID string,
	transport backupconfigtransfer.ReceiverTransport,
) error {
	if service == nil || service.configSnapshots == nil || service.configRestores == nil || ctx == nil ||
		transport == nil {
		return configSnapshotInvalid()
	}
	digest, err := executionplan.BackupTaskAuthorityDigest(authority)
	if err != nil {
		return err
	}
	claim, err := service.repository.GetBackupCheckpointAssignment(ctx, authority.TaskId)
	if err != nil {
		return err
	}
	assignment := claim.Record
	if assignment.AgentID != agentID || assignment.AgentGeneration != agentGeneration ||
		assignment.AssignmentID != authority.AssignmentId || assignment.BackupAuthorityFence == nil ||
		assignment.BackupAuthorityFence.AssignmentGeneration != authority.AssignmentGeneration ||
		assignment.BackupAuthorityFence.AuthoritySHA256 != hex.EncodeToString(digest) {
		return configSnapshotGuardConflict()
	}
	var step *agentpb.BackupStepAuthority
	for _, candidate := range authority.Steps {
		if candidate.StepId == stepID {
			step = candidate
			break
		}
	}
	config := step.GetRestore().GetConfig()
	if config == nil || !assignment.BackupAuthorityFence.MatchesCheckpoint(authority.AssignmentGeneration,
		step.StepId, step.ExecutionId, hex.EncodeToString(step.StepDigest)) {
		return configSnapshotGuardConflict()
	}
	ctx, cancel := context.WithDeadline(ctx, time.Unix(0, int64(step.StepDeadlineUnixNano)))
	defer cancel()
	cursor, found, err := service.repository.ReadConfigTransferCursor(ctx, authority.TaskId, authority.AssignmentId,
		step.StepId, step.ExecutionId, claim.ReadRevision)
	if err != nil {
		return err
	}
	binding := backupconfigtransfer.Binding{TaskID: authority.TaskId, AssignmentID: authority.AssignmentId,
		StepID: step.StepId, ExecutionID: step.ExecutionId,
		Direction: agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE}
	if found {
		binding.TransferID = cursor.Record.Owner.Binding.TransferID
	} else {
		binding.TransferID = ids.NewULID()
	}
	content := config.GetExpectedArchive().GetContent()
	owner, err := configRestoreTransferOwner(backupconfiguration.ConfigTransferOwner{
		Binding:              binding,
		AgentID:              agentID,
		AgentGeneration:      agentGeneration,
		AssignmentGeneration: authority.AssignmentGeneration,
		AuthoritySHA256:      hex.EncodeToString(step.StepDigest),
	}, step.GetRestore())
	if err != nil {
		return err
	}
	receiver, credit, err := OpenConfigRestoreReceiver(
		ctx,
		owner,
		content,
		service.repository,
		service.configSnapshots.protector,
	)
	if err != nil {
		return err
	}
	defer receiver.Abort()
	if receiver.Progress().Complete {
		if err := service.sealConfigRestoreCompletion(ctx, owner, step.GetRestore().PointId, receiver); err != nil {
			return err
		}
	}
	if err := transport.SendCredit(ctx, credit); err != nil {
		return err
	}
	for !receiver.Progress().Complete {
		frame, err := transport.ReadFrame(ctx)
		if err != nil {
			return err
		}
		credit, err := receiver.AcceptFrame(ctx, frame)
		backupconfigtransfer.ClearFrame(frame)
		if err != nil {
			return err
		}
		if credit != nil {
			if receiver.Progress().Complete {
				if err := service.sealConfigRestoreCompletion(ctx, owner, step.GetRestore().PointId, receiver); err != nil {
					return err
				}
			}
			if err := transport.SendCredit(ctx, credit); err != nil {
				return err
			}
		}
	}
	return nil
}

// The final grant cannot precede durable complete-generation publication.
// Reconnect repeats only this exact seal before redelivering a lost final grant.
func (service *BackupCheckpointService) sealConfigRestoreCompletion(ctx context.Context,
	owner backupconfiguration.ConfigRestoreTransferOwner, pointID string, receiver *ConfigRestoreReceiver,
) error {
	completed, err := receiver.Completed()
	if err != nil {
		return err
	}
	_, err = service.repository.SealConfigRestoreGeneration(ctx, backupconfiguration.ConfigRestoreGenerationRecord{
		Owner: owner, Completed: completed,
	})
	if err != nil {
		return err
	}
	current, err := service.repository.GetBackupRestore(ctx, owner.Transfer.Binding.TaskID)
	if err != nil {
		return err
	}
	if current.Record.Point.ID != pointID || current.Record.RestoreGenerationID != owner.GenerationID ||
		current.Record.ConfigProgress == nil {
		return configSnapshotGuardConflict()
	}
	var publication *ConfigRestorePublication
	if current.Record.ConfigProgress.RevisionRootRevision == 0 {
		publication, err = service.configRestores.PreparePublished(ctx, owner, completed.Content, pointID)
	} else {
		// Native staging already has its completion receipt. Reconnection
		// resumes that publication rather than trying to stage against a head
		// the same Restore may already have switched.
		publication, err = service.configRestores.PrepareCandidate(ctx, owner, completed.Content, pointID)
	}
	if err != nil {
		return err
	}
	defer publication.Clear()
	if current.Record.ConfigProgress.RevisionRootRevision > 0 {
		return service.repository.PublishConfigRestore(
			ctx,
			publication.SourceSeal,
			publication.Projection,
			publication.Identities,
		)
	}
	return nil
}
