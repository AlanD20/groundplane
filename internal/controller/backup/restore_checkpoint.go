package backup

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (service *BackupCheckpointService) checkpointConfigRestore(ctx context.Context,
	input backupruntime.BackupCheckpointInput, step *agentpb.BackupStepAuthority, replayRevision int64,
) (*agentpb.BackupCheckpointAck, error) {
	if step.GetRestore().GetConfig() == nil {
		return nil, configSnapshotGuardConflict()
	}
	current, err := service.repository.GetBackupRestore(ctx, input.TaskID)
	if err != nil {
		return nil, err
	}
	var publication *etcd.ConfigRestoreCheckpointPublication
	var candidate *ConfigRestorePublication
	materializing := input.Request.GetConfig().GetMaterializationVerified() != nil
	if input.Request.GetConfig().GetTransferCompleted() != nil || materializing {
		cursor, found, err := service.repository.ReadConfigTransferCursor(ctx, input.TaskID, input.AssignmentID,
			input.StepID, input.ExecutionID, 0)
		if err != nil {
			return nil, err
		}
		if !found || cursor.Record.Owner.AgentID != input.AgentID ||
			cursor.Record.Owner.AgentGeneration != input.AgentGeneration ||
			cursor.Record.Owner.AssignmentGeneration != input.AssignmentGeneration ||
			cursor.Record.Owner.AuthoritySHA256 != input.AuthoritySHA256 {
			return nil, configSnapshotGuardConflict()
		}
		owner, err := configRestoreTransferOwner(cursor.Record.Owner, step.GetRestore())
		if err != nil {
			return nil, err
		}
		if replayRevision > 0 || materializing {
			candidate, err = service.configRestores.PrepareCandidate(ctx, owner,
				step.GetRestore().GetConfig().GetExpectedArchive().GetContent(), step.GetRestore().PointId)
		} else {
			candidate, err = service.configRestores.PreparePublished(ctx, owner,
				step.GetRestore().GetConfig().GetExpectedArchive().GetContent(), step.GetRestore().PointId)
		}
		if err != nil {
			return nil, err
		}
		defer candidate.Clear()
		if materializing {
			if err := service.configRestores.ProveCandidateFiles(ctx, candidate); err != nil {
				return nil, err
			}
			candidate.RootRevision = current.Record.ConfigProgress.RevisionRootRevision
		}
		publication = &etcd.ConfigRestoreCheckpointPublication{Generation: candidate.SourceSeal,
			Root: candidate.Revision.Seal, RootRevision: candidate.RootRevision, DeleteEntryCount: uint32(len(candidate.DeleteEntries)),
			Projection: candidate.Projection, Identities: candidate.Identities, FileProof: candidate.FileProof}
	}
	if replayRevision > 0 {
		if candidate == nil {
			return nil, configSnapshotGuardConflict()
		}
		if err := service.repository.PublishConfigRestore(ctx, candidate.SourceSeal, candidate.Projection, candidate.Identities); err != nil {
			return nil, err
		}
		return backupCheckpointAcknowledgement(input.Request, replayRevision)
	}
	at := service.now().UTC()
	if !at.After(current.Record.UpdatedAt) {
		at = current.Record.UpdatedAt.Add(time.Nanosecond)
	}
	revision, err := service.repository.CheckpointConfigRestore(ctx, input, current, publication, at)
	if err != nil {
		return nil, err
	}
	if candidate != nil && !materializing {
		if err := service.repository.PublishConfigRestore(ctx, candidate.SourceSeal, candidate.Projection, candidate.Identities); err != nil {
			return nil, err
		}
	}
	return backupCheckpointAcknowledgement(input.Request, revision)
}

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
