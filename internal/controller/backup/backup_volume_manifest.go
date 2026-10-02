package backup

import (
	"bytes"
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupvolumemanifest"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (service *BackupCheckpointService) BeginVolumeManifest(ctx context.Context,
	agentID string, agentGeneration uint64, taskID, assignmentID, stepID string,
	direction agentpb.BackupVolumeManifestDirection,
) (*agentpb.BackupVolumeManifestAckCredit, bool, error) {
	owner, err := service.volumeManifestOwner(ctx, agentID, agentGeneration,
		taskID, assignmentID, stepID, direction)
	if err != nil {
		return nil, false, err
	}
	repository := service.repository.VolumeManifestRepository()
	credit, err := repository.Open(ctx, owner)
	if err != nil {
		return nil, false, err
	}
	cursor, found, err := repository.Read(ctx, owner, 0)
	if err != nil || !found {
		return nil, false, invalidVolumeManifestAuthority()
	}
	return credit, cursor.Record.Complete, nil
}

func (service *BackupCheckpointService) AcceptVolumeManifestFrame(ctx context.Context,
	agentID string, agentGeneration uint64,
	frame *agentpb.BackupVolumeManifestTransfer,
) (*agentpb.BackupVolumeManifestAckCredit, error) {
	if frame == nil {
		return nil, invalidVolumeManifestAuthority()
	}
	owner, err := service.volumeManifestOwner(ctx, agentID, agentGeneration,
		frame.TaskId, frame.AssignmentId, frame.StepId, frame.Direction)
	if err != nil {
		return nil, err
	}
	return service.repository.VolumeManifestRepository().Accept(ctx, owner, frame)
}

func (service *BackupCheckpointService) ReadCompleteVolumeManifest(ctx context.Context,
	agentID string, agentGeneration uint64, taskID, assignmentID, stepID string,
	direction agentpb.BackupVolumeManifestDirection, revision int64,
) (backupvolumemanifest.Complete, error) {
	owner, err := service.volumeManifestOwner(ctx, agentID, agentGeneration,
		taskID, assignmentID, stepID, direction)
	if err != nil {
		return backupvolumemanifest.Complete{}, err
	}
	return service.repository.VolumeManifestRepository().ReadComplete(ctx, owner, revision)
}

func (service *BackupCheckpointService) volumeManifestOwner(ctx context.Context,
	agentID string, agentGeneration uint64, taskID, assignmentID, stepID string,
	direction agentpb.BackupVolumeManifestDirection,
) (backupvolumemanifest.Owner, error) {
	if service == nil || service.repository == nil || ctx == nil || agentID == "" || agentGeneration == 0 {
		return backupvolumemanifest.Owner{}, invalidVolumeManifestAuthority()
	}
	claim, err := service.repository.GetBackupCheckpointAssignment(ctx, taskID)
	if err != nil {
		return backupvolumemanifest.Owner{}, err
	}
	assignment := claim.Record
	if claim.Revision <= 0 || assignment.BackupAuthorityFence == nil ||
		assignment.AssignmentID != assignmentID || assignment.AgentID != agentID ||
		assignment.AgentGeneration != agentGeneration || !time.Now().UTC().Before(assignment.Deadline) {
		return backupvolumemanifest.Owner{}, invalidVolumeManifestAuthority()
	}
	sealed, err := service.repository.GetBackupExecutionPlan(ctx, taskID)
	if err != nil {
		return backupvolumemanifest.Owner{}, err
	}
	operationID := ""
	switch sealed.Record.Operation {
	case agentpb.PlanOperation_PLAN_OPERATION_BACKUP:
		run, err := service.repository.GetBackupRun(ctx, taskID)
		if err != nil {
			return backupvolumemanifest.Owner{}, err
		}
		operationID = run.Record.OperationID
	case agentpb.PlanOperation_PLAN_OPERATION_RESTORE:
		restore, err := service.repository.GetBackupRestore(ctx, taskID)
		if err != nil {
			return backupvolumemanifest.Owner{}, err
		}
		operationID = restore.Record.OperationID
	default:
		return backupvolumemanifest.Owner{}, invalidVolumeManifestAuthority()
	}
	authority, digest, err := executionplan.BindBackupTaskAuthority(sealed.Record,
		executionplan.BackupAssignmentIdentity{TaskID: taskID, OperationID: operationID,
			AssignmentID: assignmentID, Generation: assignment.BackupAuthorityFence.AssignmentGeneration,
			DeadlineUnixNano: uint64(assignment.Deadline.UnixNano())})
	if err != nil || hex.EncodeToString(digest) != assignment.BackupAuthorityFence.AuthoritySHA256 {
		return backupvolumemanifest.Owner{}, invalidVolumeManifestAuthority()
	}
	for _, step := range authority.Steps {
		if step.StepId != stepID ||
			!assignment.BackupAuthorityFence.MatchesCheckpoint(
				authority.AssignmentGeneration, step.StepId, step.ExecutionId,
				hex.EncodeToString(step.StepDigest)) {
			continue
		}
		valid := direction == agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_CAPTURE &&
			step.GetCapture().GetVolume() != nil || step.GetRestore().GetVolume() != nil &&
			(direction == agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_NEW ||
				direction == agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_OLD)
		if !valid {
			return backupvolumemanifest.Owner{}, invalidVolumeManifestAuthority()
		}
		transferID, err := backupvolumetransfer.TransferID(step.ExecutionId, direction)
		if err != nil {
			return backupvolumemanifest.Owner{}, err
		}
		binding := backupvolumetransfer.Binding{TaskID: taskID, AssignmentID: assignmentID,
			StepID: stepID, TransferID: transferID, Direction: direction}
		copy(binding.AuthorityDigest[:], step.StepDigest)
		owner := backupvolumemanifest.Owner{Binding: binding, AgentID: agentID,
			AgentGeneration: agentGeneration, AssignmentGeneration: authority.AssignmentGeneration}
		return owner, owner.Validate()
	}
	return backupvolumemanifest.Owner{}, invalidVolumeManifestAuthority()
}

func invalidVolumeManifestAuthority() error {
	return errs.New(errs.KindStateConflict, "Volume manifest assignment authority changed")
}

func (service *BackupCheckpointService) ReadPointVolumeManifest(ctx context.Context,
	point backupruntime.BackupRecoveryPointSnapshot,
) (backupvolumemanifest.Complete, error) {
	if service == nil || service.repository == nil || ctx == nil ||
		point.SourceKind != backupruntime.BackupRuntimeSourceVolume ||
		backupruntime.ValidateBackupRecoveryPointSnapshot(point) != nil {
		return backupvolumemanifest.Complete{}, invalidVolumeManifestAuthority()
	}
	owner, err := point.VolumeArchive.Manifest.Owner()
	if err != nil {
		return backupvolumemanifest.Complete{}, err
	}
	complete, err := service.repository.VolumeManifestRepository().ReadComplete(ctx, owner, 0)
	if err != nil {
		return backupvolumemanifest.Complete{}, err
	}
	archive := point.VolumeArchive
	archive.Manifest = backupruntime.BackupVolumeManifestReference{}
	wire, err := archive.Wire()
	if err != nil || complete.Revision != point.VolumeArchive.Manifest.CursorRevision ||
		complete.Start.PointId != point.ID || complete.Start.Role !=
		agentpb.BackupVolumeManifestRole_BACKUP_VOLUME_MANIFEST_ROLE_CAPTURED ||
		complete.Source == nil || complete.Source.SizeBytes != point.Evidence.SourceSizeBytes ||
		!bytes.Equal(complete.Source.SHA256[:], mustDecodeVolumeSHA(point.Evidence.SourceSHA256)) ||
		!bytes.Equal(wire.ContentManifestSha256, complete.Archive.ContentManifestSHA256[:]) ||
		!bytes.Equal(wire.FullTreeSha256, complete.Archive.FullTreeSHA256[:]) ||
		wire.EntryCount != complete.Archive.EntryCount || wire.SourceSizeBytes != complete.Archive.SourceSizeBytes {
		return backupvolumemanifest.Complete{}, invalidVolumeManifestAuthority()
	}
	return complete, nil
}

func (service *BackupCheckpointService) ReadRestoreNewVolumeManifest(ctx context.Context,
	agentID string, agentGeneration uint64, taskID, assignmentID, stepID string,
) (backupvolumemanifest.Complete, error) {
	_, err := service.volumeManifestOwner(ctx, agentID, agentGeneration, taskID, assignmentID, stepID,
		agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_NEW)
	if err != nil {
		return backupvolumemanifest.Complete{}, err
	}
	restore, err := service.repository.GetBackupRestore(ctx, taskID)
	if err != nil || restore.Record.Point.SourceKind != backupruntime.BackupRuntimeSourceVolume ||
		restore.Record.State == backupruntime.BackupRestoreFailedSafe ||
		restore.Record.State == backupruntime.BackupRestoreRecoveryRequired {
		return backupvolumemanifest.Complete{}, invalidVolumeManifestAuthority()
	}
	return service.ReadPointVolumeManifest(ctx, restore.Record.Point)
}

func mustDecodeVolumeSHA(value string) []byte {
	digest, _ := hex.DecodeString(value)
	return digest
}
