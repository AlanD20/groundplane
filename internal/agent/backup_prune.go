package agent

import (
	"context"
	"crypto/sha256"
	taskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"

	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/s3compatible"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (p *WorkerPool) executeBackupArtifactPrune(
	ctx context.Context,
	assignment taskassignment.Assignment,
	step *agentpb.ExecutionStep,
) error {
	prune := step.GetBackupArtifactPrune()
	if prune == nil || len(prune.GetStoredSha256()) != sha256.Size {
		return errs.New(errs.KindInternal, "agent: Controller sent an invalid Backup prune step")
	}
	pathStyle, err := backupPathStyle(prune.GetConnectorAddressing())
	if err != nil {
		return err
	}
	var storedSHA256 [sha256.Size]byte
	copy(storedSHA256[:], prune.GetStoredSha256())
	authority := backupobject.PruneAuthority{
		Key:             prune.GetProtectedObjectKey(),
		EnvironmentID:   prune.GetEnvironmentId(),
		SourceID:        prune.GetSourceId(),
		RecoveryPointID: prune.GetPointId(),
		StoredSizeBytes: prune.GetStoredSizeBytes(),
		StoredSHA256:    storedSHA256,
	}

	err = p.ConsumeBackupSecretSlot(
		ctx,
		assignment.TaskID,
		assignment.AssignmentID,
		step.GetStepId(),
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY,
		func(accessKey []byte) error {
			return p.ConsumeBackupSecretSlot(
				ctx,
				assignment.TaskID,
				assignment.AssignmentID,
				step.GetStepId(),
				agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY,
				func(secretKey []byte) error {
					adapter, err := s3compatible.New(s3compatible.Config{
						Endpoint:  prune.GetConnectorEndpoint(),
						Bucket:    prune.GetConnectorBucket(),
						Prefix:    prune.GetConnectorPrefix(),
						Region:    prune.GetConnectorRegion(),
						PathStyle: pathStyle,
						AccessKey: string(accessKey),
						SecretKey: string(secretKey),
					})
					if err != nil {
						return err
					}
					return adapter.PruneExact(ctx, authority)
				},
			)
		},
	)
	if err != nil {
		return err
	}

	checkpoint := &agentpb.BackupCheckpointRequest{
		TaskId:       assignment.TaskID,
		AssignmentId: assignment.AssignmentID,
		StepId:       step.GetStepId(),
		Sequence:     1,
		Kind:         agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_REMOTE_OBJECT_ABSENT,
		Payload: &agentpb.BackupCheckpointRequest_RemoteObjectAbsent{
			RemoteObjectAbsent: &agentpb.BackupRemoteObjectAbsentCheckpoint{PointId: prune.GetPointId()},
		},
	}
	checkpoint.ControlPayloadSha256, err = executionplan.ComputeBackupCheckpointPayloadDigest(checkpoint)
	if err != nil {
		return err
	}
	return p.CheckpointBackup(ctx, checkpoint)
}

func backupPathStyle(addressing agentpb.BackupS3Addressing) (bool, error) {
	switch addressing {
	case agentpb.BackupS3Addressing_BACKUP_S3_ADDRESSING_PATH_STYLE:
		return true, nil
	case agentpb.BackupS3Addressing_BACKUP_S3_ADDRESSING_VIRTUAL_HOSTED_STYLE:
		return false, nil
	default:
		return false, errs.New(errs.KindInternal, "agent: Controller sent invalid Backup S3 addressing")
	}
}
