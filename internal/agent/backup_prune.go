package agent

import (
	"context"
	taskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/AlanD20/groundplane/internal/infra/s3compatible"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (p *WorkerPool) executeBackupArtifactPrune(
	ctx context.Context,
	assignment taskassignment.Assignment,
	step *agentpb.ExecutionStep,
) error {
	sealed := step.GetBackupStep()
	prune := sealed.GetPrune()
	if prune == nil || assignment.BackupAuthority == nil || assignment.BackupResume == nil {
		return errs.New(errs.KindInternal, "agent: Backup prune assignment authority is incomplete")
	}
	var resume *agentpb.BackupPruneResume
	for _, state := range assignment.BackupResume.Steps {
		if state.StepId == sealed.StepId && state.ExecutionId == sealed.ExecutionId {
			resume = state.GetPrune()
			break
		}
	}
	if resume == nil {
		return errs.New(errs.KindInternal, "agent: Backup prune resume is missing")
	}
	ctx, cancel := context.WithDeadline(ctx, time.Unix(0, int64(sealed.StepDeadlineUnixNano)))
	defer cancel()
	// One sealed step owns one object, so its two secret slots are consumed once.
	if len(prune.Objects) != 1 {
		return errs.New(errs.KindInternal, "agent: Backup prune step must own one object")
	}
	object := prune.Objects[0]
	if object.Ordinal < resume.NextObjectOrdinal {
		return nil
	}
	authority, err := backupPruneObjectAuthority(assignment.Plan.TargetId, object)
	if err != nil {
		return err
	}
	connector := object.Object.Connector
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
						Endpoint:  connector.CanonicalEndpointUrl,
						Bucket:    object.Object.Bucket,
						Prefix:    connector.Prefix,
						Region:    connector.Region,
						PathStyle: connector.GetPathStyle(),
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
		ExecutionId:  sealed.ExecutionId, CheckpointSequence: resume.CheckpointSequence + 1,
		AuthorityDigest: append([]byte(nil), sealed.StepDigest...), PrecedingCheckpoint: resume.PrecedingCheckpoint,
		Checkpoint: &agentpb.BackupCheckpointRequest_PruneObjectDeleted{
			PruneObjectDeleted: &agentpb.BackupPruneObjectDeleted{
				Ordinal: object.Ordinal,
				PointId: object.PointId,
				Object:  proto.Clone(object.Object).(*agentpb.BackupObjectIdentity),
			},
		},
	}
	return p.CheckpointBackup(ctx, checkpoint)
}

func backupPruneObjectAuthority(
	environmentID string,
	object *agentpb.BackupPruneObject,
) (backupobject.PruneAuthority, error) {
	if object == nil || object.Object == nil || object.Object.Connector == nil || object.Evidence == nil {
		return backupobject.PruneAuthority{}, errs.New(errs.KindInternal, "agent: Backup prune object is missing")
	}
	key := object.Object.ObjectKey
	prefix := strings.Trim(object.Object.Connector.Prefix, "/")
	if prefix != "" {
		key = strings.TrimPrefix(key, prefix+"/")
	}
	parts := strings.Split(key, "/")
	if len(parts) != 4 || parts[0] != environmentID || parts[2] != object.PointId {
		return backupobject.PruneAuthority{}, errs.New(errs.KindInternal, "agent: Backup prune object scope is invalid")
	}
	authority := backupobject.PruneAuthority{
		Key:             object.Object.ObjectKey,
		EnvironmentID:   environmentID,
		SourceID:        parts[1],
		RecoveryPointID: object.PointId,
		Evidence: backupobject.Evidence{
			SourceSizeBytes: object.Evidence.SourceSizeBytes,
			StoredSizeBytes: object.Evidence.StoredSizeBytes,
		},
		MetadataCount: object.MetadataCount,
	}
	copy(authority.Evidence.SourceSHA256[:], object.Evidence.SourceSha256)
	copy(authority.Evidence.StoredSHA256[:], object.Evidence.StoredSha256)
	copy(authority.MetadataSHA256[:], object.MetadataSha256)
	switch discriminator := object.Object.Discriminator.(type) {
	case *agentpb.BackupObjectIdentity_VersionId:
		authority.Discriminator = backupobject.Discriminator{Kind: backupobject.DiscriminatorVersionID, Value: discriminator.VersionId.Value}
	case *agentpb.BackupObjectIdentity_Etag:
		authority.Discriminator = backupobject.Discriminator{Kind: backupobject.DiscriminatorETag, Value: discriminator.Etag.Value}
	}
	return authority, authority.Validate(object.Object.Connector.Prefix)
}
