package agent

import (
	"context"

	"github.com/AlanD20/groundplane/internal/agent/backupartifact"
	"github.com/AlanD20/groundplane/internal/agent/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"github.com/AlanD20/groundplane/internal/infra/s3compatible"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (pool *WorkerPool) downloadConfigRestore(ctx context.Context, assignment taskassignment.Assignment,
	step *agentpb.BackupStepAuthority, stage *backupstage.Stage, source, stored *backupstage.Artifact,
) (*backupconfiguration.RestoreArtifact, error) {
	authority, err := backupartifact.NewRestoreAuthority(assignment.BackupAuthority, step)
	if err != nil {
		return nil, err
	}
	purpose := agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_CURRENT_AGE_IDENTITY
	if step.GetRestore().Encryption.GetSecretSlotId() == backupsecret.OperatorOldAgeIdentitySlotID {
		purpose = agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_OPERATOR_OLD_AGE_IDENTITY
	}
	var artifact *backupconfiguration.RestoreArtifact
	err = pool.ConsumeBackupSecretSlot(ctx, assignment.TaskID, assignment.AssignmentID, step.StepId,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY, func(access []byte) error {
			return pool.ConsumeBackupSecretSlot(ctx, assignment.TaskID, assignment.AssignmentID, step.StepId,
				agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY, func(secret []byte) error {
					object := step.GetRestore().SourceObject
					connector := object.Connector
					store, err := s3compatible.New(s3compatible.Config{Endpoint: connector.CanonicalEndpointUrl,
						Bucket: object.Bucket, Prefix: connector.Prefix, Region: connector.Region,
						PathStyle: connector.GetPathStyle(), AccessKey: string(access), SecretKey: string(secret)})
					if err != nil {
						return err
					}
					return pool.ConsumeBackupSecretSlot(ctx, assignment.TaskID, assignment.AssignmentID, step.StepId,
						purpose, func(identity []byte) error {
							var err error
							artifact, err = backupconfiguration.PrepareRestore(ctx, backupartifact.DownloadInput{
								Authority: authority, Stage: stage, Store: store, Identity: identity, Source: source, Stored: stored})
							return err
						})
				})
		})
	return artifact, err
}
