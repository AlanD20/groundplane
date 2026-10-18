package backupruntime

import (
	"context"

	"github.com/AlanD20/groundplane/internal/agent/backupartifact"
	"github.com/AlanD20/groundplane/internal/agent/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/infra/s3compatible"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func downloadConfigRestore(ctx context.Context, assignment *agentpb.BackupWorkAssignment,
	ordinal uint32, staged CaptureStage, exchange SourceExchange,
) (*backupconfiguration.RestoreArtifact, error) {
	return downloadCapturedConfigRestore(ctx, assignment.Input, assignment.Attempt, ordinal, staged, exchange)
}

func downloadCapturedConfigRestore(ctx context.Context, input *agentpb.BackupWorkInput,
	attempt *agentpb.WorkAttemptIdentity, ordinal uint32, staged CaptureStage, exchange SourceExchange,
) (*backupconfiguration.RestoreArtifact, error) {
	step := input.Steps[ordinal]
	stage, source, stored := staged.Stage, staged.Source, staged.Stored
	if err := exchange.RequestBackupSecrets(ctx, attempt, ordinal); err != nil {
		return nil, err
	}
	authority, err := backupartifact.NewRestoreAuthority(input.EnvironmentId, step.GetRestore())
	if err != nil {
		return nil, err
	}
	purpose := agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_CURRENT_AGE_IDENTITY
	if step.GetRestore().Encryption.GetSecretSlotId() == backupsecret.OperatorOldAgeIdentitySlotID {
		purpose = agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_OPERATOR_OLD_AGE_IDENTITY
	}
	var artifact *backupconfiguration.RestoreArtifact
	err = exchange.ConsumeBackupSecret(ctx, attempt, ordinal,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY, func(access []byte) error {
			return exchange.ConsumeBackupSecret(ctx, attempt, ordinal,
				agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY, func(secret []byte) error {
					object := step.GetRestore().SourceObject
					connector := object.Connector
					store, err := s3compatible.New(s3compatible.Config{Endpoint: connector.CanonicalEndpointUrl,
						Bucket: object.Bucket, Prefix: connector.Prefix, Region: connector.Region,
						PathStyle: connector.GetPathStyle(), AccessKey: string(access), SecretKey: string(secret)})
					if err != nil {
						return err
					}
					return exchange.ConsumeBackupSecret(ctx, attempt, ordinal,
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
