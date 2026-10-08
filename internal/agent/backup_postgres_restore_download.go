package agent

import (
	"context"

	"github.com/AlanD20/groundplane/internal/agent/backupartifact"
	"github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"github.com/AlanD20/groundplane/internal/infra/s3compatible"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type databaseRestoreArtifact struct {
	stage    *backupstage.Stage
	source   *backupstage.Artifact
	evidence backupstage.ArtifactEvidence
	proof    *agentpb.BackupRestoreArtifactValidated
}

func (pool *WorkerPool) downloadDatabaseRestore(ctx context.Context,
	assignment taskassignment.Assignment, step *agentpb.BackupStepAuthority,
) (*databaseRestoreArtifact, error) {
	stage, source, stored, err := pool.backupStaging.databaseRestoreStage(ctx, assignment.TaskID, step)
	if err != nil {
		return nil, err
	}
	authority, err := backupartifact.NewRestoreAuthority(assignment.BackupAuthority, step)
	if err != nil {
		return nil, err
	}
	purpose := agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_CURRENT_AGE_IDENTITY
	if step.GetRestore().Encryption.GetSecretSlotId() == backupsecret.OperatorOldAgeIdentitySlotID {
		purpose = agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_OPERATOR_OLD_AGE_IDENTITY
	}
	var download backupartifact.DownloadResult
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
					downloadWithIdentity := func(identity []byte) error {
						var err error
						download, err = backupartifact.Download(ctx, backupartifact.DownloadInput{
							Authority: authority, Stage: stage, Store: store, Identity: identity,
							Source: source, Stored: stored})
						return err
					}
					if step.GetRestore().Encryption.GetKind() == agentpb.BackupEncryption_BACKUP_ENCRYPTION_NONE {
						return downloadWithIdentity(nil)
					}
					return pool.ConsumeBackupSecretSlot(ctx, assignment.TaskID, assignment.AssignmentID, step.StepId,
						purpose, downloadWithIdentity)
				})
		})
	if err != nil {
		return nil, err
	}
	if download.SourceEvidence.Size == 0 ||
		download.SourceEvidence.Size > postgres16protocol.MaximumRestoreSourceBytes {
		return nil, invalidAgentStaging()
	}
	proof := &agentpb.BackupRestoreArtifactValidated{PointId: step.GetRestore().PointId,
		Object: proto.CloneOf(
			step.GetRestore().SourceObject,
		), Evidence: proto.CloneOf(step.GetRestore().ExpectedEvidence),
		Finals: &agentpb.BackupStagingFinals{SourceRelativeName: download.SourceEvidence.Name,
			StoredRelativeName: download.StoredEvidence.Name, SameInode: proto.Bool(download.Source == download.Stored)},
	}
	if mysql := step.GetRestore().GetMysql(); mysql != nil {
		proof.Archive = &agentpb.BackupRestoreArtifactValidated_Mysql{Mysql: proto.CloneOf(mysql.ExpectedArchive)}
	} else {
		proof.Archive = &agentpb.BackupRestoreArtifactValidated_Postgres{Postgres: proto.CloneOf(step.GetRestore().GetPostgres().ExpectedArchive)}
	}
	return &databaseRestoreArtifact{stage: stage, source: download.Source,
		evidence: download.SourceEvidence, proof: proof}, nil
}
