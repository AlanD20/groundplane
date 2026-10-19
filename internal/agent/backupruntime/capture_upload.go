package backupruntime

import (
	"context"

	"github.com/AlanD20/groundplane/internal/agent/backupartifact"
	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"github.com/AlanD20/groundplane/internal/infra/s3compatible"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// uploadCapture consumes only this source's granted credentials. The caller
// owns staging; the publisher supplies the acknowledged source cursor. Success proves the exact upload
// checkpoint, not source cleanup or completion of the whole Backup.
func uploadCapture(ctx context.Context, assignment *agentpb.BackupWorkAssignment, ordinal uint32,
	prepared *agentpb.BackupArtifactPrepared,
	stored *backupstage.Artifact, delivery *checkpointdelivery.Backup, exchange SourceExchange,
) (backupartifact.Result, error) {
	if assignment == nil || assignment.Input == nil ||
		assignment.Input.Operation != agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_CAPTURE ||
		uint64(ordinal) >= uint64(len(assignment.Input.Steps)) || exchange == nil {
		return backupartifact.Result{}, errs.New(errs.KindStateConflict, "Backup upload requires its captured source")
	}
	capture := assignment.Input.Steps[ordinal].GetCapture()
	authority, err := backupartifact.NewAuthority(assignment.Input.EnvironmentId, capture)
	if err != nil {
		return backupartifact.Result{}, err
	}
	publisher, err := delivery.Source(ordinal)
	if err != nil {
		return backupartifact.Result{}, err
	}
	cursor, err := publisher.Current(ctx)
	if err != nil {
		return backupartifact.Result{}, err
	}
	resume := cursor.GetCapture()
	if resume == nil {
		return backupartifact.Result{}, errs.New(errs.KindStateConflict, "Backup capture cursor is unavailable")
	}
	if resume.PreparedArtifact != nil && !proto.Equal(resume.PreparedArtifact, prepared) {
		return backupartifact.Result{}, errs.New(
			errs.KindStateConflict,
			"Backup staged artifact differs from acknowledged preparation",
		)
	}
	var result backupartifact.Result
	err = exchange.ConsumeBackupSecret(ctx, assignment.Attempt, ordinal,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY, func(access []byte) error {
			return exchange.ConsumeBackupSecret(ctx, assignment.Attempt, ordinal,
				agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY, func(secret []byte) error {
					connector := capture.Target.Connector
					store, err := s3compatible.New(s3compatible.Config{Endpoint: connector.CanonicalEndpointUrl,
						Bucket: capture.Target.Bucket, Prefix: connector.Prefix, Region: connector.Region,
						PathStyle: connector.GetPathStyle(), AccessKey: string(access), SecretKey: string(secret)})
					if err != nil {
						return err
					}
					result, err = backupartifact.Upload(ctx, backupartifact.UploadInput{
						Authority: authority, Prepared: prepared, Phase: resume.Phase,
						UploadCompleted: resume.GetUploadCompleted(), UploadVerified: resume.GetUploadVerified(),
						Stored: stored, Store: store, Publish: publisher,
					})
					return err
				})
		})
	return result, err
}
