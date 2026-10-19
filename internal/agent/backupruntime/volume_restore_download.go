package backupruntime

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/agent/backupartifact"
	"github.com/AlanD20/groundplane/internal/common/backupformat"
	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/backupvolume"
	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"github.com/AlanD20/groundplane/internal/infra/s3compatible"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type volumeRestoreArtifact struct {
	staging   CaptureStage
	stage     *backupstage.Stage
	source    *backupstage.Artifact
	stored    *backupstage.Artifact
	validated backupvolume.ValidatedArchive
	manifest  []backupvolume.ManifestEntryBytes
	proof     *agentpb.BackupRestoreArtifactValidated
}

// downloadVolumeRestore authenticates the exact Point object and its entire
// canonical archive before the caller is permitted to stop or mutate Services.
func downloadVolumeRestore(
	ctx context.Context,
	assignment *agentpb.BackupWorkAssignment,
	identity workexecution.Identity,
	ordinal uint32,
	staging *Staging,
	exchange SourceExchange,
	expectedEntries []backupvolume.Entry,
	expectedManifest []backupvolume.ManifestEntryBytes,
) (_ *volumeRestoreArtifact, resultErr error) {
	if assignment == nil || assignment.Input == nil || uint64(ordinal) >= uint64(len(assignment.Input.Steps)) {
		return nil, errs.New(errs.KindStateConflict, "Volume Restore source is missing")
	}
	step := assignment.Input.Steps[ordinal]
	if staging == nil || exchange == nil || step.GetRestore().GetVolume() == nil ||
		len(expectedEntries) == 0 || len(expectedManifest) != len(expectedEntries) {
		return nil, errs.New(errs.KindStateConflict, "Volume Restore artifact differs from the selected Point manifest")
	}
	staged, err := staging.Restore(ctx, assignment, identity, ordinal)
	if err != nil {
		return nil, err
	}
	return downloadCapturedVolumeRestore(
		ctx,
		assignment.Input,
		assignment.Attempt,
		ordinal,
		staged,
		exchange,
		expectedEntries,
		expectedManifest,
	)
}

func downloadCapturedVolumeRestore(ctx context.Context, input *agentpb.BackupWorkInput,
	attempt *agentpb.WorkAttemptIdentity, ordinal uint32, staged CaptureStage, exchange SourceExchange,
	expectedEntries []backupvolume.Entry, expectedManifest []backupvolume.ManifestEntryBytes,
) (*volumeRestoreArtifact, error) {
	if input == nil || attempt == nil || uint64(ordinal) >= uint64(len(input.Steps)) || exchange == nil ||
		staged.Stage == nil {
		return nil, errs.New(errs.KindStateConflict, "Volume Restore download authority is unavailable")
	}
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
	var download backupartifact.DownloadResult
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
					return exchange.ConsumeBackupSecret(ctx, attempt, ordinal,
						purpose, downloadWithIdentity)
				})
		})
	if err != nil {
		return nil, err
	}
	archive, err := backupvolumetransfer.ArchiveEvidenceFromWire(step.GetRestore().GetVolume().Archive)
	if err != nil {
		return nil, err
	}
	expected := backupvolume.ArtifactEvidence{Source: backupformat.Evidence{
		SizeBytes: download.SourceEvidence.Size, SHA256: download.SourceEvidence.SHA256}, Archive: archive}
	if expected.Source.SizeBytes != step.GetRestore().ExpectedEvidence.SourceSizeBytes ||
		!bytes.Equal(expected.Source.SHA256[:], step.GetRestore().ExpectedEvidence.SourceSha256) ||
		backupvolumetransfer.VerifyEntries(
			expectedEntries,
			expectedManifest,
			step.GetRestore().GetVolume().Archive,
		) != nil {
		return nil, errs.New(errs.KindStateConflict, "Volume Restore artifact differs from the selected Point manifest")
	}
	reader, err := download.Source.OpenPrefix(ctx)
	if err != nil {
		return nil, err
	}
	validated, validateErr := backupvolume.Validate(ctx, reader, expectedManifest, expected)
	closeErr := reader.Close()
	if validateErr != nil {
		return nil, validateErr
	}
	if closeErr != nil {
		return nil, errs.Wrap(errs.KindStorageUnavailable, closeErr)
	}
	if len(validated.Entries) != len(expectedEntries) {
		return nil, errs.New(errs.KindStateConflict, "Volume Restore artifact differs from the selected Point manifest")
	}
	for index := range validated.Entries {
		left, right := validated.Entries[index], expectedEntries[index]
		if !bytes.Equal(left.Path, right.Path) || left.Kind != right.Kind || left.Mode != right.Mode ||
			left.UID != right.UID || left.GID != right.GID || left.SizeBytes != right.SizeBytes ||
			left.ContentSHA256 != right.ContentSHA256 {
			return nil, errs.New(
				errs.KindStateConflict,
				"Volume Restore artifact differs from the selected Point manifest",
			)
		}
	}
	sameInode := download.Source == download.Stored
	proof := &agentpb.BackupRestoreArtifactValidated{PointId: step.GetRestore().PointId,
		Object: proto.CloneOf(
			step.GetRestore().SourceObject,
		), Evidence: proto.CloneOf(step.GetRestore().ExpectedEvidence),
		Finals: &agentpb.BackupStagingFinals{SourceRelativeName: download.SourceEvidence.Name,
			StoredRelativeName: download.StoredEvidence.Name, SameInode: proto.Bool(sameInode)},
		Archive: &agentpb.BackupRestoreArtifactValidated_Volume{
			Volume: proto.CloneOf(step.GetRestore().GetVolume().Archive)},
	}
	return &volumeRestoreArtifact{staging: staged, stage: stage, source: download.Source, stored: download.Stored,
		validated: validated, manifest: expectedManifest, proof: proof}, nil
}
