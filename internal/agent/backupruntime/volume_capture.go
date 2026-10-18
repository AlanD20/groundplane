package backupruntime

import (
	"bytes"
	"context"
	"github.com/AlanD20/groundplane/internal/agent/backupartifact"
	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/common/backupformat"
	"github.com/AlanD20/groundplane/internal/common/backupvolume"
	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"github.com/AlanD20/groundplane/internal/infra/backupvolumefs"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"time"
)

func captureVolume(ctx context.Context, assignment *agentpb.BackupWorkAssignment,
	identity workexecution.Identity, ordinal uint32, staging *Staging, volumeRoot string,
	delivery *checkpointdelivery.Backup, exchange Exchange,
) (resultErr error) {
	binding, err := backupvolumetransfer.Bind(assignment, identity, ordinal, agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_CAPTURE)
	if err != nil {
		return err
	}
	step := assignment.Input.Steps[ordinal]
	capture := step.GetCapture()
	publisher, err := delivery.Source(ordinal)
	if err != nil {
		return err
	}
	cursor, err := publisher.Current(ctx)
	if err != nil {
		return err
	}
	resume := cursor.GetCapture()
	if resume == nil {
		return errs.New(errs.KindStateConflict, "Volume capture cursor is missing")
	}
	ctx, cancel := context.WithDeadline(ctx, time.Unix(0, int64(step.StepDeadlineUnixNano)))
	defer cancel()
	absent, err := staging.SourceAbsent(assignment, identity, ordinal)
	if err != nil {
		return err
	}
	if resume.GetSourceCleanupCompleted() != nil {
		if !absent {
			return errs.New(errs.KindStateConflict, "Completed Volume capture still has staged files")
		}
		return nil
	}
	if resume.Phase == agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_POINT_COMMIT && absent {
		return publisher.SourceCleanupCompleted(ctx, &agentpb.BackupSourceCleanupCompleted{PointId: capture.PointId, Evidence: proto.CloneOf(resume.GetUploadVerified().GetEvidence())})
	}
	captured, err := staging.VolumeCapture(ctx, assignment, identity, ordinal, resume)
	if err != nil {
		return err
	}
	stage, source, sourceEvidence, stored := captured.Stage, captured.Source, captured.SourceEvidence, captured.Stored
	if err := exchange.RequestBackupSecrets(ctx, assignment.Attempt, ordinal); err != nil {
		return err
	}
	volumeAuthority := capture.GetVolume()
	var archiveEvidence backupvolume.ArtifactEvidence
	var entries []backupvolume.Entry
	if sourceEvidence.Name == "" {
		volume, err := backupvolumefs.Open(ctx, volumeRoot,
			volumeAuthority.Projection.AuthorizedVolumeDir, volumeAuthority.Projection.ComposeVolumeKey)
		if err != nil {
			return err
		}
		defer func() {
			if closeErr := volume.Close(); closeErr != nil {
				resultErr = errs.WrapJoined(errs.KindStorageUnavailable, resultErr, closeErr)
			}
		}()
		writer := &volumeArtifactWriter{ctx: ctx, artifact: source}
		tree, _, captured, err := volume.Capture(ctx, writer, backupvolumetransfer.EncodeEntries)
		if err != nil {
			return err
		}
		entries, archiveEvidence = tree.Entries, captured
		sourceEvidence, err = source.Publish(ctx)
		if err != nil {
			return err
		}
		if sourceEvidence.Size != captured.Source.SizeBytes || sourceEvidence.SHA256 != captured.Source.SHA256 {
			return errs.New(errs.KindStateConflict, "Volume capture evidence differs from its staged artifact")
		}
	} else {
		prepared := resume.PreparedArtifact
		if prepared.GetVolume() == nil || prepared.Evidence == nil ||
			prepared.Evidence.SourceSizeBytes != sourceEvidence.Size ||
			!bytes.Equal(prepared.Evidence.SourceSha256, sourceEvidence.SHA256[:]) {
			return errs.New(errs.KindStateConflict, "Volume capture evidence differs from its staged artifact")
		}
		archive, err := backupvolumetransfer.ArchiveEvidenceFromWire(prepared.GetVolume())
		if err != nil {
			return err
		}
		archiveEvidence = backupvolume.ArtifactEvidence{Source: backupformat.Evidence{
			SizeBytes: sourceEvidence.Size, SHA256: sourceEvidence.SHA256}, Archive: archive}
		reader, err := source.OpenPrefix(ctx)
		if err != nil {
			return err
		}
		validated, _, inspectErr := backupvolumefs.InspectCapturedArchive(ctx, reader, archiveEvidence,
			backupvolumetransfer.EncodeEntries)
		closeErr := reader.Close()
		if inspectErr != nil {
			return inspectErr
		}
		if closeErr != nil {
			return errs.Wrap(errs.KindStorageUnavailable, closeErr)
		}
		entries = validated.Entries
	}
	if archiveEvidence.Source.SizeBytes > volumeAuthority.SourceSizeUpperBound {
		return errs.New(errs.KindStateConflict, "Volume capture evidence differs from its staged artifact")
	}
	if err := sendVolumeManifest(ctx, binding, step,
		agentpb.BackupVolumeManifestRole_BACKUP_VOLUME_MANIFEST_ROLE_CAPTURED,
		entries, &archiveEvidence, exchange); err != nil {
		return err
	}
	storedEvidence := sourceEvidence
	if capture.Encryption.Kind == agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE {
		err = exchange.ConsumeBackupSecret(
			ctx,
			assignment.Attempt,
			ordinal,
			agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_CURRENT_AGE_IDENTITY,
			func(identity []byte) error {
				var encryptionErr error
				stored, storedEvidence, encryptionErr = backupartifact.EncryptSource(ctx, capture.Encryption, identity,
					stage, source, archiveEvidence.Source, stored)
				return encryptionErr
			},
		)
		if err != nil {
			return err
		}
	} else if capture.Encryption.Kind != agentpb.BackupEncryption_BACKUP_ENCRYPTION_NONE {
		return errs.New(errs.KindStateConflict, "Volume capture evidence differs from its staged artifact")
	} else {
		stored = source
	}
	prepared := &agentpb.BackupArtifactPrepared{PointId: capture.PointId,
		Evidence: &agentpb.BackupArtifactEvidence{SourceSizeBytes: sourceEvidence.Size,
			SourceSha256: sourceEvidence.SHA256[:], StoredSizeBytes: storedEvidence.Size,
			StoredSha256: storedEvidence.SHA256[:]},
		Finals: &agentpb.BackupStagingFinals{SourceRelativeName: executionplan.BackupSourceStagingFinal,
			StoredRelativeName: storedEvidence.Name, SameInode: proto.Bool(stored == source)},
		Archive: &agentpb.BackupArtifactPrepared_Volume{
			Volume: backupvolumetransfer.ArchiveEvidenceToWire(archiveEvidence.Archive),
		},
	}
	if resume.PreparedArtifact != nil && !proto.Equal(resume.PreparedArtifact, prepared) {
		return errs.New(errs.KindStateConflict, "Volume capture evidence differs from its staged artifact")
	}

	if _, err := uploadCapture(ctx, assignment, ordinal, prepared, stored, delivery, exchange); err != nil {
		return err
	}
	if err := staging.Cleanup(ctx, captured); err != nil {
		return err
	}
	return publisher.SourceCleanupCompleted(ctx, &agentpb.BackupSourceCleanupCompleted{PointId: capture.PointId, Evidence: proto.CloneOf(prepared.Evidence)})
}

type volumeArtifactWriter struct {
	ctx      context.Context
	artifact *backupstage.Artifact
}

func (writer *volumeArtifactWriter) Write(content []byte) (int, error) {
	return writer.artifact.Write(writer.ctx, content)
}
