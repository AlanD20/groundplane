package backupruntime

import (
	"context"
	"github.com/AlanD20/groundplane/internal/agent/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentconfigjournal"
	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"time"
)

func captureConfig(ctx context.Context, assignment *agentpb.BackupWorkAssignment,
	identity workexecution.Identity, ordinal uint32, staging *Staging,
	delivery *checkpointdelivery.Backup, exchange Exchange,
) (resultErr error) {
	binding, err := backupconfigtransfer.Bind(assignment, identity, ordinal, agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_CAPTURE)
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
		return errs.New(errs.KindStateConflict, "Config capture cursor is missing")
	}
	ctx, cancel := context.WithDeadline(ctx, time.Unix(0, int64(step.StepDeadlineUnixNano)))
	defer cancel()
	journalPath := agentprotocol.StatePath + "/config-transfers/" + binding.TransferID
	absent, err := staging.SourceAbsent(assignment, identity, ordinal)
	if err != nil {
		return err
	}
	if resume.GetSourceCleanupCompleted() != nil {
		if !absent {
			return errs.New(errs.KindStateConflict, "Completed Config capture still has staged files")
		}
		return agentconfigjournal.ResumeCleanup(ctx, journalPath, binding, step, cursor)
	}
	if resume.Phase == agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_POINT_COMMIT && absent {
		verified := resume.GetUploadVerified()
		if verified == nil {
			return errs.New(errs.KindStateConflict, "Config cleanup lacks its verified upload")
		}
		if err := publisher.SourceCleanupCompleted(ctx, &agentpb.BackupSourceCleanupCompleted{
			PointId: verified.PointId, Evidence: proto.CloneOf(verified.Evidence),
		}); err != nil {
			return err
		}
		completion, err := publisher.Current(ctx)
		if err != nil {
			return err
		}
		return agentconfigjournal.ResumeCleanup(ctx, journalPath, binding, step, completion)
	}
	captured, err := staging.ConfigCapture(ctx, assignment, identity, ordinal, resume)
	if err != nil {
		return err
	}
	stream, err := exchange.CaptureConfigStream(binding)
	if err != nil {
		return err
	}
	journal, err := agentconfigjournal.Open(ctx, journalPath, binding, step)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := journal.Close(); closeErr != nil {
			resultErr = errs.WrapJoined(errs.KindStorageUnavailable, resultErr, closeErr)
		}
	}()
	progress, err := backupconfiguration.Capture(ctx, binding, capture.GetConfig().Content,
		captured.Source, captured.SourceEvidence, journal, stream)
	if err != nil {
		return err
	}
	if resume.Phase == agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_CAPTURING &&
		resume.GetConfigProgress().GetTransfer() == nil {
		if err := publisher.Config(ctx, &agentpb.BackupConfigCheckpoint{
			Checkpoint: &agentpb.BackupConfigCheckpoint_TransferCompleted{TransferCompleted: configCaptureCompleted(capture, progress)},
		}); err != nil {
			return err
		}
	}
	if err := exchange.RequestBackupSecrets(ctx, assignment.Attempt, ordinal); err != nil {
		return err
	}
	stored := captured.Stored
	var storedEvidence backupstage.ArtifactEvidence
	err = exchange.ConsumeBackupSecret(ctx, assignment.Attempt, ordinal,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_CURRENT_AGE_IDENTITY,
		func(identity []byte) error {
			var encryptionErr error
			stored, storedEvidence, encryptionErr = backupconfiguration.Encrypt(ctx, capture.Encryption, identity,
				captured.Stage, captured.Source, progress.Source, stored)
			return encryptionErr
		})
	if err != nil {
		return err
	}
	prepared := configCapturePrepared(capture, progress, storedEvidence)
	if resume.PreparedArtifact != nil && !proto.Equal(resume.PreparedArtifact, prepared) {
		return errs.New(errs.KindStateConflict, "Config artifact differs from its acknowledged preparation")
	}
	if _, err := uploadCapture(ctx, assignment, ordinal, prepared, stored, delivery, exchange); err != nil {
		return err
	}
	if err := staging.Cleanup(ctx, captured); err != nil {
		return err
	}
	if err := publisher.SourceCleanupCompleted(ctx, &agentpb.BackupSourceCleanupCompleted{
		PointId: prepared.PointId, Evidence: proto.CloneOf(prepared.Evidence),
	}); err != nil {
		return err
	}
	completion, err := publisher.Current(ctx)
	if err != nil {
		return err
	}
	return journal.Cleanup(ctx, completion)
}

func configCaptureCompleted(capture *agentpb.BackupCaptureAuthority,
	progress backupconfigtransfer.ReceiverProgress,
) *agentpb.BackupConfigTransferCompleted {
	return &agentpb.BackupConfigTransferCompleted{Content: proto.CloneOf(capture.GetConfig().Content),
		CommittedRecordCount: progress.Cursor.RecordSequence, ValueChainSha256: append([]byte(nil), progress.Cursor.ChainSHA256[:]...),
		TransferTranscriptSha256: append([]byte(nil), progress.TranscriptSHA256[:]...)}
}

func configCapturePrepared(capture *agentpb.BackupCaptureAuthority, progress backupconfigtransfer.ReceiverProgress,
	stored backupstage.ArtifactEvidence,
) *agentpb.BackupArtifactPrepared {
	return &agentpb.BackupArtifactPrepared{PointId: capture.PointId,
		Evidence: &agentpb.BackupArtifactEvidence{SourceSizeBytes: progress.Source.SizeBytes,
			SourceSha256: append([]byte(nil), progress.Source.SHA256[:]...), StoredSizeBytes: stored.Size,
			StoredSha256: append([]byte(nil), stored.SHA256[:]...)},
		Finals: &agentpb.BackupStagingFinals{SourceRelativeName: executionplan.BackupSourceStagingFinal,
			StoredRelativeName: executionplan.BackupStoredStagingFinal, SameInode: proto.Bool(false)},
		Archive: &agentpb.BackupArtifactPrepared_Config{Config: &agentpb.BackupConfigArchiveEvidence{
			Content: proto.CloneOf(
				capture.GetConfig().Content,
			), CaptureTranscriptSha256: append([]byte(nil), progress.TranscriptSHA256[:]...),
		}},
	}
}
