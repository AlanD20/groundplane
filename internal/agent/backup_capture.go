package agent

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/backupartifact"
	"github.com/AlanD20/groundplane/internal/agent/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/agentconfigjournal"
	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"github.com/AlanD20/groundplane/internal/infra/s3compatible"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (pool *WorkerPool) executeBackupConfigCapture(ctx context.Context, assignment taskassignment.Assignment,
	execution *agentpb.ExecutionStep,
) (resultErr error) {
	step := execution.GetBackupStep()
	capture := step.GetCapture()
	if capture.GetConfig() == nil || assignment.BackupAuthority == nil || assignment.BackupResume == nil {
		return errs.New(errs.KindInternal, "Config capture assignment authority is incomplete")
	}
	var resume *agentpb.BackupCaptureResume
	for _, candidate := range assignment.BackupResume.Steps {
		if candidate.StepId == step.StepId && candidate.ExecutionId == step.ExecutionId {
			resume = candidate.GetCapture()
			break
		}
	}
	if resume == nil {
		return errs.New(errs.KindInternal, "Config capture resume is missing")
	}
	ctx, cancel := context.WithDeadline(ctx, time.Unix(0, int64(step.StepDeadlineUnixNano)))
	defer cancel()
	if resume.GetSourceCleanupCompleted() != nil {
		binding := backupconfigtransfer.Binding{TaskID: assignment.TaskID, AssignmentID: assignment.AssignmentID,
			StepID: step.StepId, ExecutionID: step.ExecutionId, TransferID: step.ExecutionId,
			Direction: agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_CAPTURE}
		return agentconfigjournal.ResumeCleanup(ctx, agentprotocol.StatePath+"/config-transfers/"+binding.TransferID,
			binding, step, &agentpb.BackupStepResume{StepId: step.StepId, ExecutionId: step.ExecutionId,
				Operation: &agentpb.BackupStepResume_Capture{Capture: resume}})
	}
	publisher := &backupStepCheckpoint{pool: pool, taskID: assignment.TaskID, assignID: assignment.AssignmentID,
		step: step, sequence: resume.CheckpointSequence, fence: proto.CloneOf(resume.PrecedingCheckpoint)}
	if resume.Phase == agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_POINT_COMMIT {
		absent, err := pool.backupStaging.configStageAbsent(assignment.TaskID, step)
		if err != nil {
			return err
		}
		if absent {
			return pool.completeAbsentConfigStage(ctx, assignment, step, resume, publisher)
		}
	}
	stage, source, retained, stored, err := pool.backupStaging.configCaptureStage(ctx, assignment.TaskID, step)
	if err != nil {
		return err
	}
	stream, err := pool.backupConfigs.Stream(
		assignment.TaskID,
		assignment.AssignmentID,
		step.StepId,
		pool.publishBackupConfigCredit,
	)
	if err != nil {
		return err
	}
	binding := stream.Binding()
	journal, err := agentconfigjournal.Open(ctx, agentprotocol.StatePath+"/config-transfers/"+binding.TransferID,
		binding, step)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := journal.Close(); closeErr != nil {
			resultErr = errs.WrapJoined(errs.KindStorageUnavailable, resultErr, closeErr)
		}
	}()
	progress, err := backupconfiguration.Capture(
		ctx,
		binding,
		capture.GetConfig().Content,
		source,
		retained,
		journal,
		stream,
	)
	if err != nil {
		return err
	}
	if resume.Phase == agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_CAPTURING &&
		resume.GetConfigProgress().GetTransfer() == nil {
		completed := configCaptureCompleted(capture, progress)
		if err := publisher.publish(ctx, &agentpb.BackupCheckpointRequest{Checkpoint: &agentpb.BackupCheckpointRequest_Config{
			Config: &agentpb.BackupConfigCheckpoint{Checkpoint: &agentpb.BackupConfigCheckpoint_TransferCompleted{
				TransferCompleted: completed,
			}},
		}}); err != nil {
			return err
		}
	}
	var storedEvidence backupstage.ArtifactEvidence
	err = pool.ConsumeBackupSecretSlot(ctx, assignment.TaskID, assignment.AssignmentID, step.StepId,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_CURRENT_AGE_IDENTITY, func(identity []byte) error {
			var encryptionErr error
			stored, storedEvidence, encryptionErr = backupconfiguration.Encrypt(ctx, capture.Encryption, identity,
				stage, source, progress.Source, stored)
			return encryptionErr
		})
	if err != nil {
		return err
	}
	prepared := configCapturePrepared(capture, progress, storedEvidence)
	if resume.PreparedArtifact != nil && !proto.Equal(resume.PreparedArtifact, prepared) {
		return invalidAgentStaging()
	}
	uploadAuthority, err := backupartifact.NewAuthority(assignment.BackupAuthority, step, capture)
	if err != nil {
		return err
	}
	if err := pool.uploadConfigCapture(ctx, assignment, step, resume, publisher, stage, stored, prepared, uploadAuthority); err != nil {
		return err
	}
	completion := &agentpb.BackupCaptureResume{
		PreparedArtifact:    proto.CloneOf(prepared),
		CheckpointSequence:  publisher.sequence,
		PrecedingCheckpoint: proto.CloneOf(publisher.fence),
		Phase:               agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_SERVICE_RECOVERY,
		Cursor: &agentpb.BackupCaptureCursor{ObjectAttempt: 1, ConfigRecordSequence: progress.Cursor.RecordSequence,
			ConfigValueOrdinal: progress.Cursor.NextOrdinal, CumulativeChainSha256: append([]byte(nil), progress.Cursor.ChainSHA256[:]...)},
		Checkpoint: &agentpb.BackupCaptureResume_SourceCleanupCompleted{
			SourceCleanupCompleted: proto.CloneOf(publisher.lastRequest.GetSourceCleanupCompleted()),
		},
	}
	return journal.Cleanup(ctx, &agentpb.BackupStepResume{StepId: step.StepId, ExecutionId: step.ExecutionId,
		Operation: &agentpb.BackupStepResume_Capture{Capture: completion}})
}

func (pool *WorkerPool) uploadConfigCapture(ctx context.Context, assignment taskassignment.Assignment,
	step *agentpb.BackupStepAuthority, resume *agentpb.BackupCaptureResume, publisher *backupStepCheckpoint,
	stage *backupstage.Stage, stored *backupstage.Artifact, prepared *agentpb.BackupArtifactPrepared,
	authority *backupartifact.Authority,
) error {
	target := step.GetCapture().Target
	return pool.ConsumeBackupSecretSlot(ctx, assignment.TaskID, assignment.AssignmentID, step.StepId,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY, func(access []byte) error {
			return pool.ConsumeBackupSecretSlot(ctx, assignment.TaskID, assignment.AssignmentID, step.StepId,
				agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY, func(secret []byte) error {
					connector := target.Connector
					store, err := s3compatible.New(s3compatible.Config{Endpoint: connector.CanonicalEndpointUrl,
						Bucket: target.Bucket, Prefix: connector.Prefix, Region: connector.Region,
						PathStyle: connector.GetPathStyle(), AccessKey: string(access), SecretKey: string(secret)})
					if err != nil {
						return err
					}
					_, err = backupartifact.Upload(
						ctx,
						backupartifact.UploadInput{Authority: authority, Prepared: prepared,
							Phase: resume.Phase, UploadCompleted: resume.GetUploadCompleted(), UploadVerified: resume.GetUploadVerified(),
							Stored: stored, Store: store, Publish: publisher.publish},
					)
					if err != nil {
						return err
					}
					if err := stage.Cleanup(ctx); err != nil {
						return err
					}
					pool.backupStaging.retireConfigStage(assignment.TaskID, step, stage)
					return publisher.publish(ctx, &agentpb.BackupCheckpointRequest{
						Checkpoint: &agentpb.BackupCheckpointRequest_SourceCleanupCompleted{
							SourceCleanupCompleted: &agentpb.BackupSourceCleanupCompleted{
								PointId: prepared.PointId, Evidence: proto.CloneOf(prepared.Evidence),
							},
						},
					})
				})
		})
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
