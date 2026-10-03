package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"hash"
	"io"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/backupartifact"
	"github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/backupformat"
	"github.com/AlanD20/groundplane/internal/common/backupvolume"
	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	ageinfra "github.com/AlanD20/groundplane/internal/infra/age"
	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"github.com/AlanD20/groundplane/internal/infra/backupvolumefs"
	"github.com/AlanD20/groundplane/internal/infra/s3compatible"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (pool *WorkerPool) executeBackupVolumeCapture(ctx context.Context, assignment taskassignment.Assignment,
	execution *agentpb.ExecutionStep,
) (resultErr error) {
	step := execution.GetBackupStep()
	capture := step.GetCapture()
	if capture.GetVolume() == nil || assignment.BackupAuthority == nil || assignment.BackupResume == nil ||
		pool.backupStaging == nil {
		return errs.New(errs.KindInternal, "Volume capture assignment authority is incomplete")
	}
	var resume *agentpb.BackupCaptureResume
	for _, candidate := range assignment.BackupResume.Steps {
		if candidate.StepId == step.StepId && candidate.ExecutionId == step.ExecutionId {
			resume = candidate.GetCapture()
			break
		}
	}
	if resume == nil {
		return invalidAgentStaging()
	}
	ctx, cancel := context.WithDeadline(ctx, time.Unix(0, int64(step.StepDeadlineUnixNano)))
	defer cancel()
	if resume.GetSourceCleanupCompleted() != nil {
		absent, err := pool.backupStaging.volumeStageAbsent(assignment.TaskID, step)
		if err != nil || !absent {
			return invalidAgentStaging()
		}
		return nil
	}
	publisher := &backupStepCheckpoint{pool: pool, taskID: assignment.TaskID, assignID: assignment.AssignmentID,
		step: step, sequence: resume.CheckpointSequence, fence: proto.CloneOf(resume.PrecedingCheckpoint)}
	if resume.Phase == agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_POINT_COMMIT {
		absent, err := pool.backupStaging.volumeStageAbsent(assignment.TaskID, step)
		if err != nil {
			return err
		}
		if absent {
			return publisher.publish(ctx, &agentpb.BackupCheckpointRequest{
				Checkpoint: &agentpb.BackupCheckpointRequest_SourceCleanupCompleted{
					SourceCleanupCompleted: &agentpb.BackupSourceCleanupCompleted{
						PointId: capture.PointId, Evidence: proto.CloneOf(resume.GetUploadVerified().GetEvidence()),
					},
				},
			})
		}
	}
	stage, source, sourceEvidence, stored, err := pool.backupStaging.volumeCaptureStage(ctx, assignment.TaskID, step)
	if err != nil {
		return err
	}
	volumeAuthority := capture.GetVolume()
	var archiveEvidence backupvolume.ArtifactEvidence
	var entries []backupvolume.Entry
	if sourceEvidence.Name == "" {
		volume, err := backupvolumefs.Open(ctx, pool.volumeRoot,
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
			return invalidAgentStaging()
		}
	} else {
		prepared := resume.PreparedArtifact
		if prepared.GetVolume() == nil || prepared.Evidence == nil ||
			prepared.Evidence.SourceSizeBytes != sourceEvidence.Size ||
			!bytes.Equal(prepared.Evidence.SourceSha256, sourceEvidence.SHA256[:]) {
			return invalidAgentStaging()
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
		return invalidAgentStaging()
	}
	if err := pool.sendBackupVolumeManifest(ctx, assignment, step,
		agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_CAPTURE,
		agentpb.BackupVolumeManifestRole_BACKUP_VOLUME_MANIFEST_ROLE_CAPTURED,
		entries, &archiveEvidence); err != nil {
		return err
	}
	storedEvidence := sourceEvidence
	if capture.Encryption.Kind == agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE {
		err = pool.ConsumeBackupSecretSlot(
			ctx,
			assignment.TaskID,
			assignment.AssignmentID,
			step.StepId,
			agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_CURRENT_AGE_IDENTITY,
			func(identity []byte) error {
				var encryptionErr error
				stored, storedEvidence, encryptionErr = encryptVolumeSource(ctx, capture.Encryption, identity,
					stage, source, archiveEvidence.Source, stored)
				return encryptionErr
			},
		)
		if err != nil {
			return err
		}
	} else if capture.Encryption.Kind != agentpb.BackupEncryption_BACKUP_ENCRYPTION_NONE {
		return invalidAgentStaging()
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
		return invalidAgentStaging()
	}
	uploadAuthority, err := backupartifact.NewAuthority(assignment.BackupAuthority, step, capture)
	if err != nil {
		return err
	}
	err = pool.ConsumeBackupSecretSlot(ctx, assignment.TaskID, assignment.AssignmentID, step.StepId,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY, func(access []byte) error {
			return pool.ConsumeBackupSecretSlot(ctx, assignment.TaskID, assignment.AssignmentID, step.StepId,
				agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY, func(secret []byte) error {
					connector := capture.Target.Connector
					store, err := s3compatible.New(s3compatible.Config{Endpoint: connector.CanonicalEndpointUrl,
						Bucket: capture.Target.Bucket, Prefix: connector.Prefix, Region: connector.Region,
						PathStyle: connector.GetPathStyle(), AccessKey: string(access), SecretKey: string(secret)})
					if err != nil {
						return err
					}
					_, err = backupartifact.Upload(ctx, backupartifact.UploadInput{Authority: uploadAuthority,
						Prepared: prepared, Phase: resume.Phase, UploadCompleted: resume.GetUploadCompleted(),
						UploadVerified: resume.GetUploadVerified(), Stored: stored, Store: store, Publish: publisher.publish})
					return err
				})
		})
	if err != nil {
		return err
	}
	if err := stage.Cleanup(ctx); err != nil {
		return err
	}
	pool.backupStaging.retireVolumeStage(assignment.TaskID, step, stage)
	return publisher.publish(ctx, &agentpb.BackupCheckpointRequest{
		Checkpoint: &agentpb.BackupCheckpointRequest_SourceCleanupCompleted{
			SourceCleanupCompleted: &agentpb.BackupSourceCleanupCompleted{
				PointId: capture.PointId, Evidence: proto.CloneOf(prepared.Evidence),
			},
		},
	})
}

type volumeArtifactWriter struct {
	ctx      context.Context
	artifact *backupstage.Artifact
}

func (writer *volumeArtifactWriter) Write(content []byte) (int, error) {
	return writer.artifact.Write(writer.ctx, content)
}

func encryptVolumeSource(ctx context.Context, authority *agentpb.BackupEncryptionAuthority, identity []byte,
	stage *backupstage.Stage, source *backupstage.Artifact, sourceEvidence backupformat.Evidence,
	retained *backupstage.Artifact,
) (_ *backupstage.Artifact, _ backupstage.ArtifactEvidence, resultErr error) {
	recipient, err := ageinfra.IdentityRecipient(identity, authority.RecipientSha256)
	if err != nil {
		return nil, backupstage.ArtifactEvidence{}, err
	}
	if retained != nil {
		evidence, err := retained.Publish(ctx)
		if err != nil {
			return nil, backupstage.ArtifactEvidence{}, err
		}
		expected, err := backupformat.AgeStoredSize(sourceEvidence.SizeBytes)
		if err != nil || evidence.Size != expected {
			return nil, backupstage.ArtifactEvidence{}, invalidAgentStaging()
		}
		reader, err := retained.Open(ctx)
		if err != nil {
			return nil, backupstage.ArtifactEvidence{}, err
		}
		defer func() {
			if closeErr := reader.Close(); closeErr != nil {
				resultErr = errs.WrapJoined(errs.KindStorageUnavailable, resultErr, closeErr)
			}
		}()
		proof := &volumeSourceProof{hash: sha256.New()}
		if err := ageinfra.DecryptStream(ctx, string(identity), reader, proof,
			sourceEvidence.SizeBytes, func() { _ = reader.Close() }); err != nil {
			return nil, backupstage.ArtifactEvidence{}, err
		}
		if proof.size != sourceEvidence.SizeBytes || proof.sum() != sourceEvidence.SHA256 {
			return nil, backupstage.ArtifactEvidence{}, invalidAgentStaging()
		}
		return retained, evidence, nil
	}
	stored, err := stage.CreateFile(ctx, executionplan.BackupStoredStagingFinal)
	if err != nil {
		return nil, backupstage.ArtifactEvidence{}, err
	}
	reader, err := source.Open(ctx)
	if err != nil {
		return nil, backupstage.ArtifactEvidence{}, err
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			resultErr = errs.WrapJoined(errs.KindStorageUnavailable, resultErr, closeErr)
		}
	}()
	if err := ageinfra.EncryptStream(ctx, recipient, reader,
		&volumeArtifactWriter{ctx: ctx, artifact: stored}, func() { _ = reader.Close() }); err != nil {
		return nil, backupstage.ArtifactEvidence{}, err
	}
	evidence, err := stored.Publish(ctx)
	if err != nil {
		return nil, backupstage.ArtifactEvidence{}, err
	}
	expected, err := backupformat.AgeStoredSize(sourceEvidence.SizeBytes)
	if err != nil || evidence.Size != expected {
		return nil, backupstage.ArtifactEvidence{}, invalidAgentStaging()
	}
	return stored, evidence, nil
}

var _ io.Writer = (*volumeArtifactWriter)(nil)

type volumeSourceProof struct {
	hash hash.Hash
	size uint64
}

func (proof *volumeSourceProof) Write(content []byte) (int, error) {
	count, err := proof.hash.Write(content)
	proof.size += uint64(count)
	return count, err
}

func (proof *volumeSourceProof) sum() [sha256.Size]byte {
	var digest [sha256.Size]byte
	copy(digest[:], proof.hash.Sum(nil))
	return digest
}
