package agent

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/backupartifact"
	"github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/backupformat"
	"github.com/AlanD20/groundplane/internal/common/backuppostgres"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/internal/infra/agentpostgresjournal"
	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"github.com/AlanD20/groundplane/internal/infra/docker/postgres16execution"
	"github.com/AlanD20/groundplane/internal/infra/s3compatible"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (pool *WorkerPool) executeBackupPostgresCapture(ctx context.Context, assignment taskassignment.Assignment,
	execution *agentpb.ExecutionStep,
) (resultErr error) {
	step := execution.GetBackupStep()
	capture := step.GetCapture()
	if capture.GetPostgres() == nil || assignment.BackupAuthority == nil || assignment.BackupResume == nil ||
		pool.backupStaging == nil {
		return errs.New(errs.KindInternal, "PostgreSQL capture assignment authority is incomplete")
	}
	resume := postgresCaptureResume(assignment, step)
	if resume == nil {
		return invalidAgentStaging()
	}
	ctx, cancel := context.WithDeadline(ctx, time.Unix(0, int64(step.StepDeadlineUnixNano)))
	defer cancel()
	if err := agentpostgresjournal.Mark(ctx, databaseExecutionIDs(assignment.TaskID, step)); err != nil {
		return err
	}
	dumpStart := resume.DumpStart
	if resume.GetSourceCleanupCompleted() != nil {
		absent, err := pool.backupStaging.volumeStageAbsent(assignment.TaskID, step)
		if err != nil || !absent {
			return invalidAgentStaging()
		}
		return pool.retirePostgresExecution(ctx, assignment, step, dumpStart, nil)
	}
	authority, err := postgresAuthority(assignment, step)
	if err != nil {
		return err
	}
	publisher := &backupStepCheckpoint{pool: pool, taskID: assignment.TaskID, assignID: assignment.AssignmentID,
		step: step, sequence: resume.CheckpointSequence, fence: proto.CloneOf(resume.PrecedingCheckpoint)}
	if resume.Phase == agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_POINT_COMMIT {
		absent, err := pool.backupStaging.volumeStageAbsent(assignment.TaskID, step)
		if err != nil {
			return err
		}
		if absent {
			if err := publisher.publish(ctx, &agentpb.BackupCheckpointRequest{
				Checkpoint: &agentpb.BackupCheckpointRequest_SourceCleanupCompleted{
					SourceCleanupCompleted: &agentpb.BackupSourceCleanupCompleted{
						PointId: capture.PointId, Evidence: proto.CloneOf(resume.GetUploadVerified().GetEvidence()),
					}},
			}); err != nil {
				return err
			}
			return pool.retirePostgresExecution(ctx, assignment, step, dumpStart, nil)
		}
	}
	stage, source, sourceEvidence, stored, err := pool.backupStaging.databaseCaptureStage(ctx, assignment.TaskID, step,
		dumpStart != nil || resume.PreparedArtifact != nil)
	if err != nil {
		return err
	}
	if sourceEvidence.Name == "" && dumpStart != nil {
		return errs.New(errs.KindStateConflict, "PostgreSQL dump started without a complete retained source")
	}
	if sourceEvidence.Name == "" && resume.PreparedArtifact != nil {
		return invalidAgentStaging()
	}
	var archive backuppostgres.ArchiveEvidence
	if resume.PreparedArtifact != nil {
		archive, err = backuppostgres.FromWire(resume.PreparedArtifact.GetPostgres())
		if err != nil {
			return err
		}
	}
	if sourceEvidence.Name == "" {
		recorder := &postgresStartRecorder{publisher: publisher, authority: authority,
			pointID: capture.PointId, resume: &agentpb.BackupStepResume{Operation: &agentpb.BackupStepResume_Capture{Capture: resume}}}
		executor, err := postgres16execution.New(authority.index, nativePostgresArchitecture(), recorder)
		if err != nil {
			return err
		}
		defer func() {
			if closeErr := executor.Close(); closeErr != nil {
				resultErr = errs.WrapJoined(errs.KindInternal, resultErr, closeErr)
			}
		}()
		container, err := executor.ResolveContainer(ctx, authority.selection)
		if err != nil {
			return err
		}
		if resume.CheckpointSequence == 0 || resume.GetPostgresContainerObserved() != nil {
			if err := publishPostgresContainer(ctx, publisher, resume.GetPostgresContainerObserved(), authority,
				container); err != nil {
				return err
			}
		}
		archive, err = observePostgresArchive(ctx, executor, container, step, authority.database)
		if err != nil {
			return err
		}
		recorder.archive = archive
		request, err := newPostgresRequest(postgres16protocol.OperationDump, step,
			authority.database, authority.role, 0, postgres16protocol.Digest{})
		if err != nil {
			return err
		}
		writer := &postgresArtifactWriter{ctx: ctx, artifact: source, limit: authority.maxPlaintext}
		result, err := executor.Execute(ctx, container, request, nil, writer)
		if err != nil {
			return err
		}
		dumpStart = recorder.dumpStart
		sourceEvidence, err = source.Publish(ctx)
		if err != nil || sourceEvidence.Size == 0 || sourceEvidence.Size > authority.maxPlaintext ||
			sourceEvidence.Size != result.Stdout.Bytes || sourceEvidence.SHA256 != result.Stdout.SHA256 {
			return invalidAgentStaging()
		}
		if err := postgresListArchive(ctx, executor, container, step, source, sourceEvidence); err != nil {
			return err
		}
	} else {
		prepared := resume.PreparedArtifact
		if resume.Phase == agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_CAPTURING &&
			dumpStart == nil ||
			prepared != nil && (prepared.GetPostgres() == nil || prepared.Evidence == nil ||
				prepared.Evidence.SourceSizeBytes != sourceEvidence.Size ||
				!bytes.Equal(prepared.Evidence.SourceSha256, sourceEvidence.SHA256[:])) {
			return invalidAgentStaging()
		}
		if dumpStart != nil && prepared == nil {
			archive, err = backuppostgres.FromWire(dumpStart.Archive)
			if err != nil {
				return err
			}
			original, containerID, execID, err := postgresOriginalRequest(step, dumpStart, nil)
			if err != nil {
				return err
			}
			executor, err := postgres16execution.New(authority.index, nativePostgresArchitecture(), nil)
			if err != nil {
				return err
			}
			defer func() {
				if closeErr := executor.Close(); closeErr != nil {
					resultErr = errs.WrapJoined(errs.KindInternal, resultErr, closeErr)
				}
			}()
			container, err := executor.ResolveContainer(ctx, authority.selection)
			if err != nil || container.ID != containerID {
				return errs.New(errs.KindStateConflict, "PostgreSQL dump container identity changed")
			}
			evidence, err := executor.RecoverExecution(ctx, container, original, execID)
			if err != nil {
				return err
			}
			output := evidence.State.IOEvidence.Stdout
			if !output.EOF || sourceEvidence.Size == 0 || sourceEvidence.Size != output.Bytes ||
				postgres16protocol.Digest(sourceEvidence.SHA256) != output.SHA256 {
				return errs.New(errs.KindStateConflict, "PostgreSQL retained dump does not match original execution")
			}
			sourceEvidence, err = source.Publish(ctx)
			if err != nil {
				return err
			}
			if err := postgresListArchive(ctx, executor, container, step, source, sourceEvidence); err != nil {
				return err
			}
		}
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
					stage, source, backupformat.Evidence{SizeBytes: sourceEvidence.Size,
						SHA256: sourceEvidence.SHA256}, stored)
				return encryptionErr
			},
		)
		if err != nil {
			return err
		}
	} else if capture.Encryption.Kind == agentpb.BackupEncryption_BACKUP_ENCRYPTION_NONE {
		stored = source
	} else {
		return invalidAgentStaging()
	}
	wireArchive, err := archive.Wire()
	if err != nil {
		return err
	}
	prepared := &agentpb.BackupArtifactPrepared{PointId: capture.PointId,
		Evidence: &agentpb.BackupArtifactEvidence{SourceSizeBytes: sourceEvidence.Size,
			SourceSha256: sourceEvidence.SHA256[:], StoredSizeBytes: storedEvidence.Size,
			StoredSha256: storedEvidence.SHA256[:]},
		Finals: &agentpb.BackupStagingFinals{SourceRelativeName: executionplan.BackupSourceStagingFinal,
			StoredRelativeName: storedEvidence.Name, SameInode: proto.Bool(stored == source)},
		Archive: &agentpb.BackupArtifactPrepared_Postgres{Postgres: wireArchive},
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
	pool.backupStaging.retireDatabaseStage(assignment.TaskID, step, stage)
	if err := publisher.publish(ctx, &agentpb.BackupCheckpointRequest{
		Checkpoint: &agentpb.BackupCheckpointRequest_SourceCleanupCompleted{
			SourceCleanupCompleted: &agentpb.BackupSourceCleanupCompleted{
				PointId: capture.PointId, Evidence: proto.CloneOf(prepared.Evidence)}},
	}); err != nil {
		return err
	}
	return pool.retirePostgresExecution(ctx, assignment, step, dumpStart, nil)
}

func postgresCaptureResume(assignment taskassignment.Assignment,
	step *agentpb.BackupStepAuthority,
) *agentpb.BackupCaptureResume {
	for _, candidate := range assignment.BackupResume.Steps {
		if candidate.StepId == step.StepId && candidate.ExecutionId == step.ExecutionId {
			return candidate.GetCapture()
		}
	}
	return nil
}

type postgresArtifactWriter struct {
	ctx      context.Context
	artifact *backupstage.Artifact
	limit    uint64
	written  uint64
}

func (writer *postgresArtifactWriter) Write(content []byte) (int, error) {
	if uint64(len(content)) > writer.limit-writer.written {
		return 0, invalidAgentStaging()
	}
	n, err := writer.artifact.Write(writer.ctx, content)
	writer.written += uint64(n)
	return n, err
}

func (*postgresArtifactWriter) Close() error { return nil }

func newPostgresRequest(operation postgres16protocol.Operation, step *agentpb.BackupStepAuthority,
	database, role string, sourceSize uint64, sourceSHA postgres16protocol.Digest,
) (postgres16protocol.Request, error) {
	nonce, err := newPostgresNonce()
	if err != nil {
		return postgres16protocol.Request{}, err
	}
	request := postgres16protocol.Request{Operation: operation, Nonce: nonce,
		DeadlineUnixNano: step.StepDeadlineUnixNano, Database: database, Role: role,
		SourceSize: sourceSize, SourceSHA256: sourceSHA}
	if err := request.Validate(); err != nil {
		return postgres16protocol.Request{}, err
	}
	return request, nil
}

func postgresListArchive(ctx context.Context, executor *postgres16execution.Executor,
	container postgres16execution.Container, step *agentpb.BackupStepAuthority,
	source *backupstage.Artifact, evidence backupstage.ArtifactEvidence,
) error {
	reader, err := source.OpenPrefix(ctx)
	if err != nil {
		return err
	}
	defer reader.Close()
	request, err := newPostgresRequest(postgres16protocol.OperationRestoreList, step,
		"", "", evidence.Size, postgres16protocol.Digest(evidence.SHA256))
	if err != nil {
		return err
	}
	result, err := executor.Execute(ctx, container, request, reader, nil)
	if err != nil || !result.Stdin.EOF || result.Stdin.Bytes != evidence.Size ||
		result.Stdin.SHA256 != evidence.SHA256 {
		return errs.New(errs.KindStateConflict, "PostgreSQL archive list proof is unavailable")
	}
	return nil
}
