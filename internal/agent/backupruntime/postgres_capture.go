package backupruntime

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/backupartifact"
	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/common/backupformat"
	"github.com/AlanD20/groundplane/internal/common/backuppostgres"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentpostgresjournal"
	"github.com/AlanD20/groundplane/internal/infra/docker/postgres16execution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func capturePostgres(ctx context.Context, assignment *agentpb.BackupWorkAssignment,
	identity workexecution.Identity, ordinal uint32, staging *Staging,
	delivery *checkpointdelivery.Backup, exchange Exchange,
) (resultErr error) {
	authority, err := postgresAuthority(assignment, identity, ordinal)
	if err != nil {
		return err
	}
	step := assignment.Input.Steps[ordinal]
	capture := step.GetCapture()
	if capture.GetPostgres() == nil {
		return invalidDatabaseStaging()
	}
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
		return invalidDatabaseStaging()
	}
	ctx, cancel := context.WithDeadline(ctx, time.Unix(0, int64(step.StepDeadlineUnixNano)))
	defer cancel()
	if err := agentpostgresjournal.Mark(ctx, agentpostgresjournal.IDs{Task: assignment.Attempt.TaskId, ExecutionID: step.ExecutionId, Point: capture.PointId}); err != nil {
		return err
	}
	dumpStart := resume.DumpStart
	if resume.GetSourceCleanupCompleted() != nil {
		absent, err := staging.SourceAbsent(assignment, identity, ordinal)
		if err != nil || !absent {
			return invalidDatabaseStaging()
		}
		return retirePostgresExecution(ctx, assignment, identity, ordinal, publisher, dumpStart, nil)
	}
	if resume.Phase == agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_POINT_COMMIT {
		absent, err := staging.SourceAbsent(assignment, identity, ordinal)
		if err != nil {
			return err
		}
		if absent {
			if err := publisher.SourceCleanupCompleted(ctx, &agentpb.BackupSourceCleanupCompleted{
				PointId: capture.PointId, Evidence: proto.CloneOf(resume.GetUploadVerified().GetEvidence()),
			}); err != nil {
				return err
			}
			return retirePostgresExecution(ctx, assignment, identity, ordinal, publisher, dumpStart, nil)
		}
	}
	captured, err := staging.DatabaseCapture(ctx, assignment, identity, ordinal, resume)
	if err != nil {
		return err
	}
	stage, source, sourceEvidence, stored := captured.Stage, captured.Source, captured.SourceEvidence, captured.Stored
	if sourceEvidence.Name == "" && dumpStart != nil {
		return errs.New(errs.KindStateConflict, "PostgreSQL dump started without a complete retained source")
	}
	if sourceEvidence.Name == "" && resume.PreparedArtifact != nil {
		return invalidDatabaseStaging()
	}
	var archive backuppostgres.ArchiveEvidence
	if resume.PreparedArtifact != nil {
		archive, err = backuppostgres.FromWire(resume.PreparedArtifact.GetPostgres())
		if err != nil {
			return err
		}
	}
	if sourceEvidence.Name == "" {
		recorder := &postgresStartRecorder{publisher: publisher, authority: authority, step: step,
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
			return invalidDatabaseStaging()
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
			return invalidDatabaseStaging()
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
	if err := exchange.RequestBackupSecrets(ctx, assignment.Attempt, ordinal); err != nil {
		return err
	}
	storedEvidence := sourceEvidence
	if capture.Encryption.Kind == agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE {
		err = exchange.ConsumeBackupSecret(
			ctx, assignment.Attempt, ordinal,
			agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_CURRENT_AGE_IDENTITY,
			func(identity []byte) error {
				var encryptionErr error
				stored, storedEvidence, encryptionErr = backupartifact.EncryptSource(ctx, capture.Encryption, identity,
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
		return invalidDatabaseStaging()
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
		return invalidDatabaseStaging()
	}

	if _, err := uploadCapture(ctx, assignment, ordinal, prepared, stored, delivery, exchange); err != nil {
		return err
	}
	if err := staging.Cleanup(ctx, captured); err != nil {
		return err
	}
	if err := publisher.SourceCleanupCompleted(ctx, &agentpb.BackupSourceCleanupCompleted{
		PointId: capture.PointId, Evidence: proto.CloneOf(prepared.Evidence),
	}); err != nil {
		return err
	}
	return retirePostgresExecution(ctx, assignment, identity, ordinal, publisher, dumpStart, nil)
}
