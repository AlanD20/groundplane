package backupruntime

import (
	"bytes"
	"context"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/backupartifact"
	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/common/backupformat"
	"github.com/AlanD20/groundplane/internal/common/backupmysql"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/mysql84protocol"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentpostgresjournal"
	"github.com/AlanD20/groundplane/internal/infra/docker/mysql84execution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func captureMySQL(ctx context.Context, assignment *agentpb.BackupWorkAssignment,
	identity workexecution.Identity, ordinal uint32, staging *Staging,
	delivery *checkpointdelivery.Backup, exchange Exchange,
) (resultErr error) {
	authority, err := mysqlAuthority(assignment, identity, ordinal)
	if err != nil {
		return err
	}
	step := assignment.Input.Steps[ordinal]
	capture := step.GetCapture()
	if capture.GetMysql() == nil {
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
	dumpStart := resume.MysqlDumpStart
	if resume.GetSourceCleanupCompleted() != nil {
		absent, err := staging.SourceAbsent(assignment, identity, ordinal)
		if err != nil || !absent {
			return invalidDatabaseStaging()
		}
		return retireMySQLExecution(ctx, assignment, identity, ordinal, publisher, dumpStart, nil)
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
			return retireMySQLExecution(ctx, assignment, identity, ordinal, publisher, dumpStart, nil)
		}
	}
	captured, err := staging.DatabaseCapture(ctx, assignment, identity, ordinal, resume)
	if err != nil {
		return err
	}
	stage, source, sourceEvidence, stored := captured.Stage, captured.Source, captured.SourceEvidence, captured.Stored
	if sourceEvidence.Name == "" && dumpStart != nil {
		return errs.New(errs.KindStateConflict, "MySQL dump started without a complete retained source")
	}
	if sourceEvidence.Name == "" && resume.PreparedArtifact != nil {
		return invalidDatabaseStaging()
	}
	var archive backupmysql.ArchiveEvidence
	if dumpStart != nil {
		archive, err = backupmysql.FromWire(dumpStart.Archive)
		if err != nil {
			return err
		}
	}
	if prepared := resume.PreparedArtifact; prepared != nil {
		preparedArchive, archiveErr := backupmysql.FromWire(prepared.GetMysql())
		if archiveErr != nil {
			return archiveErr
		}
		if dumpStart != nil && preparedArchive != archive {
			return invalidDatabaseStaging()
		}
		archive = preparedArchive
	}
	if sourceEvidence.Name == "" {
		recorder := &mysqlStartRecorder{
			publisher: publisher,
			authority: authority,
			step:      step,
			pointID:   capture.PointId,
			resume:    &agentpb.BackupStepResume{Operation: &agentpb.BackupStepResume_Capture{Capture: resume}},
		}
		executor, err := mysql84execution.New(authority.selection, recorder)
		if err != nil {
			return err
		}
		defer func() {
			if closeErr := executor.Close(); closeErr != nil {
				resultErr = errs.WrapJoined(errs.KindInternal, resultErr, closeErr)
			}
		}()
		container, err := executor.ResolveContainer(ctx)
		if err != nil {
			return err
		}
		if resume.CheckpointSequence == 0 || resume.GetMysqlContainerObserved() != nil {
			if err := publishMySQLContainer(ctx, publisher, resume.GetMysqlContainerObserved(), authority, container); err != nil {
				return err
			}
		}
		archive, err = observeMySQLArchive(ctx, executor, container, step)
		if err != nil {
			return err
		}
		wireArchive, err := archive.Wire()
		if err != nil {
			return err
		}
		recorder.archive = wireArchive
		request, err := newMySQLRequest(mysql84protocol.OperationDump, step, authority.database,
			authority.role, authority.maxPlaintext, 0, mysql84protocol.Digest{})
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
	} else {
		prepared := resume.PreparedArtifact
		if resume.Phase == agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_CAPTURING && dumpStart == nil ||
			prepared != nil && (prepared.GetMysql() == nil || prepared.Evidence == nil ||
				prepared.Evidence.SourceSizeBytes != sourceEvidence.Size ||
				!bytes.Equal(prepared.Evidence.SourceSha256, sourceEvidence.SHA256[:])) {
			return invalidDatabaseStaging()
		}
		if dumpStart != nil && prepared == nil {
			original, containerID, execID, err := mysqlOriginalRequest(step, dumpStart, nil)
			if err != nil {
				return err
			}
			executor, err := mysql84execution.New(authority.selection, nil)
			if err != nil {
				return err
			}
			defer func() {
				if closeErr := executor.Close(); closeErr != nil {
					resultErr = errs.WrapJoined(errs.KindInternal, resultErr, closeErr)
				}
			}()
			container, err := executor.ResolveContainer(ctx)
			if err != nil || container.ID != containerID {
				return errs.New(errs.KindStateConflict, "MySQL dump container identity changed")
			}
			prefix, err := source.OpenPrefix(ctx)
			if err != nil {
				return err
			}
			writer := &databaseDumpRecoveryWriter{prefix: prefix, remainingPrefix: sourceEvidence.Size,
				output: &postgresArtifactWriter{ctx: ctx, artifact: source, limit: authority.maxPlaintext - sourceEvidence.Size},
				limit:  authority.maxPlaintext}
			result, recoveryErr := executor.RecoverExecution(ctx, container, original, execID, writer)
			closeErr := prefix.Close()
			if recoveryErr != nil || closeErr != nil || !result.Stdout.EOF || writer.remainingPrefix != 0 {
				return errs.WrapJoined(errs.KindStateConflict,
					errs.New(errs.KindStateConflict, "original MySQL dump recovery is incomplete"), recoveryErr, closeErr)
			}
			sourceEvidence, err = source.Publish(ctx)
			if err != nil {
				return err
			}
			if sourceEvidence.Size != result.Stdout.Bytes || mysql84protocol.Digest(sourceEvidence.SHA256) != result.Stdout.SHA256 {
				return errs.New(errs.KindStateConflict, "MySQL retained dump does not match original execution")
			}
		}
	}
	if err := exchange.RequestBackupSecrets(ctx, assignment.Attempt, ordinal); err != nil {
		return err
	}
	storedEvidence := sourceEvidence
	if capture.Encryption.Kind == agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE {
		err = exchange.ConsumeBackupSecret(ctx, assignment.Attempt, ordinal,
			agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_CURRENT_AGE_IDENTITY,
			func(identity []byte) error {
				var encryptionErr error
				stored, storedEvidence, encryptionErr = backupartifact.EncryptSource(
					ctx,
					capture.Encryption,
					identity,
					stage,
					source,
					backupformat.Evidence{SizeBytes: sourceEvidence.Size, SHA256: sourceEvidence.SHA256},
					stored,
				)
				return encryptionErr
			})
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
			StoredSha256: storedEvidence.SHA256[:]}, Finals: &agentpb.BackupStagingFinals{
			SourceRelativeName: executionplan.BackupSourceStagingFinal, StoredRelativeName: storedEvidence.Name,
			SameInode: proto.Bool(
				stored == source,
			)}, Archive: &agentpb.BackupArtifactPrepared_Mysql{Mysql: wireArchive}}
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
	return retireMySQLExecution(ctx, assignment, identity, ordinal, publisher, dumpStart, nil)
}

func observeMySQLArchive(ctx context.Context, executor *mysql84execution.Executor,
	container mysql84execution.Container, step *agentpb.BackupStepAuthority,
) (backupmysql.ArchiveEvidence, error) {
	authority := step.GetCapture().GetMysql()
	if authority == nil {
		return backupmysql.ArchiveEvidence{}, invalidDatabaseStaging()
	}
	for _, operation := range []mysql84protocol.Operation{
		mysql84protocol.OperationProbeMySQL, mysql84protocol.OperationProbeMySQLDump,
	} {
		request, err := newMySQLRequest(operation, step, "", "", 0, 0, mysql84protocol.Digest{})
		if err != nil {
			return backupmysql.ArchiveEvidence{}, err
		}
		if _, err := executor.Execute(ctx, container, request, nil, nil); err != nil {
			return backupmysql.ArchiveEvidence{}, err
		}
	}
	serverRequest, err := newMySQLRequest(mysql84protocol.OperationServerVersion, step,
		authority.DatabaseName, "", 0, 0, mysql84protocol.Digest{})
	if err != nil {
		return backupmysql.ArchiveEvidence{}, err
	}
	server, err := executor.Execute(ctx, container, serverRequest, nil, nil)
	if err != nil {
		return backupmysql.ArchiveEvidence{}, err
	}
	toolRequest, err := newMySQLRequest(mysql84protocol.OperationToolVersion, step,
		"", "", 0, 0, mysql84protocol.Digest{})
	if err != nil {
		return backupmysql.ArchiveEvidence{}, err
	}
	tool, err := executor.Execute(ctx, container, toolRequest, nil, nil)
	if err != nil {
		return backupmysql.ArchiveEvidence{}, err
	}
	evidence := backupmysql.ArchiveEvidence{SourceServerVersion: strings.TrimSpace(string(server.Proof)),
		BackupToolVersion: strings.TrimSpace(string(tool.Proof)), ArtifactFormat: mysql84protocol.ArtifactFormat,
		AdapterContractVersion: mysql84protocol.AdapterContractVersion}
	return evidence, evidence.Validate()
}
