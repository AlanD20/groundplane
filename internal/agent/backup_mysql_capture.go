package agent

import (
	"bytes"
	"context"
	"io"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/backupartifact"
	"github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/backupformat"
	"github.com/AlanD20/groundplane/internal/common/backupmysql"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/mysql84protocol"
	"github.com/AlanD20/groundplane/internal/infra/agentpostgresjournal"
	"github.com/AlanD20/groundplane/internal/infra/docker/mysql84execution"
	"github.com/AlanD20/groundplane/internal/infra/s3compatible"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (pool *WorkerPool) executeBackupMySQLCapture(ctx context.Context, assignment taskassignment.Assignment,
	execution *agentpb.ExecutionStep,
) (resultErr error) {
	step := execution.GetBackupStep()
	capture := step.GetCapture()
	if capture.GetMysql() == nil || assignment.BackupAuthority == nil || assignment.BackupResume == nil ||
		pool.backupStaging == nil {
		return errs.New(errs.KindInternal, "MySQL capture assignment authority is incomplete")
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
	dumpStart := resume.MysqlDumpStart
	if resume.GetSourceCleanupCompleted() != nil {
		absent, err := pool.backupStaging.volumeStageAbsent(assignment.TaskID, step)
		if err != nil || !absent {
			return invalidAgentStaging()
		}
		return pool.retireMySQLExecution(ctx, assignment, step, dumpStart, nil)
	}
	authority, err := mysqlAuthority(assignment, step)
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
			if err := publisher.publish(ctx, &agentpb.BackupCheckpointRequest{Checkpoint: &agentpb.BackupCheckpointRequest_SourceCleanupCompleted{SourceCleanupCompleted: &agentpb.BackupSourceCleanupCompleted{PointId: capture.PointId,
				Evidence: proto.CloneOf(resume.GetUploadVerified().GetEvidence())}}}); err != nil {
				return err
			}
			return pool.retireMySQLExecution(ctx, assignment, step, dumpStart, nil)
		}
	}
	stage, source, sourceEvidence, stored, err := pool.backupStaging.databaseCaptureStage(ctx,
		assignment.TaskID, step, dumpStart != nil || resume.PreparedArtifact != nil)
	if err != nil {
		return err
	}
	if sourceEvidence.Name == "" && dumpStart != nil {
		return errs.New(errs.KindStateConflict, "MySQL dump started without a complete retained source")
	}
	if sourceEvidence.Name == "" && resume.PreparedArtifact != nil {
		return invalidAgentStaging()
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
			return invalidAgentStaging()
		}
		archive = preparedArchive
	}
	if sourceEvidence.Name == "" {
		recorder := &mysqlStartRecorder{publisher: publisher, authority: authority, pointID: capture.PointId,
			resume: &agentpb.BackupStepResume{Operation: &agentpb.BackupStepResume_Capture{Capture: resume}}}
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
			return invalidAgentStaging()
		}
	} else {
		prepared := resume.PreparedArtifact
		if resume.Phase == agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_CAPTURING && dumpStart == nil ||
			prepared != nil && (prepared.GetMysql() == nil || prepared.Evidence == nil ||
				prepared.Evidence.SourceSizeBytes != sourceEvidence.Size ||
				!bytes.Equal(prepared.Evidence.SourceSha256, sourceEvidence.SHA256[:])) {
			return invalidAgentStaging()
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
			result, err := executor.RecoverExecution(ctx, container, original, execID, discardWriteCloser{Writer: io.Discard})
			if err != nil || !result.Stdout.EOF || sourceEvidence.Size != result.Stdout.Bytes ||
				mysql84protocol.Digest(sourceEvidence.SHA256) != result.Stdout.SHA256 {
				return errs.New(errs.KindStateConflict, "MySQL retained dump does not match original execution")
			}
			sourceEvidence, err = source.Publish(ctx)
			if err != nil {
				return err
			}
		}
	}
	storedEvidence := sourceEvidence
	if capture.Encryption.Kind == agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE {
		err = pool.ConsumeBackupSecretSlot(ctx, assignment.TaskID, assignment.AssignmentID, step.StepId,
			agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_CURRENT_AGE_IDENTITY,
			func(identity []byte) error {
				var encryptionErr error
				stored, storedEvidence, encryptionErr = encryptVolumeSource(
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
		return invalidAgentStaging()
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
						UploadVerified: resume.GetUploadVerified(), Stored: stored, Store: store,
						Publish: publisher.publish})
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
	if err := publisher.publish(ctx, &agentpb.BackupCheckpointRequest{Checkpoint: &agentpb.BackupCheckpointRequest_SourceCleanupCompleted{SourceCleanupCompleted: &agentpb.BackupSourceCleanupCompleted{PointId: capture.PointId,
		Evidence: proto.CloneOf(prepared.Evidence)}}}); err != nil {
		return err
	}
	return pool.retireMySQLExecution(ctx, assignment, step, dumpStart, nil)
}

func observeMySQLArchive(ctx context.Context, executor *mysql84execution.Executor,
	container mysql84execution.Container, step *agentpb.BackupStepAuthority,
) (backupmysql.ArchiveEvidence, error) {
	authority := step.GetCapture().GetMysql()
	if authority == nil {
		return backupmysql.ArchiveEvidence{}, invalidAgentStaging()
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

type discardWriteCloser struct{ io.Writer }

func (discardWriteCloser) Close() error { return nil }
