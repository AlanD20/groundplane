package backupruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/common/databaseversion"
	"github.com/AlanD20/groundplane/internal/common/mysql84protocol"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentpostgresjournal"
	"github.com/AlanD20/groundplane/internal/infra/docker/mysql84execution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func restoreMySQL(ctx context.Context, assignment *agentpb.BackupWorkAssignment,
	identity workexecution.Identity, ordinal uint32, staging *Staging, consumers BackupConsumers,
	delivery *checkpointdelivery.Backup, exchange Exchange,
) (mutationAttempted bool, resultErr error) {
	authority, err := mysqlAuthority(assignment, identity, ordinal)
	if err != nil {
		return false, err
	}
	step := assignment.Input.Steps[ordinal]
	if step.GetRestore().GetMysql() == nil {
		return false, invalidDatabaseStaging()
	}
	publisher, err := delivery.Source(ordinal)
	if err != nil {
		return false, err
	}
	cursor, err := publisher.Current(ctx)
	if err != nil {
		return false, err
	}
	resume := cursor.GetRestore()
	if resume == nil {
		return false, invalidDatabaseStaging()
	}
	ctx, cancel := context.WithDeadline(ctx, time.Unix(0, int64(step.StepDeadlineUnixNano)))
	defer cancel()
	if err := agentpostgresjournal.Mark(ctx, agentpostgresjournal.IDs{Task: assignment.Attempt.TaskId, ExecutionID: step.ExecutionId, Point: step.GetRestore().PointId}); err != nil {
		return false, err
	}
	if resume.GetSourceCleanupCompleted() != nil {
		absent, err := staging.SourceAbsent(assignment, identity, ordinal)
		if err != nil || !absent || !proto.Equal(resume.GetSourceCleanupCompleted().Evidence,
			step.GetRestore().ExpectedEvidence) {
			return true, invalidDatabaseStaging()
		}
		return true, retireMySQLExecution(ctx, assignment, identity, ordinal, publisher, nil, resume.MysqlApplyStart)
	}
	if resume.Phase != agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_ARTIFACT_VALIDATION &&
		resume.Phase != agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_PREPARATION &&
		resume.Phase != agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_MUTATION &&
		resume.Phase != agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY {
		return true, invalidDatabaseStaging()
	}
	mutationAttempted = resume.Phase >= agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_MUTATION ||
		resume.GetMysqlRestoreVerified() != nil || resume.GetMysqlServiceProgress() != nil
	var artifact *databaseRestoreArtifact
	stageAbsent, err := staging.SourceAbsent(assignment, identity, ordinal)
	if err != nil {
		return mutationAttempted, err
	}
	if resume.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY && stageAbsent {
		if !databaseRecoveryComplete(resume, len(step.ConsumerServiceIds)) {
			return true, errs.New(errs.KindStateConflict, "MySQL Restore source disappeared before Service recovery")
		}
	} else {
		artifact, err = downloadDatabaseRestore(ctx, assignment, identity, ordinal, staging, exchange)
		if err != nil {
			return mutationAttempted, err
		}
	}
	recorder := &mysqlStartRecorder{publisher: publisher, authority: authority, step: step,
		pointID:   step.GetRestore().PointId,
		resume:    &agentpb.BackupStepResume{Operation: &agentpb.BackupStepResume_Restore{Restore: resume}},
		attempted: &mutationAttempted}
	executor, err := mysql84execution.New(authority.selection, recorder)
	if err != nil {
		return mutationAttempted, err
	}
	defer func() {
		if closeErr := executor.Close(); closeErr != nil {
			resultErr = errs.WrapJoined(errs.KindInternal, resultErr, closeErr)
		}
	}()
	container, err := executor.ResolveContainer(ctx)
	if err != nil {
		return mutationAttempted, err
	}
	observed, err := observeMySQLTarget(ctx, executor, container, step, authority.database)
	expected, expectedErr := databaseversion.FromWire(step.GetRestore().GetMysql().ExpectedTargetVersions)
	if err != nil || expectedErr != nil || observed != expected {
		return mutationAttempted, errs.New(
			errs.KindStateConflict,
			"Restore target identity or version changed; obtain a new preview",
		)
	}
	if resume.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_MUTATION {
		original, containerID, execID, err := mysqlOriginalRequest(step, nil, resume.MysqlApplyStart)
		if err != nil || containerID != container.ID {
			return true, invalidDatabaseStaging()
		}
		if _, err := executor.RecoverExecution(ctx, container, original, execID, nil); err != nil {
			return true, err
		}
		return finishMySQLRestore(ctx, assignment, identity, ordinal, staging, consumers, authority, executor, container,
			artifact, publisher, resume.MysqlApplyStart)
	}
	if resume.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY {
		if _, err := mysqlVerifyRestore(ctx, executor, container, step, authority); err != nil {
			return true, err
		}
		if err := databaseRecoverConsumers(ctx, consumers, assignment, ordinal, resume, publisher); err != nil {
			return true, err
		}
		if err := databaseRestoreCleanup(ctx, assignment, identity, ordinal, staging, artifact, publisher); err != nil {
			return true, err
		}
		return true, retireMySQLExecution(ctx, assignment, identity, ordinal, publisher, nil, resume.MysqlApplyStart)
	}
	if resume.CheckpointSequence == 0 {
		if err := publisher.RestoreArtifactValidated(ctx, artifact.proof); err != nil {
			return mutationAttempted, err
		}
	} else if validated := resume.GetArtifactValidated(); validated != nil && !proto.Equal(validated, artifact.proof) {
		return mutationAttempted, invalidDatabaseStaging()
	}
	if resume.CheckpointSequence <= 1 || resume.GetMysqlContainerObserved() != nil {
		if err := publishMySQLContainer(ctx, publisher, resume.GetMysqlContainerObserved(), authority, container); err != nil {
			return mutationAttempted, err
		}
	}
	if err := databaseStopConsumers(ctx, consumers, assignment, ordinal, resume, publisher, &mutationAttempted); err != nil {
		return mutationAttempted, err
	}
	request, err := newMySQLRequest(mysql84protocol.OperationAssertZeroConnections, step,
		authority.database, "", 0, 0, mysql84protocol.Digest{})
	if err != nil {
		return mutationAttempted, err
	}
	if _, err := executor.Execute(ctx, container, request, nil, nil); err != nil {
		return mutationAttempted, err
	}
	if err := ctx.Err(); err != nil {
		return mutationAttempted, err
	}
	// After admission, a channel disconnect must not truncate the verified SQL
	// input of the original Exec. Join that operation within the existing step
	// deadline; checkpoint delivery and later recovery still use the session.
	applyDeadline, _ := ctx.Deadline()
	applyCtx, cancelApply := context.WithDeadline(context.WithoutCancel(ctx), applyDeadline)
	defer cancelApply()
	reader, err := artifact.source.OpenPrefix(applyCtx)
	if err != nil {
		return mutationAttempted, err
	}
	request, err = newMySQLRequest(mysql84protocol.OperationRestoreApply, step,
		authority.database, authority.role, 0, artifact.evidence.Size,
		mysql84protocol.Digest(artifact.evidence.SHA256))
	if err != nil {
		_ = reader.Close()
		return mutationAttempted, err
	}
	result, err := executor.Execute(applyCtx, container, request, reader, nil)
	closeErr := reader.Close()
	if err != nil || closeErr != nil || !result.Stdin.EOF || result.Stdin.Bytes != artifact.evidence.Size ||
		result.Stdin.SHA256 != artifact.evidence.SHA256 {
		return mutationAttempted, errs.New(errs.KindStateConflict, "MySQL Restore apply proof is unavailable")
	}
	return finishMySQLRestore(ctx, assignment, identity, ordinal, staging, consumers, authority, executor, container,
		artifact, publisher, recorder.applyStart)
}

func finishMySQLRestore(ctx context.Context, assignment *agentpb.BackupWorkAssignment,
	identity workexecution.Identity, ordinal uint32, staging *Staging, consumers BackupConsumers,
	authority mysqlStepAuthority, executor *mysql84execution.Executor,
	container mysql84execution.Container, artifact *databaseRestoreArtifact, publisher *checkpointdelivery.BackupSource,
	start *agentpb.BackupMySQLRestoreApplyStartCheckpoint,
) (bool, error) {
	step := assignment.Input.Steps[ordinal]
	verificationSHA, err := mysqlVerifyRestore(ctx, executor, container, step, authority)
	if err != nil {
		return true, err
	}
	if err := publisher.MysqlRestoreVerified(ctx, &agentpb.BackupMySQLRestoreVerified{PointId: step.GetRestore().PointId, ContainerId: container.ID,
		Evidence:           proto.CloneOf(step.GetRestore().ExpectedEvidence),
		VerificationSha256: verificationSHA[:]}); err != nil {
		return true, err
	}
	if err := databaseRecoverConsumers(ctx, consumers, assignment, ordinal, nil, publisher); err != nil {
		return true, err
	}
	if err := databaseRestoreCleanup(ctx, assignment, identity, ordinal, staging, artifact, publisher); err != nil {
		return true, err
	}
	return true, retireMySQLExecution(ctx, assignment, identity, ordinal, publisher, nil, start)
}

func mysqlVerifyRestore(ctx context.Context, executor *mysql84execution.Executor,
	container mysql84execution.Container, step *agentpb.BackupStepAuthority,
	authority mysqlStepAuthority,
) ([sha256.Size]byte, error) {
	request, err := newMySQLRequest(mysql84protocol.OperationPostRestoreVerify, step,
		authority.database, "", 0, 0, mysql84protocol.Digest{})
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	result, err := executor.Execute(ctx, container, request, nil, nil)
	proof := bytes.TrimSpace(result.Proof)
	if err != nil || !bytes.Equal(proof, []byte(authority.database)) {
		return [sha256.Size]byte{}, errs.New(errs.KindStateConflict, "MySQL post-Restore proof is unavailable")
	}
	return sha256.Sum256(proof), nil
}
