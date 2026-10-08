package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/databaseversion"
	"github.com/AlanD20/groundplane/internal/common/mysql84protocol"
	"github.com/AlanD20/groundplane/internal/infra/agentpostgresjournal"
	"github.com/AlanD20/groundplane/internal/infra/docker/mysql84execution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (pool *WorkerPool) executeBackupMySQLRestore(ctx context.Context,
	assignment taskassignment.Assignment, execution *agentpb.ExecutionStep,
) (mutationAttempted bool, resultErr error) {
	step := execution.GetBackupStep()
	if step.GetRestore().GetMysql() == nil || assignment.BackupAuthority == nil ||
		assignment.BackupResume == nil || pool.backupStaging == nil || pool.compose == nil {
		return false, errs.New(errs.KindInternal, "MySQL Restore runtime or assignment authority is incomplete")
	}
	resume := postgresRestoreResume(assignment, step)
	if resume == nil {
		return false, invalidAgentStaging()
	}
	ctx, cancel := context.WithDeadline(ctx, time.Unix(0, int64(step.StepDeadlineUnixNano)))
	defer cancel()
	if err := agentpostgresjournal.Mark(ctx, databaseExecutionIDs(assignment.TaskID, step)); err != nil {
		return false, err
	}
	if resume.GetSourceCleanupCompleted() != nil {
		absent, err := pool.backupStaging.volumeStageAbsent(assignment.TaskID, step)
		if err != nil || !absent || !proto.Equal(resume.GetSourceCleanupCompleted().Evidence,
			step.GetRestore().ExpectedEvidence) {
			return true, invalidAgentStaging()
		}
		return true, pool.retireMySQLExecution(ctx, assignment, step, nil, resume.MysqlApplyStart)
	}
	if resume.Phase != agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_ARTIFACT_VALIDATION &&
		resume.Phase != agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_PREPARATION &&
		resume.Phase != agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_MUTATION &&
		resume.Phase != agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY {
		return true, invalidAgentStaging()
	}
	mutationAttempted = resume.Phase >= agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_MUTATION ||
		resume.GetMysqlRestoreVerified() != nil || resume.GetMysqlServiceProgress() != nil
	authority, err := mysqlAuthority(assignment, step)
	if err != nil {
		return mutationAttempted, err
	}
	var artifact *databaseRestoreArtifact
	stageAbsent, err := pool.backupStaging.volumeStageAbsent(assignment.TaskID, step)
	if err != nil {
		return mutationAttempted, err
	}
	if resume.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY && stageAbsent {
		if !databaseRecoveryComplete(resume, len(step.ConsumerServiceIds)) {
			return true, errs.New(errs.KindStateConflict, "MySQL Restore source disappeared before Service recovery")
		}
	} else {
		artifact, err = pool.downloadDatabaseRestore(ctx, assignment, step)
		if err != nil {
			return mutationAttempted, err
		}
	}
	publisher := &backupStepCheckpoint{pool: pool, taskID: assignment.TaskID,
		assignID: assignment.AssignmentID, step: step, sequence: resume.CheckpointSequence,
		fence: proto.CloneOf(resume.PrecedingCheckpoint)}
	recorder := &mysqlStartRecorder{publisher: publisher, authority: authority,
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
			return true, invalidAgentStaging()
		}
		if _, err := executor.RecoverExecution(ctx, container, original, execID, nil); err != nil {
			return true, err
		}
		return pool.finishMySQLRestore(ctx, assignment, execution, authority, executor, container,
			artifact, publisher, resume.MysqlApplyStart)
	}
	if resume.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY {
		if _, err := mysqlVerifyRestore(ctx, executor, container, step, authority); err != nil {
			return true, err
		}
		if err := pool.databaseRecoverConsumers(ctx, assignment, execution, resume, publisher); err != nil {
			return true, err
		}
		if err := pool.databaseRestoreCleanup(ctx, assignment, step, artifact, publisher); err != nil {
			return true, err
		}
		return true, pool.retireMySQLExecution(ctx, assignment, step, nil, resume.MysqlApplyStart)
	}
	if resume.CheckpointSequence == 0 {
		if err := publisher.publish(ctx, &agentpb.BackupCheckpointRequest{Checkpoint: &agentpb.BackupCheckpointRequest_RestoreArtifactValidated{RestoreArtifactValidated: artifact.proof}}); err != nil {
			return mutationAttempted, err
		}
	} else if validated := resume.GetArtifactValidated(); validated != nil && !proto.Equal(validated, artifact.proof) {
		return mutationAttempted, invalidAgentStaging()
	}
	if resume.CheckpointSequence <= 1 || resume.GetMysqlContainerObserved() != nil {
		if err := publishMySQLContainer(ctx, publisher, resume.GetMysqlContainerObserved(), authority, container); err != nil {
			return mutationAttempted, err
		}
	}
	if err := pool.databaseStopConsumers(ctx, assignment, execution, resume, publisher, &mutationAttempted); err != nil {
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
	reader, err := artifact.source.OpenPrefix(ctx)
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
	result, err := executor.Execute(ctx, container, request, reader, nil)
	closeErr := reader.Close()
	if err != nil || closeErr != nil || !result.Stdin.EOF || result.Stdin.Bytes != artifact.evidence.Size ||
		result.Stdin.SHA256 != artifact.evidence.SHA256 {
		return mutationAttempted, errs.New(errs.KindStateConflict, "MySQL Restore apply proof is unavailable")
	}
	return pool.finishMySQLRestore(ctx, assignment, execution, authority, executor, container,
		artifact, publisher, recorder.applyStart)
}

func (pool *WorkerPool) finishMySQLRestore(ctx context.Context, assignment taskassignment.Assignment,
	execution *agentpb.ExecutionStep, authority mysqlStepAuthority, executor *mysql84execution.Executor,
	container mysql84execution.Container, artifact *databaseRestoreArtifact, publisher *backupStepCheckpoint,
	start *agentpb.BackupMySQLRestoreApplyStartCheckpoint,
) (bool, error) {
	step := execution.GetBackupStep()
	verificationSHA, err := mysqlVerifyRestore(ctx, executor, container, step, authority)
	if err != nil {
		return true, err
	}
	if err := publisher.publish(ctx, &agentpb.BackupCheckpointRequest{Checkpoint: &agentpb.BackupCheckpointRequest_MysqlRestoreVerified{MysqlRestoreVerified: &agentpb.BackupMySQLRestoreVerified{PointId: step.GetRestore().PointId, ContainerId: container.ID,
		Evidence:           proto.CloneOf(step.GetRestore().ExpectedEvidence),
		VerificationSha256: verificationSHA[:]}}}); err != nil {
		return true, err
	}
	if err := pool.databaseRecoverConsumers(ctx, assignment, execution, nil, publisher); err != nil {
		return true, err
	}
	if err := pool.databaseRestoreCleanup(ctx, assignment, step, artifact, publisher); err != nil {
		return true, err
	}
	return true, pool.retireMySQLExecution(ctx, assignment, step, nil, start)
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
