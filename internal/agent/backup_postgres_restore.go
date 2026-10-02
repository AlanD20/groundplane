package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/internal/infra/agentpostgresjournal"
	"github.com/AlanD20/groundplane/internal/infra/docker/postgres16execution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (pool *WorkerPool) executeBackupPostgresRestore(ctx context.Context,
	assignment taskassignment.Assignment, execution *agentpb.ExecutionStep,
) (mutationAttempted bool, resultErr error) {
	step := execution.GetBackupStep()
	if step.GetRestore().GetPostgres() == nil || assignment.BackupAuthority == nil ||
		assignment.BackupResume == nil || pool.backupStaging == nil || pool.compose == nil {
		return false, errs.New(errs.KindInternal, "PostgreSQL Restore runtime or assignment authority is incomplete")
	}
	resume := postgresRestoreResume(assignment, step)
	if resume == nil {
		return false, invalidAgentStaging()
	}
	ctx, cancel := context.WithDeadline(ctx, time.Unix(0, int64(step.StepDeadlineUnixNano)))
	defer cancel()
	if err := agentpostgresjournal.Mark(ctx, postgresExecutionIDs(assignment.TaskID, step)); err != nil {
		return false, err
	}
	if resume.GetSourceCleanupCompleted() != nil {
		absent, err := pool.backupStaging.volumeStageAbsent(assignment.TaskID, step)
		if err != nil || !absent || !proto.Equal(resume.GetSourceCleanupCompleted().Evidence,
			step.GetRestore().ExpectedEvidence) {
			return true, invalidAgentStaging()
		}
		return true, pool.retirePostgresExecution(ctx, assignment, step, nil, resume.ApplyStart)
	}
	if resume.Phase != agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_ARTIFACT_VALIDATION &&
		resume.Phase != agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_PREPARATION &&
		resume.Phase != agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_MUTATION &&
		resume.Phase != agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY {
		return true, invalidAgentStaging()
	}
	mutationAttempted = resume.Phase >= agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_MUTATION ||
		resume.GetPostgresRestoreVerified() != nil || resume.GetPostgresServiceProgress() != nil
	authority, err := postgresAuthority(assignment, step)
	if err != nil {
		return mutationAttempted, err
	}
	var artifact *postgresRestoreArtifact
	stageAbsent, err := pool.backupStaging.volumeStageAbsent(assignment.TaskID, step)
	if err != nil {
		return mutationAttempted, err
	}
	if resume.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY && stageAbsent {
		if !postgresRecoveryComplete(resume, len(step.ConsumerServiceIds)) {
			return true, errs.New(
				errs.KindStateConflict,
				"PostgreSQL Restore source disappeared before Service recovery",
			)
		}
	} else {
		artifact, err = pool.downloadPostgresRestore(ctx, assignment, step)
		if err != nil {
			return mutationAttempted, err
		}
	}
	publisher := &backupStepCheckpoint{pool: pool, taskID: assignment.TaskID,
		assignID: assignment.AssignmentID, step: step, sequence: resume.CheckpointSequence,
		fence: proto.CloneOf(resume.PrecedingCheckpoint)}
	recorder := &postgresStartRecorder{publisher: publisher, authority: authority,
		pointID:   step.GetRestore().PointId,
		resume:    &agentpb.BackupStepResume{Operation: &agentpb.BackupStepResume_Restore{Restore: resume}},
		attempted: &mutationAttempted}
	executor, err := postgres16execution.New(authority.index, nativePostgresArchitecture(), recorder)
	if err != nil {
		return mutationAttempted, err
	}
	defer func() { resultErr = errs.WrapJoined(errs.KindInternal, resultErr, executor.Close()) }()
	container, err := executor.ResolveContainer(ctx, authority.selection)
	if err != nil {
		return mutationAttempted, err
	}
	if resume.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_MUTATION {
		// Inspect the original Exec and its durable terminal/reap/source proof.
		// No code in this branch can execute a second RestoreApply.
		original, containerID, execID, err := postgresOriginalRequest(step, nil, resume.ApplyStart)
		if err != nil || containerID != container.ID {
			return true, invalidAgentStaging()
		}
		if _, err := executor.RecoverExecution(ctx, container, original, execID); err != nil {
			return true, err
		}
		return pool.finishPostgresRestore(ctx, assignment, execution, authority, executor, container,
			artifact, publisher, resume.ApplyStart)
	}
	if resume.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY {
		if _, err := postgresVerifyRestore(ctx, executor, container, step, authority); err != nil {
			return true, err
		}
		if err := pool.postgresRecoverConsumers(ctx, assignment, execution, resume, publisher); err != nil {
			return true, err
		}
		if err := pool.postgresRestoreCleanup(ctx, assignment, step, artifact, publisher); err != nil {
			return true, err
		}
		return true, pool.retirePostgresExecution(ctx, assignment, step, nil, resume.ApplyStart)
	}
	if err := postgresListArchive(ctx, executor, container, step, artifact.source, artifact.evidence); err != nil {
		return mutationAttempted, err
	}
	if resume.CheckpointSequence == 0 {
		if err := publisher.publish(ctx, &agentpb.BackupCheckpointRequest{Checkpoint: &agentpb.BackupCheckpointRequest_RestoreArtifactValidated{RestoreArtifactValidated: artifact.proof}}); err != nil {
			return mutationAttempted, err
		}
	} else if validated := resume.GetArtifactValidated(); validated != nil && !proto.Equal(validated, artifact.proof) {
		return mutationAttempted, invalidAgentStaging()
	}
	if resume.CheckpointSequence <= 1 || resume.GetPostgresContainerObserved() != nil {
		if err := publishPostgresContainer(ctx, publisher, resume.GetPostgresContainerObserved(),
			authority, container); err != nil {
			return mutationAttempted, err
		}
	}
	if err := pool.postgresStopConsumers(ctx, assignment, execution, resume, publisher, &mutationAttempted); err != nil {
		return mutationAttempted, err
	}
	for _, operation := range []postgres16protocol.Operation{
		postgres16protocol.OperationTerminateDBConnections,
		postgres16protocol.OperationAssertZeroDBConnections,
	} {
		request, err := newPostgresRequest(operation, step, authority.database, "", 0, postgres16protocol.Digest{})
		if err != nil {
			return mutationAttempted, err
		}
		if _, err := executor.Execute(ctx, container, request, nil, nil); err != nil {
			return mutationAttempted, err
		}
	}
	reader, err := artifact.source.OpenPrefix(ctx)
	if err != nil {
		return mutationAttempted, err
	}
	request, err := newPostgresRequest(postgres16protocol.OperationRestoreApply, step,
		authority.database, authority.role, artifact.evidence.Size,
		postgres16protocol.Digest(artifact.evidence.SHA256))
	if err != nil {
		_ = reader.Close()
		return mutationAttempted, err
	}
	result, err := executor.Execute(ctx, container, request, reader, nil)
	closeErr := reader.Close()
	if err != nil || closeErr != nil || !result.Stdin.EOF ||
		result.Stdin.Bytes != artifact.evidence.Size || result.Stdin.SHA256 != artifact.evidence.SHA256 {
		return mutationAttempted, errs.New(errs.KindStateConflict, "PostgreSQL Restore apply proof is unavailable")
	}
	return pool.finishPostgresRestore(ctx, assignment, execution, authority, executor, container,
		artifact, publisher, recorder.applyStart)
}

func (pool *WorkerPool) finishPostgresRestore(ctx context.Context, assignment taskassignment.Assignment,
	execution *agentpb.ExecutionStep, authority postgresStepAuthority, executor *postgres16execution.Executor,
	container postgres16execution.Container, artifact *postgresRestoreArtifact, publisher *backupStepCheckpoint,
	start *agentpb.BackupPostgresRestoreApplyStartCheckpoint,
) (bool, error) {
	step := execution.GetBackupStep()
	verificationSHA, err := postgresVerifyRestore(ctx, executor, container, step, authority)
	if err != nil {
		return true, err
	}
	if err := publisher.publish(ctx, &agentpb.BackupCheckpointRequest{Checkpoint: &agentpb.BackupCheckpointRequest_PostgresRestoreVerified{PostgresRestoreVerified: &agentpb.BackupPostgresRestoreVerified{
		PointId: step.GetRestore().PointId, ContainerId: container.ID,
		Evidence: proto.CloneOf(step.GetRestore().ExpectedEvidence), VerificationSha256: verificationSHA[:],
	}}}); err != nil {
		return true, err
	}
	if err := pool.postgresRecoverConsumers(ctx, assignment, execution, nil, publisher); err != nil {
		return true, err
	}
	if err := pool.postgresRestoreCleanup(ctx, assignment, step, artifact, publisher); err != nil {
		return true, err
	}
	return true, pool.retirePostgresExecution(ctx, assignment, step, nil, start)
}

func postgresVerifyRestore(ctx context.Context, executor *postgres16execution.Executor,
	container postgres16execution.Container, step *agentpb.BackupStepAuthority,
	authority postgresStepAuthority,
) ([sha256.Size]byte, error) {
	request, err := newPostgresRequest(postgres16protocol.OperationPostRestoreVerify, step,
		authority.database, "", 0, postgres16protocol.Digest{})
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	result, err := executor.Execute(ctx, container, request, nil, nil)
	if err != nil || !bytes.Equal(bytes.TrimSpace(result.Proof), []byte(authority.database)) {
		return [sha256.Size]byte{}, errs.New(errs.KindStateConflict, "PostgreSQL post-Restore proof is unavailable")
	}
	return sha256.Sum256(result.Proof), nil
}

func (pool *WorkerPool) postgresRestoreCleanup(ctx context.Context, assignment taskassignment.Assignment,
	step *agentpb.BackupStepAuthority, artifact *postgresRestoreArtifact,
	publisher *backupStepCheckpoint,
) error {
	if artifact != nil {
		if err := artifact.stage.Cleanup(ctx); err != nil {
			return err
		}
		pool.backupStaging.retirePostgresStage(assignment.TaskID, step, artifact.stage)
	} else {
		absent, err := pool.backupStaging.volumeStageAbsent(assignment.TaskID, step)
		if err != nil || !absent {
			return invalidAgentStaging()
		}
	}
	return publisher.publish(
		ctx,
		&agentpb.BackupCheckpointRequest{
			Checkpoint: &agentpb.BackupCheckpointRequest_SourceCleanupCompleted{
				SourceCleanupCompleted: &agentpb.BackupSourceCleanupCompleted{
					PointId: step.GetRestore().PointId, Evidence: proto.CloneOf(step.GetRestore().ExpectedEvidence),
				},
			},
		},
	)
}

func postgresRecoveryComplete(resume *agentpb.BackupRestoreResume, consumers int) bool {
	if resume == nil || resume.Phase != agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY {
		return false
	}
	if consumers == 0 {
		return resume.GetPostgresRestoreVerified() != nil
	}
	progress := resume.GetPostgresServiceProgress()
	return progress != nil && progress.ServiceCursor == 0 &&
		(progress.Phase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_HEALTHY ||
			progress.Phase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_NOT_RUNNING)
}

func postgresRestoreResume(assignment taskassignment.Assignment,
	step *agentpb.BackupStepAuthority,
) *agentpb.BackupRestoreResume {
	for _, candidate := range assignment.BackupResume.Steps {
		if candidate.StepId == step.StepId && candidate.ExecutionId == step.ExecutionId {
			return candidate.GetRestore()
		}
	}
	return nil
}
