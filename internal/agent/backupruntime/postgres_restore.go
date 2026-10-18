package backupruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/common/databaseversion"
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentpostgresjournal"
	"github.com/AlanD20/groundplane/internal/infra/docker/postgres16execution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func restorePostgres(ctx context.Context, assignment *agentpb.BackupWorkAssignment,
	identity workexecution.Identity, ordinal uint32, staging *Staging, consumers BackupConsumers,
	delivery *checkpointdelivery.Backup, exchange Exchange,
) (mutationAttempted bool, resultErr error) {
	authority, err := postgresAuthority(assignment, identity, ordinal)
	if err != nil {
		return false, err
	}
	step := assignment.Input.Steps[ordinal]
	if step.GetRestore().GetPostgres() == nil {
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
		return true, retirePostgresExecution(ctx, assignment, identity, ordinal, publisher, nil, resume.ApplyStart)
	}
	if resume.Phase != agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_ARTIFACT_VALIDATION &&
		resume.Phase != agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_PREPARATION &&
		resume.Phase != agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_MUTATION &&
		resume.Phase != agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY {
		return true, invalidDatabaseStaging()
	}
	mutationAttempted = resume.Phase >= agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_MUTATION ||
		resume.GetPostgresRestoreVerified() != nil || resume.GetPostgresServiceProgress() != nil
	var artifact *databaseRestoreArtifact
	stageAbsent, err := staging.SourceAbsent(assignment, identity, ordinal)
	if err != nil {
		return mutationAttempted, err
	}
	if resume.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY && stageAbsent {
		if !databaseRecoveryComplete(resume, len(step.ConsumerServiceIds)) {
			return true, errs.New(
				errs.KindStateConflict,
				"PostgreSQL Restore source disappeared before Service recovery",
			)
		}
	} else {
		artifact, err = downloadDatabaseRestore(ctx, assignment, identity, ordinal, staging, exchange)
		if err != nil {
			return mutationAttempted, err
		}
	}
	recorder := &postgresStartRecorder{publisher: publisher, authority: authority, step: step,
		pointID:   step.GetRestore().PointId,
		resume:    &agentpb.BackupStepResume{Operation: &agentpb.BackupStepResume_Restore{Restore: resume}},
		attempted: &mutationAttempted}
	executor, err := postgres16execution.New(authority.index, nativePostgresArchitecture(), recorder)
	if err != nil {
		return mutationAttempted, err
	}
	defer func() {
		if closeErr := executor.Close(); closeErr != nil {
			resultErr = errs.WrapJoined(errs.KindInternal, resultErr, closeErr)
		}
	}()
	container, err := executor.ResolveContainer(ctx, authority.selection)
	if err != nil {
		return mutationAttempted, err
	}
	observed, err := observePostgresTarget(ctx, executor, container, step, authority.database)
	expected, expectedErr := databaseversion.FromWire(step.GetRestore().GetPostgres().ExpectedTargetVersions)
	if err != nil || expectedErr != nil || observed != expected {
		return mutationAttempted, errs.New(
			errs.KindStateConflict,
			"Restore target identity or version changed; obtain a new preview",
		)
	}
	if resume.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_MUTATION {
		// Inspect the original Exec and its durable terminal/reap/source proof.
		// No code in this branch can execute a second RestoreApply.
		original, containerID, execID, err := postgresOriginalRequest(step, nil, resume.ApplyStart)
		if err != nil || containerID != container.ID {
			return true, invalidDatabaseStaging()
		}
		if _, err := executor.RecoverExecution(ctx, container, original, execID); err != nil {
			return true, err
		}
		return finishPostgresRestore(ctx, assignment, identity, ordinal, staging, consumers, authority, executor, container,
			artifact, publisher, resume.ApplyStart)
	}
	if resume.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY {
		if _, err := postgresVerifyRestore(ctx, executor, container, step, authority); err != nil {
			return true, err
		}
		if err := databaseRecoverConsumers(ctx, consumers, assignment, ordinal, resume, publisher); err != nil {
			return true, err
		}
		if err := databaseRestoreCleanup(ctx, assignment, identity, ordinal, staging, artifact, publisher); err != nil {
			return true, err
		}
		return true, retirePostgresExecution(ctx, assignment, identity, ordinal, publisher, nil, resume.ApplyStart)
	}
	if err := postgresListArchive(ctx, executor, container, step, artifact.source, artifact.evidence); err != nil {
		return mutationAttempted, err
	}
	if resume.CheckpointSequence == 0 {
		if err := publisher.RestoreArtifactValidated(ctx, artifact.proof); err != nil {
			return mutationAttempted, err
		}
	} else if validated := resume.GetArtifactValidated(); validated != nil && !proto.Equal(validated, artifact.proof) {
		return mutationAttempted, invalidDatabaseStaging()
	}
	if resume.CheckpointSequence <= 1 || resume.GetPostgresContainerObserved() != nil {
		if err := publishPostgresContainer(ctx, publisher, resume.GetPostgresContainerObserved(),
			authority, container); err != nil {
			return mutationAttempted, err
		}
	}
	if err := databaseStopConsumers(ctx, consumers, assignment, ordinal, resume, publisher, &mutationAttempted); err != nil {
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
	return finishPostgresRestore(ctx, assignment, identity, ordinal, staging, consumers, authority, executor, container,
		artifact, publisher, recorder.applyStart)
}

func finishPostgresRestore(ctx context.Context, assignment *agentpb.BackupWorkAssignment,
	identity workexecution.Identity, ordinal uint32, staging *Staging, consumers BackupConsumers,
	authority postgresStepAuthority, executor *postgres16execution.Executor,
	container postgres16execution.Container, artifact *databaseRestoreArtifact, publisher *checkpointdelivery.BackupSource,
	start *agentpb.BackupPostgresRestoreApplyStartCheckpoint,
) (bool, error) {
	step := assignment.Input.Steps[ordinal]
	verificationSHA, err := postgresVerifyRestore(ctx, executor, container, step, authority)
	if err != nil {
		return true, err
	}
	if err := publisher.PostgresRestoreVerified(ctx, &agentpb.BackupPostgresRestoreVerified{
		PointId: step.GetRestore().PointId, ContainerId: container.ID,
		Evidence: proto.CloneOf(step.GetRestore().ExpectedEvidence), VerificationSha256: verificationSHA[:],
	}); err != nil {
		return true, err
	}
	if err := databaseRecoverConsumers(ctx, consumers, assignment, ordinal, nil, publisher); err != nil {
		return true, err
	}
	if err := databaseRestoreCleanup(ctx, assignment, identity, ordinal, staging, artifact, publisher); err != nil {
		return true, err
	}
	return true, retirePostgresExecution(ctx, assignment, identity, ordinal, publisher, nil, start)
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
