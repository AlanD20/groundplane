package agent

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/infra/agentconfigjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (pool *WorkerPool) executeBackupConfigRestore(ctx context.Context, assignment taskassignment.Assignment,
	execution *agentpb.ExecutionStep,
) (mutationAttempted bool, resultErr error) {
	step := execution.GetBackupStep()
	if step.GetRestore().GetConfig() == nil || assignment.BackupAuthority == nil || assignment.BackupResume == nil ||
		pool.materializer == nil {
		return false, errs.New(errs.KindInternal, "Config Restore runtime or assignment authority is incomplete")
	}
	var resume *agentpb.BackupRestoreResume
	for _, candidate := range assignment.BackupResume.Steps {
		if candidate.StepId == step.StepId && candidate.ExecutionId == step.ExecutionId {
			resume = candidate.GetRestore()
			break
		}
	}
	if resume == nil {
		return false, errs.New(errs.KindInternal, "Config Restore resume is missing")
	}
	mutationAttempted = resume.Phase >= agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_PUBLICATION
	ctx, cancel := context.WithDeadline(ctx, time.Unix(0, int64(step.StepDeadlineUnixNano)))
	defer cancel()
	stream, err := pool.backupConfigs.RestoreStream(ctx, assignment.TaskID, assignment.AssignmentID,
		step.StepId, pool.publishBackupConfigFrame)
	if err != nil {
		return mutationAttempted, err
	}
	if resume.GetConfigProgress().GetSourceCleanupCompleted() != nil {
		absent, err := pool.backupStaging.configStageAbsent(assignment.TaskID, step)
		if err != nil {
			return true, err
		}
		if !absent {
			return true, invalidAgentStaging()
		}
		return true, agentconfigjournal.ResumeCleanup(
			ctx,
			agentprotocol.StatePath+"/config-transfers/"+stream.Binding().TransferID,
			stream.Binding(),
			step,
			&agentpb.BackupStepResume{StepId: step.StepId, ExecutionId: step.ExecutionId,
				Operation: &agentpb.BackupStepResume_Restore{Restore: resume}},
		)
	}
	journal, err := agentconfigjournal.Open(
		ctx,
		agentprotocol.StatePath+"/config-transfers/"+stream.Binding().TransferID,
		stream.Binding(),
		step,
	)
	if err != nil {
		return mutationAttempted, err
	}
	defer func() {
		if closeErr := journal.Close(); closeErr != nil {
			resultErr = errs.WrapJoined(errs.KindStorageUnavailable, resultErr, closeErr)
		}
	}()
	publisher := &backupStepCheckpoint{pool: pool, taskID: assignment.TaskID, assignID: assignment.AssignmentID,
		step: step, sequence: resume.CheckpointSequence, fence: proto.CloneOf(resume.PrecedingCheckpoint)}
	if resume.GetConfigProgress().GetMaterialization() != nil {
		return true, pool.finishConfigRestore(ctx, assignment, step, resume, publisher, journal)
	}
	stage, source, stored, err := pool.backupStaging.configRestoreStage(ctx, assignment.TaskID, step)
	if err != nil {
		return mutationAttempted, err
	}
	artifact, err := pool.downloadConfigRestore(ctx, assignment, step, stage, source, stored)
	if err != nil {
		return mutationAttempted, err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if closeErr := artifact.Close(closeCtx); closeErr != nil {
			resultErr = errs.WrapJoined(errs.KindStorageUnavailable, resultErr, closeErr)
		}
	}()
	if resume.CheckpointSequence == 0 {
		if err := publisher.publish(ctx, &agentpb.BackupCheckpointRequest{
			Checkpoint: &agentpb.BackupCheckpointRequest_RestoreArtifactValidated{RestoreArtifactValidated: artifact.Evidence()},
		}); err != nil {
			return mutationAttempted, err
		}
	} else if resume.GetArtifactValidated() != nil && !proto.Equal(resume.GetArtifactValidated(), artifact.Evidence()) {
		return mutationAttempted, invalidAgentStaging()
	}
	completed, err := artifact.SendRestore(ctx, stream, journal)
	if err != nil {
		return mutationAttempted, err
	}
	if resume.GetConfigProgress().GetTransfer() == nil {
		// Publication can have committed even if its acknowledgement is lost.
		// Mark the possible mutation before sending, not only after success.
		mutationAttempted = true
		if err := publisher.publish(ctx, &agentpb.BackupCheckpointRequest{Checkpoint: &agentpb.BackupCheckpointRequest_Config{
			Config: &agentpb.BackupConfigCheckpoint{Checkpoint: &agentpb.BackupConfigCheckpoint_TransferCompleted{TransferCompleted: completed}},
		}}); err != nil {
			return true, err
		}
	} else if !proto.Equal(resume.GetConfigProgress().GetTransfer(), completed) {
		return true, invalidAgentStaging()
	}
	proof, err := artifact.Materialize(ctx, assignment.TaskID, step.StepId, pool.volumeRoot, pool.materializer)
	if err != nil {
		return true, err
	}
	if err := publisher.publish(ctx, &agentpb.BackupCheckpointRequest{Checkpoint: &agentpb.BackupCheckpointRequest_Config{
		Config: &agentpb.BackupConfigCheckpoint{Checkpoint: &agentpb.BackupConfigCheckpoint_MaterializationVerified{MaterializationVerified: proof}},
	}}); err != nil {
		return true, err
	}
	resume, err = configRestoreMaterializedResume(ctx, journal, publisher, proof)
	if err != nil {
		return true, err
	}
	if err := artifact.Close(ctx); err != nil {
		return true, err
	}
	if err := stage.Cleanup(ctx); err != nil {
		return true, err
	}
	pool.backupStaging.retireConfigStage(assignment.TaskID, step, stage)
	return true, pool.finishConfigRestore(ctx, assignment, step, resume, publisher, journal)
}

// Build the local cleanup cursor from the actual durable transfer credits,
// never from the Entry count or a presumed number of chunks.
func configRestoreMaterializedResume(
	ctx context.Context,
	journal *agentconfigjournal.Journal,
	publisher *backupStepCheckpoint,
	proof *agentpb.BackupConfigMaterializationVerified,
) (*agentpb.BackupRestoreResume, error) {
	credits, err := journal.ReadCredits(ctx)
	if err != nil {
		return nil, err
	}
	var metadata *agentpb.BackupConfigMetadataAccepted
	var last *agentpb.BackupConfigCredit
	for _, credit := range credits {
		if credit.GetMetadataAccepted() != nil {
			metadata = credit.GetMetadataAccepted()
		}
		last = credit
	}
	if metadata == nil || last == nil || last.NextOrdinal != proof.MaterializedEntryCount+1 {
		return nil, invalidAgentStaging()
	}
	return &agentpb.BackupRestoreResume{
		CheckpointSequence:  publisher.sequence,
		PrecedingCheckpoint: proto.CloneOf(publisher.fence),
		Phase:               agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_CLEANUP,
		Cursor: &agentpb.BackupRestoreCursor{ObjectAttempt: 1, ConfigRecordSequence: last.CommittedRecordSequence,
			ConfigValueOrdinal: last.NextOrdinal, CumulativeChainSha256: append([]byte(nil), last.CumulativeChainSha256...)},
		Checkpoint: &agentpb.BackupRestoreResume_ConfigProgress{ConfigProgress: &agentpb.BackupConfigProgress{
			MetadataAccepted: proto.Bool(
				true,
			), MetadataTranscriptSha256: append([]byte(nil), metadata.MetadataTranscriptSha256...),
			NextValueOrdinal: last.NextOrdinal, ValueChainSha256: append([]byte(nil), last.CumulativeChainSha256...),
			Progress: &agentpb.BackupConfigProgress_Materialization{Materialization: proto.CloneOf(proof)},
		}},
	}, nil
}
