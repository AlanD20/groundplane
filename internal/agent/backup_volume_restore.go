package agent

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/backupvolume"
	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/internal/infra/agentvolumejournal"
	"github.com/AlanD20/groundplane/internal/infra/backupvolumefs"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (pool *WorkerPool) executeBackupVolumeRestore(ctx context.Context,
	assignment taskassignment.Assignment, execution *agentpb.ExecutionStep,
) (mutationAttempted bool, resultErr error) {
	step := execution.GetBackupStep()
	if step.GetRestore().GetVolume() == nil || assignment.BackupAuthority == nil ||
		assignment.BackupResume == nil || pool.backupStaging == nil || pool.compose == nil {
		return false, errs.New(errs.KindInternal, "Volume Restore runtime or authority is incomplete")
	}
	var resume *agentpb.BackupRestoreResume
	for _, candidate := range assignment.BackupResume.Steps {
		if candidate.StepId == step.StepId && candidate.ExecutionId == step.ExecutionId {
			resume = candidate.GetRestore()
			break
		}
	}
	if resume == nil {
		return false, invalidAgentStaging()
	}
	if volumeRestoreCheckpointProvesVerified(resume, len(step.ConsumerServiceIds)) {
		stageAbsent, err := pool.backupStaging.volumeStageAbsent(assignment.TaskID, step)
		if err != nil {
			return true, err
		}
		metadataAbsent, err := pool.backupStaging.volumeMetadataAbsent(assignment.TaskID, step)
		if err != nil || !stageAbsent || !metadataAbsent {
			return true, invalidAgentStaging()
		}
		return true, nil
	}
	mutationAttempted = resume.Phase >= agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_MUTATION ||
		resume.GetVolumeProgress() != nil
	ctx, cancel := context.WithDeadline(ctx, time.Unix(0, int64(step.StepDeadlineUnixNano)))
	defer cancel()
	receiver, newEntries, newManifest, err := pool.receiveBackupVolumeRestoreNew(ctx, assignment, step)
	if err != nil {
		return mutationAttempted, err
	}
	defer func() {
		if closeErr := receiver.Close(); closeErr != nil {
			resultErr = errs.WrapJoined(errs.KindStorageUnavailable, resultErr, closeErr)
		}
	}()
	artifact, err := pool.downloadVolumeRestore(ctx, assignment, step, newEntries, newManifest)
	if err != nil {
		return mutationAttempted, err
	}
	publisher := &backupStepCheckpoint{pool: pool, taskID: assignment.TaskID,
		assignID: assignment.AssignmentID, step: step, sequence: resume.CheckpointSequence,
		fence: proto.CloneOf(resume.PrecedingCheckpoint)}
	if resume.CheckpointSequence == 0 {
		if err := publisher.publish(ctx, &agentpb.BackupCheckpointRequest{Checkpoint: &agentpb.BackupCheckpointRequest_RestoreArtifactValidated{RestoreArtifactValidated: artifact.proof}}); err != nil {
			return mutationAttempted, err
		}
	} else if validated := resume.GetArtifactValidated(); validated != nil && !proto.Equal(validated, artifact.proof) {
		return mutationAttempted, invalidAgentStaging()
	}
	volumeAuthority := step.GetRestore().GetVolume().Projection
	volume, err := backupvolumefs.Open(ctx, pool.volumeRoot,
		volumeAuthority.AuthorizedVolumeDir, volumeAuthority.ComposeVolumeKey)
	if err != nil {
		return mutationAttempted, err
	}
	defer func() {
		if closeErr := volume.Close(); closeErr != nil {
			resultErr = errs.WrapJoined(errs.KindStorageUnavailable, resultErr, closeErr)
		}
	}()
	stopped := volumeRestoreConsumersStopped(resume)
	if !stopped {
		mutationAttempted = true // stop intent is published before the helper call.
		count, digest, err := pool.stopBackupVolumeConsumers(ctx, assignment, execution, resume, publisher)
		if err != nil {
			return mutationAttempted, err
		}
		old, err := volume.Snapshot(ctx)
		if err != nil {
			return mutationAttempted, err
		}
		if err := pool.sendBackupVolumeManifest(ctx, assignment, step,
			agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_OLD,
			agentpb.BackupVolumeManifestRole_BACKUP_VOLUME_MANIFEST_ROLE_STOPPED_LIVE_OLD,
			old.Entries, nil); err != nil {
			return mutationAttempted, err
		}
		if err := publisher.publish(ctx, &agentpb.BackupCheckpointRequest{Checkpoint: &agentpb.BackupCheckpointRequest_Volume{Volume: &agentpb.BackupVolumeCheckpoint{Checkpoint: &agentpb.BackupVolumeCheckpoint_ConsumersStopped{ConsumersStopped: &agentpb.BackupVolumeConsumersStopped{
			StoppedCount: count, ServiceProgressSha256: digest[:]}}}}}); err != nil {
			return mutationAttempted, err
		}
	}
	newManifestSHA, err := backupvolume.ContentManifestSHA256(newManifest)
	if err != nil || !bytes.Equal(newManifestSHA[:], step.GetRestore().GetVolume().Archive.ContentManifestSha256) {
		return mutationAttempted, invalidAgentStaging()
	}
	journalConfig := agentvolumejournal.Config{JournalRoot: agentvolumejournal.AgentRoot,
		TaskID: assignment.TaskID, AssignmentID: assignment.AssignmentID, StepID: step.StepId,
		PointID: step.GetRestore().PointId, RestoreGenerationID: step.ExecutionId,
		NewManifestSHA256: newManifestSHA,
		NewFullTreeSHA256: artifact.validated.Evidence.Archive.FullTreeSHA256,
		NewEntryCount:     uint64(len(newEntries)),
		VolumeRoot:        pool.volumeRoot, AuthorizedVolumeDir: volumeAuthority.AuthorizedVolumeDir,
		ComposeKey: volumeAuthority.ComposeVolumeKey, SiblingName: ".gp-restore-" + step.ExecutionId}
	copy(journalConfig.StepSHA256[:], step.StepDigest)
	journalConfig, existingJournal, err := findVolumeRestoreJournal(ctx, journalConfig, resume)
	if err != nil {
		return mutationAttempted, err
	}
	var oldTree backupvolumefs.Tree
	if !existingJournal {
		oldTree, err = volume.Snapshot(ctx)
		if err != nil {
			return mutationAttempted, err
		}
		oldManifest, err := backupvolumetransfer.EncodeEntries(oldTree.Entries)
		if err != nil {
			return mutationAttempted, err
		}
		journalConfig.OldManifestSHA256, err = backupvolume.ContentManifestSHA256(oldManifest)
		if err != nil {
			return mutationAttempted, err
		}
		journalConfig.OldFullTreeSHA256 = oldTree.FullTreeSHA256
		journalConfig.OldEntryCount = uint64(len(oldTree.Entries))
	}
	journal, err := agentvolumejournal.Open(ctx, journalConfig)
	if err != nil {
		return mutationAttempted, err
	}
	defer func() {
		if closeErr := journal.Close(); closeErr != nil {
			resultErr = errs.WrapJoined(errs.KindStorageUnavailable, resultErr, closeErr)
		}
	}()
	mutationJournal := &volumeRestoreCheckpointJournal{local: journal, publisher: publisher,
		pointID: step.GetRestore().PointId, generationID: step.ExecutionId, newManifest: newManifestSHA}
	state, err := journal.ReadState(ctx)
	if err != nil {
		return mutationAttempted, err
	}
	if state.UncommittedPrefix {
		return mutationAttempted, invalidAgentStaging()
	}
	if err := reconcileVolumeRestoreJournal(ctx, state, mutationJournal, resume); err != nil {
		return mutationAttempted, err
	}
	if existingJournal && !state.Exchanged &&
		(state.Pending == nil || state.Pending.Kind != backupvolumefs.MutationExchange) {
		oldTree, err = volume.Snapshot(ctx)
		if err != nil || oldTree.FullTreeSHA256 != journalConfig.OldFullTreeSHA256 {
			return mutationAttempted, invalidAgentStaging()
		}
	}
	var replacement *backupvolumefs.Replacement
	openedExchanged := false
	switch {
	case state.RootDeleted:
		oldTree, err = reconstructVolumeOldTree(nil, state, journalConfig)
	case state.Exchanged || state.Pending != nil && state.Pending.Kind == backupvolumefs.MutationExchange:
		replacement, err = volume.OpenExchanged(ctx, journalConfig.SiblingName,
			state.ExchangeOldInode, state.ExchangeNewInode)
		openedExchanged = err == nil
		if err != nil && state.Exchanged && state.Pending != nil &&
			state.Pending.Kind == backupvolumefs.MutationDelete &&
			bytes.Equal(state.Pending.Entry.Path, []byte(".")) {
			oldTree, err = reconstructVolumeOldTree(nil, state, journalConfig)
			if err == nil {
				err = volume.ResumeRootDeletion(ctx, journalConfig.SiblingName,
					state.ExchangeOldInode, state.ExchangeNewInode, state.Pending.Ordinal,
					oldTree.FullTreeSHA256, journalConfig.NewFullTreeSHA256,
					oldTree.Entries[0], mutationJournal)
			}
			if err == nil {
				state, err = journal.ReadState(ctx)
			}
		}
		if err != nil && state.Pending != nil && state.Pending.Kind == backupvolumefs.MutationExchange {
			// An exchange intent can precede the physical rename. The exact
			// pre-exchange placement is checked by OpenReplacement/Exchange.
			replacement, err = volume.OpenReplacement(ctx, journalConfig.SiblingName, state.RootNewInode)
			openedExchanged = false
		}
		if err == nil && replacement != nil && openedExchanged {
			remaining, snapshotErr := replacement.SnapshotOld(ctx)
			if snapshotErr != nil {
				return mutationAttempted, snapshotErr
			}
			oldTree, err = reconstructVolumeOldTree(&remaining, state, journalConfig)
		} else if err == nil && replacement != nil {
			oldTree, err = volume.Snapshot(ctx)
			if err == nil && oldTree.FullTreeSHA256 != journalConfig.OldFullTreeSHA256 {
				err = invalidAgentStaging()
			}
		}
	case state.ConstructionCursor == 0 && state.Pending == nil:
		replacement, err = volume.CreateReplacement(ctx, journalConfig.SiblingName, newEntries[0], mutationJournal)
	case state.ConstructionCursor == 0 && state.Pending.Kind == backupvolumefs.MutationConstruction && state.Pending.Ordinal == 1:
		replacement, err = volume.ResumeCreatedReplacement(
			ctx,
			journalConfig.SiblingName,
			newEntries[0],
			mutationJournal,
		)
	default:
		replacement, err = volume.OpenReplacement(ctx, journalConfig.SiblingName, state.RootNewInode)
	}
	if err != nil {
		return mutationAttempted, err
	}
	if replacement != nil {
		defer func() {
			if closeErr := replacement.Close(); closeErr != nil {
				resultErr = errs.WrapJoined(errs.KindStorageUnavailable, resultErr, closeErr)
			}
		}()
	}
	state, err = journal.ReadState(ctx)
	if err != nil {
		return mutationAttempted, err
	}
	if !state.Exchanged && (state.Pending == nil || state.Pending.Kind != backupvolumefs.MutationExchange) {
		reader, err := artifact.source.OpenPrefix(ctx)
		if err != nil {
			return mutationAttempted, err
		}
		if err := replacement.ConstructArchive(ctx, reader, artifact.validated, artifact.manifest,
			max(uint64(2), state.ConstructionCursor+1),
			state.Pending != nil && state.Pending.Kind == backupvolumefs.MutationConstruction,
			mutationJournal); err != nil {
			_ = reader.Close()
			return mutationAttempted, err
		}
		if err := reader.Close(); err != nil {
			return mutationAttempted, err
		}
		state, err = journal.ReadState(ctx)
		if err != nil {
			return mutationAttempted, err
		}
		order, err := backupvolumefs.FinalizationOrder(newEntries)
		if err != nil {
			return mutationAttempted, err
		}
		for ordinal := state.FinalizationCursor + 1; ordinal <= uint64(len(order)); ordinal++ {
			if err := replacement.Finalize(ctx, ordinal, order[ordinal-1],
				state.Pending != nil && state.Pending.Kind == backupvolumefs.MutationFinalization &&
					state.Pending.Ordinal == ordinal, mutationJournal); err != nil {
				return mutationAttempted, err
			}
		}
	}
	verified := artifact.validated.Evidence.Archive.FullTreeSHA256
	if replacement != nil {
		actual, verifyErr := replacement.Verify(ctx, newEntries)
		if verifyErr != nil || actual != verified {
			return mutationAttempted, invalidAgentStaging()
		}
	}
	state, err = journal.ReadState(ctx)
	if err != nil {
		return mutationAttempted, err
	}
	if !state.Exchanged {
		if err := replacement.Exchange(ctx, oldTree, newEntries,
			state.Pending != nil && state.Pending.Kind == backupvolumefs.MutationExchange, mutationJournal); err != nil {
			return true, err
		}
	}
	state, err = journal.ReadState(ctx)
	if err != nil {
		return true, err
	}
	deletions, err := backupvolumefs.DeletionOrder(oldTree)
	if err != nil {
		return true, err
	}
	for ordinal := state.DeletionCursor + 1; ordinal <= uint64(len(deletions)); ordinal++ {
		if replacement == nil {
			return true, invalidAgentStaging()
		}
		if err := replacement.DeleteOld(ctx, ordinal, deletions[ordinal-1], oldTree.FullTreeSHA256,
			verified, state.Pending != nil && state.Pending.Kind == backupvolumefs.MutationDelete &&
				state.Pending.Ordinal == ordinal, mutationJournal); err != nil {
			return true, err
		}
	}
	recoveryResume := resume
	if resume.Phase < agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY {
		recoveryResume = &agentpb.BackupRestoreResume{Checkpoint: &agentpb.BackupRestoreResume_VolumeProgress{
			VolumeProgress: &agentpb.BackupVolumeProgress{}}}
	}
	if err := pool.recoverBackupVolumeConsumers(ctx, assignment, execution, recoveryResume, publisher); err != nil {
		return true, err
	}
	if err := artifact.stage.Cleanup(ctx); err != nil {
		return true, err
	}
	pool.backupStaging.retireVolumeStage(assignment.TaskID, step, artifact.stage)
	if err := journal.Cleanup(ctx); err != nil {
		return true, err
	}
	if err := receiver.Cleanup(ctx); err != nil {
		return true, err
	}
	if err := pool.backupStaging.retireVolumeMetadata(assignment.TaskID, step); err != nil {
		return true, err
	}
	return true, nil
}

func volumeRestoreCheckpointProvesVerified(resume *agentpb.BackupRestoreResume, consumerCount int) bool {
	progress := resume.GetVolumeProgress()
	if progress == nil || resume.Phase < agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY ||
		progress.PendingConstruction != nil || progress.PendingFinalization != nil ||
		progress.PendingExchange != nil || progress.PendingDelete != nil ||
		len(progress.OldFullTreeSha256) != 32 || progress.DeletionCursor == 0 {
		return false
	}
	if consumerCount == 0 {
		return true
	}
	return progress.ServiceCursor == uint32(consumerCount) &&
		(progress.ServicePhase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_HEALTHY ||
			progress.ServicePhase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTARTED ||
			progress.ServicePhase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_NOT_RUNNING)
}

func volumeRestoreConsumersStopped(resume *agentpb.BackupRestoreResume) bool {
	progress := resume.GetVolumeProgress()
	return progress != nil && (progress.ServicePhase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOPPED &&
		progress.ServiceCursor == 0 || progress.ConstructionCursor != 0 || progress.PendingConstruction != nil ||
		progress.FinalizationCursor != 0 || progress.PendingFinalization != nil ||
		len(progress.OldFullTreeSha256) != 0 || progress.PendingExchange != nil ||
		progress.DeletionCursor != 0 || progress.PendingDelete != nil)
}
