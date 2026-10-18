package backupruntime

import (
	"bytes"
	"context"
	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/common/backupvolume"
	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentvolumejournal"
	"github.com/AlanD20/groundplane/internal/infra/backupvolumefs"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"time"
)

func restoreVolume(ctx context.Context, assignment *agentpb.BackupWorkAssignment, identity workexecution.Identity,
	ordinal uint32, staging *Staging, volumeRoot string, consumers BackupConsumers,
	delivery *checkpointdelivery.Backup, exchange Exchange,
) (mutationAttempted bool, resultErr error) {
	binding, err := backupvolumetransfer.Bind(assignment, identity, ordinal, agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_NEW)
	if err != nil {
		return false, err
	}
	oldBinding, err := backupvolumetransfer.Bind(assignment, identity, ordinal, agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_OLD)
	if err != nil {
		return false, err
	}
	step := assignment.Input.Steps[ordinal]
	publisher, err := delivery.Source(ordinal)
	if err != nil {
		return false, err
	}
	cursor, err := publisher.Current(ctx)
	if err != nil {
		return false, err
	}
	resume := cursor.GetRestore()
	if resume == nil || staging == nil || consumers == nil {
		return false, errs.New(errs.KindStateConflict, "Volume Restore execution resources are missing")
	}
	mutationAttempted = resume.Phase >= agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_MUTATION ||
		resume.GetVolumeProgress() != nil
	ctx, cancel := context.WithDeadline(ctx, time.Unix(0, int64(step.StepDeadlineUnixNano)))
	defer cancel()
	if volumeRestoreCheckpointProvesVerified(resume, len(step.ConsumerServiceIds)) {
		return true, cleanupCompletedVolumeRestore(ctx, assignment, identity, ordinal, resume, staging, volumeRoot)
	}
	receiver, newEntries, newManifest, err := receiveVolumeRestoreManifest(ctx, assignment, identity, ordinal, exchange)
	if err != nil {
		return mutationAttempted, err
	}
	defer func() {
		if closeErr := receiver.Close(); closeErr != nil {
			resultErr = errs.WrapJoined(errs.KindStorageUnavailable, resultErr, closeErr)
		}
	}()
	artifact, err := downloadVolumeRestore(ctx, assignment, identity, ordinal, staging, exchange, newEntries, newManifest)
	if err != nil {
		return mutationAttempted, err
	}
	if resume.CheckpointSequence == 0 {
		if err := publisher.RestoreArtifactValidated(ctx, artifact.proof); err != nil {
			return mutationAttempted, err
		}
	} else if validated := resume.GetArtifactValidated(); validated != nil && !proto.Equal(validated, artifact.proof) {
		return mutationAttempted, errs.New(errs.KindStateConflict, "Volume Restore filesystem differs from its captured journal")
	}
	volumeAuthority := step.GetRestore().GetVolume().Projection
	volume, err := backupvolumefs.Open(ctx, volumeRoot,
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
		count, digest, err := stopVolumeConsumers(ctx, consumers, assignment, ordinal, resume, publisher)
		if err != nil {
			return mutationAttempted, err
		}
		old, err := volume.Snapshot(ctx)
		if err != nil {
			return mutationAttempted, err
		}
		if err := sendVolumeManifest(ctx, oldBinding, step,
			agentpb.BackupVolumeManifestRole_BACKUP_VOLUME_MANIFEST_ROLE_STOPPED_LIVE_OLD,
			old.Entries, nil, exchange); err != nil {
			return mutationAttempted, err
		}
		if err := publisher.Volume(ctx, &agentpb.BackupVolumeCheckpoint{Checkpoint: &agentpb.BackupVolumeCheckpoint_ConsumersStopped{ConsumersStopped: &agentpb.BackupConsumersStopped{
			StoppedCount: count, ServiceProgressSha256: digest[:]}}}); err != nil {
			return mutationAttempted, err
		}
	}
	newManifestSHA, err := backupvolume.ContentManifestSHA256(newManifest)
	if err != nil || !bytes.Equal(newManifestSHA[:], step.GetRestore().GetVolume().Archive.ContentManifestSha256) {
		return mutationAttempted, errs.New(errs.KindStateConflict, "Volume Restore filesystem differs from its captured journal")
	}
	journalConfig := agentvolumejournal.Config{JournalRoot: agentvolumejournal.AgentRoot,
		Binding: binding,
		PointID: step.GetRestore().PointId, RestoreGenerationID: step.ExecutionId,
		NewManifestSHA256: newManifestSHA,
		NewFullTreeSHA256: artifact.validated.Evidence.Archive.FullTreeSHA256,
		NewEntryCount:     uint64(len(newEntries)),
		VolumeRoot:        volumeRoot, AuthorizedVolumeDir: volumeAuthority.AuthorizedVolumeDir,
		ComposeKey: volumeAuthority.ComposeVolumeKey, SiblingName: ".gp-restore-" + step.ExecutionId}
	journalConfig, existingJournal, err := findVolumeRestoreJournal(ctx, journalConfig, resume)
	if err != nil {
		return mutationAttempted, err
	}
	var oldTree backupvolumefs.Tree
	if !existingJournal {
		journalConfig, oldTree, err = captureVolumeJournalBaseline(ctx, volume, journalConfig)
		if err != nil {
			return mutationAttempted, err
		}
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
		return mutationAttempted, errs.New(errs.KindStateConflict, "Volume Restore filesystem differs from its captured journal")
	}
	if err := reconcileVolumeRestoreJournal(ctx, state, mutationJournal, resume); err != nil {
		return mutationAttempted, err
	}
	cursor, err = publisher.Current(ctx)
	if err != nil {
		return mutationAttempted, err
	}
	resume = cursor.GetRestore()
	if resume == nil {
		return mutationAttempted, errs.New(errs.KindStateConflict, "Volume Restore lost its acknowledged cursor")
	}
	if err := resumeVolumeReplacement(ctx, volume, journalConfig, journal, state, existingJournal,
		oldTree, newEntries, artifact, mutationJournal); err != nil {
		return true, err
	}
	recoveryResume := resume
	if resume.Phase < agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY {
		recoveryResume = &agentpb.BackupRestoreResume{Checkpoint: &agentpb.BackupRestoreResume_VolumeProgress{
			VolumeProgress: &agentpb.BackupVolumeProgress{}}}
	}
	if err := recoverVolumeConsumers(ctx, consumers, assignment, ordinal, recoveryResume, publisher); err != nil {
		return true, err
	}
	if err := staging.Cleanup(ctx, artifact.staging); err != nil {
		return true, err
	}
	if err := journal.Cleanup(ctx); err != nil {
		return true, err
	}
	if err := receiver.Cleanup(ctx); err != nil {
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
