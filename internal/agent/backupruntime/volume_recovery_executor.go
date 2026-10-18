package backupruntime

import (
	"context"

	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/common/backupvolume"
	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/internal/infra/agentvolumejournal"
	"github.com/AlanD20/groundplane/internal/infra/backupvolumefs"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (executor *Executor) recoverVolumeFilesystem(ctx context.Context, assignment *agentpb.BackupRecoveryAssignment,
	attempt workexecution.Attempt, source executionplan.BackupWorkRecovery, ownership *agentownership.Lease,
	staging *Staging, publisher *checkpointdelivery.BackupSource, exchange Exchange,
) (_ *agentpb.BackupStepResume, resultErr error) {
	if staging == nil || publisher == nil || ownership == nil {
		return nil, invalidDatabaseStaging()
	}
	if err := ownership.CheckRecovery(source.Forward, attempt); err != nil {
		return nil, err
	}
	cursor, err := publisher.Current(ctx)
	if err != nil {
		return nil, err
	}
	step, resume := assignment.Input.Steps[0], cursor.GetRestore()
	if volumeRestoreCheckpointProvesVerified(resume, len(step.ConsumerServiceIds)) ||
		resume.GetPhase() >= agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY {
		return cursor, nil
	}
	binding, err := backupvolumetransfer.BindRecoverySource(assignment, attempt.Executor, 0,
		agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_NEW)
	if err != nil {
		return nil, err
	}
	receiver, entries, manifest, err := receiveVolumeRecoveryManifest(ctx, assignment, attempt.Executor, 0)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := receiver.Close(); err != nil {
			resultErr = errs.WrapJoined(errs.KindStorageUnavailable, resultErr, err)
		}
	}()
	manifestSHA, err := backupvolume.ContentManifestSHA256(manifest)
	if err != nil {
		return nil, err
	}
	projection := step.GetRestore().GetVolume().Projection
	archive := step.GetRestore().GetVolume().Archive
	config := agentvolumejournal.Config{JournalRoot: agentvolumejournal.AgentRoot,
		Binding: binding, PointID: step.GetRestore().PointId, RestoreGenerationID: step.ExecutionId,
		NewManifestSHA256: manifestSHA, NewEntryCount: uint64(len(entries)),
		VolumeRoot: executor.volumeRoot, AuthorizedVolumeDir: projection.AuthorizedVolumeDir,
		ComposeKey: projection.ComposeVolumeKey, SiblingName: ".gp-restore-" + step.ExecutionId}
	copy(config.NewFullTreeSHA256[:], archive.FullTreeSha256)
	config, found, err := findVolumeRestoreJournal(ctx, config, resume)
	if err != nil {
		return nil, err
	}
	staged, err := staging.RestoreRecovery(ctx, assignment, attempt.Executor, 0)
	if err != nil {
		return nil, err
	}
	artifact, err := downloadCapturedVolumeRestore(ctx, assignment.Input, assignment.Attempt, 0,
		staged, exchange, entries, manifest)
	if err != nil {
		return nil, err
	}
	volume, err := backupvolumefs.Open(ctx, executor.volumeRoot, projection.AuthorizedVolumeDir, projection.ComposeVolumeKey)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := volume.Close(); err != nil {
			resultErr = errs.WrapJoined(errs.KindStorageUnavailable, resultErr, err)
		}
	}()
	var oldTree backupvolumefs.Tree
	if !volumeRestoreConsumersStopped(resume) {
		stopped, digest, err := executor.finishVolumeConsumerStop(ctx, assignment, attempt.Executor, publisher)
		if err != nil {
			return nil, err
		}
		if err := ownership.CheckRecovery(source.Forward, attempt); err != nil {
			return nil, err
		}
		oldTree, err = volume.Snapshot(ctx)
		if err != nil {
			return nil, err
		}
		oldBinding, err := backupvolumetransfer.BindRecoverySource(assignment, attempt.Executor, 0,
			agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_OLD)
		if err != nil {
			return nil, err
		}
		if err := sendVolumeManifest(ctx, oldBinding, step,
			agentpb.BackupVolumeManifestRole_BACKUP_VOLUME_MANIFEST_ROLE_STOPPED_LIVE_OLD,
			oldTree.Entries, nil, exchange); err != nil {
			return nil, err
		}
		if err := publisher.Volume(ctx, &agentpb.BackupVolumeCheckpoint{Checkpoint: &agentpb.BackupVolumeCheckpoint_ConsumersStopped{
			ConsumersStopped: &agentpb.BackupConsumersStopped{StoppedCount: stopped, ServiceProgressSha256: digest[:]}}}); err != nil {
			return nil, err
		}
		cursor, err = publisher.Current(ctx)
		if err != nil {
			return nil, err
		}
		resume = cursor.GetRestore()
	}
	if !found {
		// findVolumeRestoreJournal rejects missing journals after any filesystem
		// intent. Before the first intent, recheck stopped consumers and capture
		// the live tree exactly as forward execution does.
		host, err := NewBackupConsumerHost(executor.journal, executor.helper, attempt.Executor, executor.ownership)
		if err != nil {
			return nil, err
		}
		for _, serviceID := range step.ConsumerServiceIds {
			observed, _, err := host.RunRecovery(ctx, assignment, 0, serviceID,
				agentpb.BackupConsumerOperation_BACKUP_CONSUMER_OPERATION_UNSPECIFIED, true)
			if err != nil {
				return nil, err
			}
			if volumeObservedRunning(observed, serviceID) {
				return nil, errs.New(errs.KindStateConflict, "Volume consumer is running before filesystem recovery")
			}
		}
		config, oldTree, err = captureVolumeJournalBaseline(ctx, volume, config)
		if err != nil {
			return nil, err
		}
	}
	journal, err := agentvolumejournal.Open(ctx, config)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := journal.Close(); err != nil {
			resultErr = errs.WrapJoined(errs.KindStorageUnavailable, resultErr, err)
		}
	}()
	state, err := journal.ReadState(ctx)
	if err != nil {
		return nil, err
	}
	mutationJournal := &volumeRestoreCheckpointJournal{local: journal, publisher: publisher,
		pointID: step.GetRestore().PointId, generationID: step.ExecutionId, newManifest: manifestSHA}
	if err := reconcileVolumeRestoreJournal(ctx, state, mutationJournal, resume); err != nil {
		return nil, err
	}
	if err := ownership.CheckRecovery(source.Forward, attempt); err != nil {
		return nil, err
	}
	if err := resumeVolumeReplacement(ctx, volume, config, journal, state, found, oldTree,
		entries, artifact, mutationJournal); err != nil {
		return nil, err
	}
	return publisher.Current(ctx)
}
