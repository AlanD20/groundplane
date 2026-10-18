package backupruntime

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentvolumejournal"
	"github.com/AlanD20/groundplane/internal/infra/agentvolumemanifest"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Only a source whose durable checkpoints prove replacement and consumer
// recovery may enter this path. No download, service action or tree mutation.
func cleanupCompletedVolumeRestore(ctx context.Context, assignment *agentpb.BackupWorkAssignment,
	identity workexecution.Identity, ordinal uint32, resume *agentpb.BackupRestoreResume,
	staging *Staging, volumeRoot string,
) error {
	binding, err := backupvolumetransfer.Bind(assignment, identity, ordinal, agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_NEW)
	if err != nil {
		return err
	}
	step := assignment.Input.Steps[ordinal]
	if !volumeRestoreCheckpointProvesVerified(resume, len(step.ConsumerServiceIds)) {
		return errs.New(errs.KindStateConflict, "Volume Restore has not finished replacement and consumer recovery")
	}
	absent, err := staging.SourceAbsent(assignment, identity, ordinal)
	if err != nil {
		return err
	}
	if !absent {
		staged, err := staging.Restore(ctx, assignment, identity, ordinal)
		if err != nil {
			return err
		}
		if err := staging.Cleanup(ctx, staged); err != nil {
			return err
		}
	}
	return cleanupCompletedVolumeRestoreEvidence(ctx, binding, step, resume, volumeRoot)
}

func cleanupCompletedVolumeRestoreEvidence(ctx context.Context, binding backupvolumetransfer.Binding,
	step *agentpb.BackupStepAuthority, resume *agentpb.BackupRestoreResume, volumeRoot string,
) error {
	if !volumeRestoreCheckpointProvesVerified(resume, len(step.ConsumerServiceIds)) {
		return errs.New(errs.KindStateConflict, "Volume cleanup lacks completed replacement and consumer recovery")
	}
	row, found, err := agentvolumejournal.Inspect(ctx, agentvolumejournal.AgentRoot, step.ExecutionId)
	if err != nil {
		return err
	}
	if found {
		config := row.Config
		projection := step.GetRestore().GetVolume().Projection
		archive := step.GetRestore().GetVolume().Archive
		if row.Unbound || row.Discarding || config.Binding != binding || config.PointID != step.GetRestore().PointId ||
			config.RestoreGenerationID != step.ExecutionId || config.VolumeRoot != volumeRoot ||
			config.AuthorizedVolumeDir != projection.AuthorizedVolumeDir || config.ComposeKey != projection.ComposeVolumeKey ||
			config.SiblingName != ".gp-restore-"+step.ExecutionId || config.NewEntryCount != archive.EntryCount ||
			!bytes.Equal(config.NewManifestSHA256[:], archive.ContentManifestSha256) ||
			!bytes.Equal(config.NewFullTreeSHA256[:], archive.FullTreeSha256) ||
			!bytes.Equal(config.OldFullTreeSHA256[:], resume.GetVolumeProgress().OldFullTreeSha256) {
			return errs.New(errs.KindStateConflict, "Volume Restore cleanup journal differs from completed source")
		}
		if row.Retiring {
			if err := agentvolumejournal.ResumeCleanup(ctx, config); err != nil {
				return err
			}
		} else {
			journal, err := agentvolumejournal.Open(ctx, config)
			if err != nil {
				return err
			}
			cleanupErr := journal.Cleanup(ctx)
			closeErr := journal.Close()
			if cleanupErr != nil || closeErr != nil {
				return errs.WrapJoined(errs.KindStorageUnavailable, cleanupErr, closeErr)
			}
		}
	}
	return cleanupCompletedVolumeManifest(ctx, binding, step)
}

func cleanupCompletedVolumeManifest(ctx context.Context, binding backupvolumetransfer.Binding, step *agentpb.BackupStepAuthority) error {
	rows, err := agentvolumemanifest.Inventory(ctx, agentvolumemanifest.AgentRoot)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if row.DirectoryName != binding.TransferID && row.DirectoryName != binding.TransferID+".retiring" {
			continue
		}
		config := row.Config
		archive := step.GetRestore().GetVolume().Archive
		evidence := step.GetRestore().ExpectedEvidence
		if row.Unbound || row.Discarding || config.Binding != binding || config.PointID != step.GetRestore().PointId || config.RestoreGenerationID != step.ExecutionId ||
			config.ExpectedArchive.EntryCount != archive.EntryCount ||
			!bytes.Equal(config.ExpectedArchive.ContentManifestSHA256[:], archive.ContentManifestSha256) ||
			!bytes.Equal(config.ExpectedArchive.FullTreeSHA256[:], archive.FullTreeSha256) ||
			config.ExpectedSource.SizeBytes != evidence.SourceSizeBytes || !bytes.Equal(config.ExpectedSource.SHA256[:], evidence.SourceSha256) {
			return errs.New(errs.KindStateConflict, "Volume Restore cleanup manifest differs from completed source")
		}
		if row.Retiring {
			return agentvolumemanifest.ResumeCleanup(ctx, config)
		}
		if !row.Complete || row.Partial {
			return errs.New(errs.KindStateConflict, "Completed Volume Restore has an incomplete manifest")
		}
		journal, err := agentvolumemanifest.Open(ctx, config)
		if err != nil {
			return err
		}
		cleanupErr := journal.Cleanup(ctx)
		closeErr := journal.Close()
		if cleanupErr != nil || closeErr != nil {
			return errs.WrapJoined(errs.KindStorageUnavailable, cleanupErr, closeErr)
		}
		return nil
	}
	return nil
}
