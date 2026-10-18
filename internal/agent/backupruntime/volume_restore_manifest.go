package backupruntime

import (
	"context"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/backupformat"
	"github.com/AlanD20/groundplane/internal/common/backupvolume"
	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentvolumemanifest"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func receiveVolumeRestoreManifest(ctx context.Context, assignment *agentpb.BackupWorkAssignment,
	identity workexecution.Identity, ordinal uint32, exchange VolumeExchange,
) (_ *agentvolumemanifest.Journal, _ []backupvolume.Entry, _ []backupvolume.ManifestEntryBytes, resultErr error) {
	binding, err := backupvolumetransfer.Bind(assignment, identity, ordinal,
		agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_NEW)
	if err != nil {
		return nil, nil, nil, err
	}
	step := assignment.Input.Steps[ordinal]
	return readVolumeRestoreManifest(ctx, binding, step, exchange)
}

func receiveVolumeRecoveryManifest(ctx context.Context, assignment *agentpb.BackupRecoveryAssignment,
	identity workexecution.Identity, ordinal uint32,
) (*agentvolumemanifest.Journal, []backupvolume.Entry, []backupvolume.ManifestEntryBytes, error) {
	binding, err := backupvolumetransfer.BindRecoverySource(assignment, identity, ordinal,
		agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_NEW)
	if err != nil {
		return nil, nil, nil, err
	}
	return readVolumeRestoreManifest(ctx, binding, assignment.Input.Steps[ordinal], nil)
}

func readVolumeRestoreManifest(ctx context.Context, binding backupvolumetransfer.Binding,
	step *agentpb.BackupStepAuthority, exchange VolumeExchange,
) (_ *agentvolumemanifest.Journal, _ []backupvolume.Entry, _ []backupvolume.ManifestEntryBytes, resultErr error) {
	restore := step.GetRestore()
	if len(restore.GetExpectedEvidence().GetSourceSha256()) != sha256.Size {
		return nil, nil, nil, errs.New(errs.KindStateConflict, "Volume Restore source evidence is missing")
	}
	archive, err := backupvolumetransfer.ArchiveEvidenceFromWire(restore.GetVolume().Archive)
	if err != nil {
		return nil, nil, nil, err
	}
	source := backupformat.Evidence{SizeBytes: restore.ExpectedEvidence.SourceSizeBytes}
	copy(source.SHA256[:], restore.ExpectedEvidence.SourceSha256)
	journal, err := agentvolumemanifest.Open(ctx, agentvolumemanifest.Config{Root: agentvolumemanifest.AgentRoot,
		Binding: binding, PointID: restore.PointId, RestoreGenerationID: step.ExecutionId,
		ExpectedArchive: archive, ExpectedSource: source})
	if err != nil {
		return nil, nil, nil, err
	}
	keep := false
	defer func() {
		if !keep {
			if closeErr := journal.Close(); closeErr != nil {
				resultErr = errs.WrapJoined(errs.KindStorageUnavailable, resultErr, closeErr)
			}
		}
	}()
	// Completion is read with the durable credit. A failed ReadComplete is an
	// error, not permission to wait for another frame from a completed sender.
	credit, complete, err := journal.Position(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	if exchange == nil {
		if !complete {
			return nil, nil, nil, errs.New(errs.KindStateConflict, "Volume recovery requires its completed captured manifest")
		}
	} else {
		if err := exchange.SendVolumeCredit(ctx, binding, credit); err != nil {
			return nil, nil, nil, err
		}
	}
	for !complete {
		frame, err := exchange.ReceiveVolumeFrame(ctx, binding)
		if err != nil {
			return nil, nil, nil, err
		}
		credit, err = journal.AcceptFrame(ctx, frame)
		if err != nil {
			return nil, nil, nil, err
		}
		if err := exchange.SendVolumeCredit(ctx, binding, credit); err != nil {
			return nil, nil, nil, err
		}
		complete = frame.GetEnd() != nil
	}
	entries, manifest, err := journal.ReadComplete(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	keep = true
	return journal, entries, manifest, nil
}
