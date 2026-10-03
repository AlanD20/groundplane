package agent

import (
	"context"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/backupformat"
	"github.com/AlanD20/groundplane/internal/common/backupvolume"
	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/internal/infra/agentvolumemanifest"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (pool *WorkerPool) receiveBackupVolumeRestoreNew(ctx context.Context,
	assignment taskassignment.Assignment, step *agentpb.BackupStepAuthority,
) (_ *agentvolumemanifest.Journal, _ []backupvolume.Entry, _ []backupvolume.ManifestEntryBytes, resultErr error) {
	if pool.backupVolumes == nil || step.GetRestore().GetVolume() == nil ||
		len(step.StepDigest) != sha256.Size || len(step.GetRestore().ExpectedEvidence.SourceSha256) != sha256.Size {
		return nil, nil, nil, invalidAgentStaging()
	}
	direction := agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_NEW
	transferID, err := backupvolumetransfer.TransferID(step.ExecutionId, direction)
	if err != nil {
		return nil, nil, nil, err
	}
	binding, err := pool.volumeBinding(assignment.TaskID, assignment.AssignmentID, step.StepId, transferID, direction)
	if err != nil {
		return nil, nil, nil, err
	}
	archive, err := backupvolumetransfer.ArchiveEvidenceFromWire(step.GetRestore().GetVolume().Archive)
	if err != nil {
		return nil, nil, nil, err
	}
	source := backupformat.Evidence{SizeBytes: step.GetRestore().ExpectedEvidence.SourceSizeBytes}
	copy(source.SHA256[:], step.GetRestore().ExpectedEvidence.SourceSha256)
	journal, err := agentvolumemanifest.Open(ctx, agentvolumemanifest.Config{Root: agentvolumemanifest.AgentRoot,
		Binding: binding, PointID: step.GetRestore().PointId, RestoreGenerationID: step.ExecutionId,
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
	credit, err := journal.CurrentCredit(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	if err := pool.publishBackupVolumeCredit(ctx, credit); err != nil {
		return nil, nil, nil, err
	}
	pool.backupVolumes.mu.Lock()
	frames := pool.backupVolumes.frames[transferID]
	pool.backupVolumes.mu.Unlock()
	if frames == nil {
		return nil, nil, nil, errs.New(errs.KindStateConflict, "Volume Restore manifest frame slot is unavailable")
	}
	for credit.NextOrdinal <= archive.EntryCount+1 {
		// End is the one record after all entries. A completed receiver reads
		// its retained proof directly without accepting another frame.
		if credit.CommittedRecordSequence > 0 {
			if entries, manifest, readErr := journal.ReadComplete(ctx); readErr == nil {
				keep = true
				return journal, entries, manifest, nil
			}
		}
		var frame *agentpb.BackupVolumeManifestTransfer
		select {
		case <-ctx.Done():
			return nil, nil, nil, ctx.Err()
		case frame = <-frames:
		}
		credit, err = journal.AcceptFrame(ctx, frame)
		if err != nil {
			return nil, nil, nil, err
		}
		if err := pool.publishBackupVolumeCredit(ctx, credit); err != nil {
			return nil, nil, nil, err
		}
		if frame.GetEnd() != nil {
			break
		}
	}
	entries, manifest, err := journal.ReadComplete(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	keep = true
	return journal, entries, manifest, nil
}
