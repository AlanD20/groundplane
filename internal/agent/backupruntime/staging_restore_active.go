package backupruntime

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Reuse only published descriptors of the source owned by the excluded forward
// executor. An incomplete source is not permission to overwrite its files.
func (state *Staging) activeRestoreSource(
	ctx context.Context,
	taskID string,
	step *agentpb.BackupStepAuthority,
) (CaptureStage, bool, error) {
	ids := backupstage.IDs{Task: taskID, Step: step.ExecutionId, Point: step.GetRestore().PointId}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closed {
		return CaptureStage{}, false, errs.New(errs.KindStateConflict, "Restore staging is closed")
	}
	stage := state.active[ids]
	if stage == nil {
		return CaptureStage{}, false, nil
	}
	key, err := executionplan.BackupSourceStagingKey(ids.Task, ids.Step, ids.Point)
	if err != nil || state.sourceRecovery[string(key)].GetRecoveryRequired() != nil {
		return CaptureStage{}, true, errs.New(
			errs.KindStateConflict,
			"Restore staging requires its startup disposition",
		)
	}
	source, sourceEvidence, err := stage.PublishedArtifact(ctx, executionplan.BackupSourceStagingFinal)
	if err != nil {
		return CaptureStage{}, true, err
	}
	expected := step.GetRestore().ExpectedEvidence
	if sourceEvidence.Size != expected.SourceSizeBytes ||
		!bytes.Equal(sourceEvidence.SHA256[:], expected.SourceSha256) {
		return CaptureStage{}, true, errs.New(errs.KindStateConflict, "Restore source differs from captured evidence")
	}
	stored, storedEvidence := source, sourceEvidence
	if step.GetRestore().Encryption.Kind != agentpb.BackupEncryption_BACKUP_ENCRYPTION_NONE {
		stored, storedEvidence, err = stage.PublishedArtifact(ctx, executionplan.BackupStoredStagingFinal)
		if err != nil {
			return CaptureStage{}, true, err
		}
	}
	if storedEvidence.Size != expected.StoredSizeBytes ||
		!bytes.Equal(storedEvidence.SHA256[:], expected.StoredSha256) {
		return CaptureStage{}, true, errs.New(
			errs.KindStateConflict,
			"Restore stored object differs from captured evidence",
		)
	}
	return CaptureStage{
		IDs:            ids,
		Stage:          stage,
		Source:         source,
		SourceEvidence: sourceEvidence,
		Stored:         stored,
	}, true, nil
}
