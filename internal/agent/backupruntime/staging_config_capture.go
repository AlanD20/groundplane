package backupruntime

import (
	"bytes"
	"context"
	"math"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (state *Staging) ConfigCapture(ctx context.Context, assignment *agentpb.BackupWorkAssignment,
	identity workexecution.Identity, ordinal uint32, resume *agentpb.BackupCaptureResume,
) (CaptureStage, error) {
	owned, attempt, err := executionplan.OpenBackupWorkAssignment(assignment, identity)
	if err != nil {
		return CaptureStage{}, err
	}
	if state == nil || uint64(ordinal) >= uint64(len(owned.Input.Steps)) || resume == nil {
		return CaptureStage{}, errs.New(errs.KindStateConflict, "Config capture source is unavailable")
	}
	step := owned.Input.Steps[ordinal]
	content := step.GetCapture().GetConfig().GetContent()
	journalGrowth, err := backupconfigtransfer.JournalGrowthUpperBound(content)
	if err != nil {
		return CaptureStage{}, err
	}
	storedSize, err := backupconfig.AgeStoredSize(content.SourceSizeBytes)
	if err != nil {
		return CaptureStage{}, err
	}
	if content.SourceSizeBytes > math.MaxUint64-storedSize ||
		content.SourceSizeBytes+storedSize > math.MaxUint64-journalGrowth {
		return CaptureStage{}, errs.New(
			errs.KindStateConflict,
			"Config capture staging reservation exceeds its size limit",
		)
	}
	ids := backupstage.IDs{Task: attempt.TaskID, Step: step.ExecutionId, Point: step.GetCapture().PointId}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closed || state.active[ids] != nil {
		return CaptureStage{}, errs.New(errs.KindStateConflict, "Config staging is closed or already in use")
	}
	result := CaptureStage{IDs: ids}
	if retained := state.prepared[ids]; retained != nil {
		for _, file := range retained.Files {
			if file.Artifact == nil {
				return CaptureStage{}, errs.New(errs.KindStateConflict, "Recovered Config file descriptor is missing")
			}
			switch file.Evidence.Name {
			case executionplan.BackupSourceStagingFinal:
				if result.Source != nil || file.Evidence.Size > content.SourceSizeBytes {
					return CaptureStage{}, errs.New(
						errs.KindStateConflict,
						"Recovered Config source exceeds its captured size",
					)
				}
				result.Source, result.SourceEvidence = file.Artifact, file.Evidence
			case executionplan.BackupStoredStagingFinal:
				if result.Stored != nil || file.Evidence.Size != storedSize {
					return CaptureStage{}, errs.New(
						errs.KindStateConflict,
						"Recovered Config object has an invalid size",
					)
				}
				result.Stored = file.Artifact
			default:
				return CaptureStage{}, errs.New(
					errs.KindStateConflict,
					"Recovered Config stage contains an unexpected file",
				)
			}
			if prepared := resume.GetPreparedArtifact(); prepared != nil {
				evidence := prepared.GetEvidence()
				if evidence == nil || prepared.PointId != ids.Point {
					return CaptureStage{}, errs.New(
						errs.KindStateConflict,
						"Config capture lacks acknowledged artifact evidence",
					)
				}
				size, digest := evidence.SourceSizeBytes, evidence.SourceSha256
				if file.Evidence.Name == executionplan.BackupStoredStagingFinal {
					size, digest = evidence.StoredSizeBytes, evidence.StoredSha256
				}
				if size != file.Evidence.Size || !bytes.Equal(digest, file.Evidence.SHA256[:]) {
					return CaptureStage{}, errs.New(
						errs.KindStateConflict,
						"Recovered Config bytes differ from acknowledged evidence",
					)
				}
			}
		}
		if result.Source == nil || resume.GetPreparedArtifact() != nil && result.Stored == nil {
			return CaptureStage{}, errs.New(
				errs.KindStateConflict,
				"Recovered Config capture is missing required files",
			)
		}
		result.Stage = retained.Stage
		state.active[ids] = retained.Stage
		return result, nil
	}
	if resume.GetPreparedArtifact() != nil || resume.GetConfigProgress().GetTransfer() != nil {
		return CaptureStage{}, errs.New(errs.KindStateConflict, "Completed Config transfer has no retained source")
	}
	stage, err := state.stager.Prepare(ctx, ids, backupstage.Bounded(content.SourceSizeBytes+storedSize+journalGrowth))
	if err != nil {
		return CaptureStage{}, err
	}
	state.active[ids] = stage
	result.Stage = stage
	result.Source, err = stage.CreateFile(ctx, executionplan.BackupSourceStagingFinal)
	if err != nil {
		return result, err
	}
	result.SourceEvidence, err = result.Source.SyncPartial(ctx)
	return result, err
}
