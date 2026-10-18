package backupruntime

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/backupformat"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (state *Staging) DatabaseCapture(ctx context.Context, assignment *agentpb.BackupWorkAssignment,
	identity workexecution.Identity, ordinal uint32, resume *agentpb.BackupCaptureResume,
) (CaptureStage, error) {
	owned, attempt, err := executionplan.OpenBackupWorkAssignment(assignment, identity)
	if err != nil {
		return CaptureStage{}, err
	}
	if state == nil || uint64(ordinal) >= uint64(len(owned.Input.Steps)) || resume == nil {
		return CaptureStage{}, errs.New(errs.KindStateConflict, "Database capture source is unavailable")
	}
	step := owned.Input.Steps[ordinal]
	maximum := step.GetCapture().GetPostgres().GetMaxPlaintextBytes()
	if mysql := step.GetCapture().GetMysql(); mysql != nil {
		maximum = mysql.MaxPlaintextBytes
	}
	if maximum == 0 || maximum > backupformat.MaxStoredBytes {
		return CaptureStage{}, errs.New(errs.KindStateConflict, "Database capture size limit is invalid")
	}
	ids := backupstage.IDs{Task: attempt.TaskID, Step: step.ExecutionId, Point: step.GetCapture().PointId}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closed || state.active[ids] != nil {
		return CaptureStage{}, errs.New(errs.KindStateConflict, "Database staging is closed or already in use")
	}
	result := CaptureStage{IDs: ids}
	if retained := state.prepared[ids]; retained != nil {
		for _, file := range retained.Files {
			if file.Artifact == nil {
				return CaptureStage{}, errs.New(errs.KindStateConflict, "Recovered database file descriptor is missing")
			}
			switch file.Evidence.Name {
			case executionplan.BackupSourceStagingFinal:
				if result.Source != nil || file.Evidence.Size > maximum {
					return CaptureStage{}, errs.New(errs.KindStateConflict, "Recovered database source exceeds its captured limit")
				}
				result.Source, result.SourceEvidence = file.Artifact, file.Evidence
			case executionplan.BackupStoredStagingFinal:
				if result.Stored != nil {
					return CaptureStage{}, errs.New(errs.KindStateConflict, "Recovered database object is duplicated")
				}
				result.Stored = file.Artifact
			default:
				return CaptureStage{}, errs.New(errs.KindStateConflict, "Recovered database stage contains an unexpected file")
			}
			if prepared := resume.GetPreparedArtifact(); prepared != nil {
				evidence := prepared.GetEvidence()
				if evidence == nil || prepared.PointId != ids.Point {
					return CaptureStage{}, errs.New(errs.KindStateConflict, "Database capture lacks acknowledged artifact evidence")
				}
				size, digest := evidence.SourceSizeBytes, evidence.SourceSha256
				if file.Evidence.Name == executionplan.BackupStoredStagingFinal {
					size, digest = evidence.StoredSizeBytes, evidence.StoredSha256
				}
				if file.Evidence.Size != size || !bytes.Equal(file.Evidence.SHA256[:], digest) {
					return CaptureStage{}, errs.New(errs.KindStateConflict, "Recovered database bytes differ from acknowledged evidence")
				}
			}
		}
		if result.Source == nil {
			return CaptureStage{}, errs.New(errs.KindStateConflict, "Recovered database source is missing")
		}
		if prepared := resume.GetPreparedArtifact(); prepared != nil {
			if prepared.GetFinals().GetSameInode() {
				if result.Stored != nil {
					return CaptureStage{}, errs.New(errs.KindStateConflict, "Unencrypted database capture contains an extra object")
				}
				result.Stored = result.Source
			} else if result.Stored == nil {
				return CaptureStage{}, errs.New(errs.KindStateConflict, "Prepared database capture is missing its stored object")
			}
		}
		result.Stage = retained.Stage
		state.active[ids] = retained.Stage
		return result, nil
	}
	if resume.GetDumpStart() != nil || resume.GetMysqlDumpStart() != nil || resume.GetPreparedArtifact() != nil {
		return CaptureStage{}, errs.New(errs.KindStateConflict, "Started database capture has no retained source; refusing to repeat the dump")
	}
	// A format limit is not a size estimate. Reserve writes as the dump grows.
	stage, err := state.stager.Prepare(ctx, ids, backupstage.ExclusiveUnknown())
	if err != nil {
		return CaptureStage{}, err
	}
	state.active[ids] = stage
	result.Stage = stage
	result.Source, err = stage.CreateFile(ctx, executionplan.BackupSourceStagingFinal)
	return result, err
}
