package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// AdvanceBackupVolumeCapture requires the completed manifest owner's exact
// archive evidence at Prepared. The Controller must fence that manifest in the
// same transaction; an Agent's archive claim alone is not manifest completion.
func AdvanceBackupVolumeCapture(assignment *agentpb.BackupWorkAssignment,
	request *agentpb.BackupWorkCheckpointRequest, authenticated workexecution.Identity,
	manifest *agentpb.BackupVolumeArchiveEvidence,
) (*agentpb.BackupStepResume, error) {
	owned, _, err := OpenBackupWorkAssignment(assignment, authenticated)
	if err != nil {
		return nil, err
	}
	report, err := ValidateBackupWorkCheckpoint(request, owned.Input, authenticated)
	if err != nil {
		return nil, err
	}
	if !proto.Equal(report.Attempt, owned.Attempt) {
		return nil, invalidBackupCheckpoint("Volume capture report belongs to another attempt")
	}
	step := owned.Input.Steps[report.SourceOrdinal]
	current := owned.Sources[report.SourceOrdinal].GetCapture()
	volume := step.GetCapture().GetVolume()
	if current == nil || volume == nil || current.CheckpointSequence == ^uint64(0) ||
		report.CheckpointSequence != current.CheckpointSequence+1 || !proto.Equal(report.PrecedingCheckpoint, current.PrecedingCheckpoint) {
		return nil, invalidBackupCheckpoint("Volume capture report does not follow saved progress")
	}
	next := proto.CloneOf(current)
	if prepared := report.GetArtifactPrepared(); prepared != nil {
		if current.CheckpointSequence != 0 || manifest == nil || RejectUnknown(manifest) != nil ||
			!proto.Equal(prepared.GetVolume(), manifest) || prepared.PointId != step.GetCapture().PointId ||
			!backupResumePreparedSourceMatches(prepared, step.GetCapture()) ||
			prepared.Evidence.SourceSizeBytes > volume.SourceSizeUpperBound {
			return nil, invalidBackupCheckpoint("Volume artifact differs from its completed manifest or size bound")
		}
		next.PreparedArtifact = proto.CloneOf(prepared)
		next.Phase = agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_ARTIFACT_PREPARED
		next.Checkpoint = &agentpb.BackupCaptureResume_ArtifactPrepared{ArtifactPrepared: prepared}
	} else if err := advanceBackupCapturedObject(next, current, step, report); err != nil {
		return nil, err
	}
	next.CheckpointSequence, next.PrecedingCheckpoint = report.CheckpointSequence, nil
	return &agentpb.BackupStepResume{StepId: step.StepId, ExecutionId: step.ExecutionId,
		Operation: &agentpb.BackupStepResume_Capture{Capture: next}}, nil
}
