package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// AdvanceBackupVolumeRestore records the next captured filesystem or consumer
// transition. Pending mutations survive reassignment: completion must match the
// saved intent rather than select a new tree or consumer from live state.
func AdvanceBackupVolumeRestore(assignment *agentpb.BackupWorkAssignment,
	request *agentpb.BackupWorkCheckpointRequest, authenticated workexecution.Identity,
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
		return nil, invalidBackupCheckpoint("Volume restore report belongs to another attempt")
	}
	return advanceVolumeRestoreSource(owned.Input, owned.Sources, report)
}

func advanceVolumeRestoreSource(input *agentpb.BackupWorkInput, sources []*agentpb.BackupStepResume,
	report *agentpb.BackupWorkCheckpointRequest,
) (*agentpb.BackupStepResume, error) {
	step := input.Steps[report.SourceOrdinal]
	current := sources[report.SourceOrdinal].GetRestore()
	if current == nil || step.GetRestore().GetVolume() == nil ||
		current.CheckpointSequence == ^uint64(0) || report.CheckpointSequence != current.CheckpointSequence+1 ||
		!proto.Equal(report.PrecedingCheckpoint, current.PrecedingCheckpoint) {
		return nil, invalidBackupCheckpoint("Volume restore report does not follow saved progress")
	}
	next := proto.CloneOf(current)
	switch {
	case report.GetRestoreArtifactValidated() != nil:
		value := report.GetRestoreArtifactValidated()
		if current.CheckpointSequence != 0 || !backupResumeRestoreArtifactMatches(value, step.GetRestore()) {
			return nil, invalidBackupCheckpoint("Volume restore artifact differs from the captured Recovery Point")
		}
		next.Phase = agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_PREPARATION
		next.Checkpoint = &agentpb.BackupRestoreResume_ArtifactValidated{ArtifactValidated: value}
	case report.GetVolume() != nil:
		if err := advanceBackupVolumeRestore(step, next, input.Services, report.GetVolume()); err != nil {
			return nil, invalidBackupCheckpoint("Volume restore transition does not match its saved filesystem or consumer progress")
		}
	default:
		return nil, invalidBackupCheckpoint("report is not a Volume restore transition")
	}
	next.CheckpointSequence, next.PrecedingCheckpoint = report.CheckpointSequence, nil
	return &agentpb.BackupStepResume{StepId: step.StepId, ExecutionId: step.ExecutionId,
		Operation: &agentpb.BackupStepResume_Restore{Restore: next}}, nil
}
