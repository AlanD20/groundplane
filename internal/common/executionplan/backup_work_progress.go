package executionplan

import "github.com/AlanD20/groundplane/proto/agentpb"

// InitialBackupSourceResumes is used only at admission, never as a fallback for
// missing execution state. Restore starts at artifact validation, not mutation.
func InitialBackupSourceResumes(input *agentpb.BackupWorkInput) ([]*agentpb.BackupStepResume, error) {
	if err := ValidateBackupWorkInput(input); err != nil {
		return nil, err
	}
	sources := make([]*agentpb.BackupStepResume, len(input.Steps))
	for index, step := range input.Steps {
		resume := &agentpb.BackupStepResume{StepId: step.StepId, ExecutionId: step.ExecutionId}
		switch input.Operation {
		case agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_CAPTURE:
			resume.Operation = &agentpb.BackupStepResume_Capture{Capture: &agentpb.BackupCaptureResume{
				Phase:  agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_CAPTURING,
				Cursor: &agentpb.BackupCaptureCursor{ObjectAttempt: 1}}}
		case agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_RESTORE:
			resume.Operation = &agentpb.BackupStepResume_Restore{Restore: &agentpb.BackupRestoreResume{
				Phase:  agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_ARTIFACT_VALIDATION,
				Cursor: &agentpb.BackupRestoreCursor{ObjectAttempt: 1}}}
		case agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_PRUNE:
			if len(step.GetPrune().GetObjects()) != 1 {
				return nil, invalidBackupCheckpoint("prune source requires one captured object")
			}
			resume.Operation = &agentpb.BackupStepResume_Prune{Prune: &agentpb.BackupPruneResume{
				NextObjectOrdinal: step.GetPrune().Objects[0].Ordinal}}
		}
		sources[index] = resume
	}
	if err := ValidateBackupSourceResumes(sources, input.Steps); err != nil {
		return nil, err
	}
	return sources, nil
}
