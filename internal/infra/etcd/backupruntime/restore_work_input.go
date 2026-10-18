package backupruntime

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ValidateRestoreWorkInput binds a native operation directly to the selected
// Recovery Point and captured surviving target. It does not manufacture an
// ExecutionPlan or invoke generic Task validation.
func ValidateRestoreWorkInput(record BackupRestoreRecord, input *agentpb.BackupWorkInput) error {
	if err := executionplan.ValidateBackupWorkInput(input); err != nil {
		return err
	}
	if ValidateBackupRestoreRecord(record) != nil ||
		input.Operation != agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_RESTORE ||
		input.EnvironmentId != record.EnvironmentID || len(input.Steps) != 1 {
		return invalidBackupRuntimeRecord("Restore work input differs from the selected Recovery Point")
	}
	scope := &agentpb.BackupPlanScope{ProjectId: input.ProjectId, Project: input.Project,
		EnvironmentId: input.EnvironmentId, Environment: input.Environment, Services: input.Services}
	step := input.Steps[0]
	switch record.Point.SourceKind {
	case BackupRuntimeSourceConfig:
		return validateConfigRestoreInput(record, scope, step)
	case BackupRuntimeSourceVolume:
		if len(input.Artifacts) == 0 {
			return invalidBackupRuntimeRecord("Volume Restore work has no captured artifacts")
		}
		return validateVolumeRestoreInput(record, scope, step)
	case BackupRuntimeSourceAttach:
		return validateDatabaseRestoreInput(record, scope, step, input.Artifacts)
	default:
		return invalidBackupRuntimeRecord("Restore source kind is unsupported")
	}
}
