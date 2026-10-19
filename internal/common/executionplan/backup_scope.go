package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func backupOperation(operation agentpb.PlanOperation) bool {
	return operation == agentpb.PlanOperation_PLAN_OPERATION_BACKUP ||
		operation == agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE ||
		operation == agentpb.PlanOperation_PLAN_OPERATION_RESTORE
}

func validateBackupPlanScope(scope *agentpb.BackupPlanScope, environmentID string) error {
	if scope == nil || scope.TaskAttempt == 0 {
		return errs.New(errs.KindValidationFailed, "backup plan attempt is invalid")
	}
	return validateBackupResourceScope(scope, environmentID)
}

func validateBackupResourceScope(scope *agentpb.BackupPlanScope, environmentID string) error {
	if scope == nil || ids.Validate(ids.KindProject, scope.ProjectId) != nil ||
		!backupCheckpointRevision(scope.Project) ||
		ids.Validate(ids.KindEnvironment, scope.EnvironmentId) != nil ||
		scope.EnvironmentId != environmentID ||
		!backupCheckpointRevision(scope.Environment) {
		return errs.New(errs.KindValidationFailed, "backup plan scope is invalid")
	}
	previous := ""
	for _, service := range scope.Services {
		if !validBackupServiceFact(service) || service.ServiceId <= previous {
			return errs.New(errs.KindValidationFailed, "backup Service facts must be valid and uniquely ordered")
		}
		previous = service.ServiceId
	}
	return nil
}

func validBackupServiceFact(service *agentpb.BackupServiceFact) bool {
	if service == nil || ids.Validate(ids.KindService, service.ServiceId) != nil || service.CurrentName == "" ||
		!backupCheckpointRevision(
			service.Service,
		) || !backupCheckpointRevision(service.Compose) || service.PriorRuntimeIntent == nil ||
		service.RequiredLabelCount == 0 || !backupCheckpointDigest(service.RequiredLabelsSha256) || !backupCheckpointDigest(service.LocalImageIdSha256) {
		return false
	}
	intent := service.PriorRuntimeIntent
	switch intent.Kind {
	case agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING,
		agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_STOPPED:
		return backupCheckpointRevision(intent.Intent)
	case agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_ABSENT:
		return intent.Intent == nil
	default:
		return false
	}
}
