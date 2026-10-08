package executionplan

import (
	"bytes"
	"crypto/sha256"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// BackupAssignmentIdentity contains delivery facts, not mutable source state.
// All resource and execution authority comes from the sealed plan.
type BackupAssignmentIdentity struct {
	TaskID           string
	OperationID      string
	AssignmentID     string
	Generation       uint64
	DeadlineUnixNano uint64
}

func BindBackupTaskAuthority(
	plan *agentpb.ExecutionPlan,
	identity BackupAssignmentIdentity,
) (*agentpb.BackupTaskAuthority, []byte, error) {
	validated, err := Validate(plan)
	if err != nil {
		return nil, nil, err
	}
	if !backupOperation(validated.Operation) || validated.BackupScope == nil {
		return nil, nil, errs.New(errs.KindValidationFailed, "backup assignment requires a sealed backup plan")
	}
	scope := validated.BackupScope
	authority := &agentpb.BackupTaskAuthority{
		AuthoritySchema: 1, TaskId: identity.TaskID, OperationId: identity.OperationID,
		TaskAttempt: scope.TaskAttempt, PlanHash: append([]byte(nil), validated.PlanHash...),
		ProjectId: scope.ProjectId, Project: scope.Project, EnvironmentId: scope.EnvironmentId,
		Environment: scope.Environment, TaskDeadlineUnixNano: identity.DeadlineUnixNano,
		AssignmentId: identity.AssignmentID, AssignmentGeneration: identity.Generation, Services: scope.Services,
	}
	for _, step := range validated.Steps {
		if restore := step.GetBackupStep().GetRestore(); restore != nil {
			if original := restore.GetOriginalIdentity(); original != nil {
				if original.OriginalExecutionId != step.GetBackupStep().ExecutionId ||
					original.OriginalAssignmentId != identity.AssignmentID {
					return nil, nil, errs.New(errs.KindStateConflict, "Restore original assignment authority changed")
				}
			} else if adopted := restore.GetAdoptedIdentity(); adopted == nil || adopted.AdoptedAssignmentId != identity.AssignmentID {
				return nil, nil, errs.New(errs.KindStateConflict, "Restore adopted assignment authority changed")
			}
		}
		authority.Steps = append(authority.Steps, step.GetBackupStep())
	}
	digest, err := BackupTaskAuthorityDigest(authority)
	if err != nil {
		return nil, nil, err
	}
	return authority, digest, nil
}

func BackupTaskAuthorityDigest(authority *agentpb.BackupTaskAuthority) ([]byte, error) {
	if err := validateBackupTaskAuthority(authority); err != nil {
		return nil, err
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(authority)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(encoded)
	digest := sha256.New()
	_, _ = digest.Write([]byte("groundplane.backup.task.schema-one.v1\x00"))
	_, _ = digest.Write(encoded)
	return digest.Sum(nil), nil
}

// ValidateBackupAssignment proves that the delivered authority is exactly the
// sealed plan bound to this assignment. It does not trust a separately supplied
// list of resources merely because its digest is internally consistent.
func ValidateBackupAssignment(plan *agentpb.ExecutionPlan, authority *agentpb.BackupTaskAuthority,
	digest []byte, identity BackupAssignmentIdentity,
) (*agentpb.BackupTaskAuthority, error) {
	expected, expectedDigest, err := BindBackupTaskAuthority(plan, identity)
	if err != nil {
		return nil, err
	}
	if len(digest) != sha256.Size || !bytes.Equal(expectedDigest, digest) || !proto.Equal(expected, authority) {
		return nil, errs.New(errs.KindValidationFailed, "backup assignment authority differs from its sealed plan")
	}
	return expected, nil
}

func backupOperation(operation agentpb.PlanOperation) bool {
	return operation == agentpb.PlanOperation_PLAN_OPERATION_BACKUP ||
		operation == agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE ||
		operation == agentpb.PlanOperation_PLAN_OPERATION_RESTORE
}

func validateBackupPlanScope(scope *agentpb.BackupPlanScope, environmentID string) error {
	if scope == nil || ids.Validate(ids.KindProject, scope.ProjectId) != nil ||
		!backupCheckpointRevision(scope.Project) ||
		ids.Validate(ids.KindEnvironment, scope.EnvironmentId) != nil ||
		scope.EnvironmentId != environmentID ||
		!backupCheckpointRevision(scope.Environment) ||
		scope.TaskAttempt == 0 {
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

func validateBackupTaskAuthority(authority *agentpb.BackupTaskAuthority) error {
	if authority == nil || RejectUnknown(authority) != nil || authority.AuthoritySchema != 1 ||
		ids.Validate(
			ids.KindTask,
			authority.TaskId,
		) != nil || ids.Validate(ids.KindOperation, authority.OperationId) != nil ||
		ids.Validate(
			ids.KindAssignment,
			authority.AssignmentId,
		) != nil || authority.TaskAttempt == 0 || authority.AssignmentGeneration == 0 ||
		!backupCheckpointDigest(
			authority.PlanHash,
		) || authority.TaskDeadlineUnixNano == 0 || len(authority.Steps) == 0 || len(authority.Steps) > MaximumBackupSources {
		return errs.New(errs.KindValidationFailed, "backup Task authority is invalid")
	}
	scope := &agentpb.BackupPlanScope{ProjectId: authority.ProjectId, Project: authority.Project,
		EnvironmentId: authority.EnvironmentId, Environment: authority.Environment, Services: authority.Services, TaskAttempt: authority.TaskAttempt}
	if err := validateBackupPlanScope(scope, authority.EnvironmentId); err != nil {
		return err
	}
	serviceIDs := make([]string, len(authority.Services))
	for i, service := range authority.Services {
		serviceIDs[i] = service.ServiceId
	}
	seen := make(map[string]bool, len(authority.Steps))
	for _, step := range authority.Steps {
		if _, err := ValidateBackupStepAuthority(step); err != nil {
			return err
		}
		if seen[step.StepId] || seen[step.ExecutionId] || step.StepDeadlineUnixNano > authority.TaskDeadlineUnixNano {
			return errs.New(errs.KindValidationFailed, "backup Task step identity or deadline is invalid")
		}
		seen[step.StepId], seen[step.ExecutionId] = true, true
		for _, serviceID := range step.ConsumerServiceIds {
			if !slices.Contains(serviceIDs, serviceID) {
				return errs.New(errs.KindValidationFailed, "backup consumer has no sealed Service fact")
			}
		}
		postgresID := step.GetCapture().GetPostgres().GetDatabaseServiceId()
		if step.GetRestore().GetPostgres() != nil {
			postgresID = step.GetRestore().GetPostgres().GetDatabaseServiceId()
		}
		if postgresID != "" && !slices.Contains(serviceIDs, postgresID) {
			return errs.New(errs.KindValidationFailed, "backup database has no sealed Service fact")
		}
		mysqlID := step.GetCapture().GetMysql().GetDatabaseServiceId()
		if step.GetRestore().GetMysql() != nil {
			mysqlID = step.GetRestore().GetMysql().GetDatabaseServiceId()
		}
		if mysqlID != "" && !slices.Contains(serviceIDs, mysqlID) {
			return errs.New(errs.KindValidationFailed, "backup database has no sealed Service fact")
		}
	}
	return nil
}
