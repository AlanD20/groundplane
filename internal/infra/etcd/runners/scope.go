package runners

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

func (desired RunnerDesiredRecord) ProjectID() string {
	if desired.OwnerKind == RunnerOwnerProject {
		return desired.OwnerID
	}
	return desired.ParentProjectID
}

func (desired RunnerDesiredRecord) EnvironmentID() string {
	if desired.OwnerKind == RunnerOwnerEnvironment {
		return desired.OwnerID
	}
	return ""
}

func TaskOwner(desired RunnerDesiredRecord) (taskjournal.TaskOwner, error) {
	if err := ValidateRunnerOwnership(desired); err != nil {
		return taskjournal.TaskOwner{}, err
	}
	owner := taskjournal.TaskOwner{
		WorkspaceType: taskjournal.TaskWorkspaceTenant,
		TenantID:      desired.TenantID, ProjectID: desired.ProjectID(), EnvironmentID: desired.EnvironmentID(),
	}
	if err := taskjournal.ValidateOwner(owner); err != nil {
		return taskjournal.TaskOwner{}, err
	}
	return owner, nil
}

func (desired RunnerDesiredRecord) IdempotencyScope() idempotency.IdempotencyScopeKind {
	switch desired.OwnerKind {
	case RunnerOwnerProject:
		return idempotency.IdempotencyScopeProject
	case RunnerOwnerEnvironment:
		return idempotency.IdempotencyScopeEnvironment
	default:
		return idempotency.IdempotencyScopeTenant
	}
}
