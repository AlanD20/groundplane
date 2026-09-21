package taskjournal

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// TaskWorkspaceType is the immutable workspace catalog stored with every Task.
type TaskWorkspaceType string

const (
	TaskWorkspacePlatform TaskWorkspaceType = "platform"
	TaskWorkspaceTenant   TaskWorkspaceType = "tenant"
)

// TaskActor records whether an authenticated operator or the Controller
// initiated one Task attempt. It deliberately contains no token identity.
type TaskActor string

const (
	TaskActorOperator TaskActor = "operator"
	TaskActorSystem   TaskActor = "system"
)

// TaskOwner freezes the initiating product scope. Empty descendant ids are
// meaningful and therefore remain explicit strings rather than pointers.
type TaskOwner struct {
	WorkspaceType TaskWorkspaceType `json:"workspace_type"`
	TenantID      string            `json:"tenant_id,omitempty"`
	ProjectID     string            `json:"project_id,omitempty"`
	EnvironmentID string            `json:"environment_id,omitempty"`
}

func PlatformTaskOwner() TaskOwner {
	return TaskOwner{WorkspaceType: TaskWorkspacePlatform}
}

func TenantTaskOwner(tenantID string) (TaskOwner, error) {
	owner := TaskOwner{WorkspaceType: TaskWorkspaceTenant, TenantID: tenantID}
	if err := ValidateOwner(owner); err != nil {
		return TaskOwner{}, err
	}
	return owner, nil
}

func TenantProjectTaskOwner(tenantID string, projectID string) (TaskOwner, error) {
	owner := TaskOwner{
		WorkspaceType: TaskWorkspaceTenant,
		TenantID:      tenantID,
		ProjectID:     projectID,
	}
	if err := ValidateOwner(owner); err != nil {
		return TaskOwner{}, err
	}
	return owner, nil
}

func ProjectTaskOwner(project hierarchyrecord.ProjectRecord) (TaskOwner, error) {
	if ids.Validate(ids.KindProject, project.ID) != nil {
		return TaskOwner{}, errs.New(errs.KindValidationFailed, "task owner Project is invalid")
	}
	var owner TaskOwner
	switch project.Kind {
	case hierarchyrecord.ProjectKindTenant:
		owner = TaskOwner{
			WorkspaceType: TaskWorkspaceTenant,
			TenantID:      project.TenantID,
			ProjectID:     project.ID,
		}
	case hierarchyrecord.ProjectKindBacking:
		if project.TenantID != "" {
			return TaskOwner{}, errs.New(errs.KindValidationFailed, "backing Project task owner has a Tenant")
		}
		owner = TaskOwner{WorkspaceType: TaskWorkspacePlatform, ProjectID: project.ID}
	default:
		return TaskOwner{}, errs.New(errs.KindValidationFailed, "task owner Project kind is invalid")
	}
	if err := ValidateOwner(owner); err != nil {
		return TaskOwner{}, err
	}
	return owner, nil
}

func EnvironmentTaskOwner(project hierarchyrecord.ProjectRecord, environment hierarchyrecord.EnvironmentRecord) (TaskOwner, error) {
	if ids.Validate(ids.KindEnvironment, environment.ID) != nil || environment.ProjectID != project.ID {
		return TaskOwner{}, errs.New(errs.KindValidationFailed, "task owner Environment hierarchy is invalid")
	}
	owner, err := ProjectTaskOwner(project)
	if err != nil {
		return TaskOwner{}, err
	}
	owner.EnvironmentID = environment.ID
	if err := ValidateOwner(owner); err != nil {
		return TaskOwner{}, err
	}
	return owner, nil
}

func ValidateOwner(owner TaskOwner) error {
	switch owner.WorkspaceType {
	case TaskWorkspacePlatform:
		if owner.TenantID != "" {
			return errs.New(errs.KindValidationFailed, "platform task owner cannot contain a Tenant")
		}
	case TaskWorkspaceTenant:
		if ids.Validate(ids.KindTenant, owner.TenantID) != nil {
			return errs.New(errs.KindValidationFailed, "tenant task owner requires a valid Tenant")
		}
	default:
		return errs.New(errs.KindValidationFailed, "task workspace_type is invalid")
	}
	if owner.ProjectID != "" && ids.Validate(ids.KindProject, owner.ProjectID) != nil {
		return errs.New(errs.KindValidationFailed, "task owner Project is invalid")
	}
	if owner.EnvironmentID != "" {
		if owner.ProjectID == "" || ids.Validate(ids.KindEnvironment, owner.EnvironmentID) != nil {
			return errs.New(errs.KindValidationFailed, "task owner Environment requires a valid Project")
		}
	}
	return nil
}

func ValidActor(actor TaskActor) bool {
	return actor == TaskActorOperator || actor == TaskActorSystem
}
