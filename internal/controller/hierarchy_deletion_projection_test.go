package controller

import (
	"testing"

	environmentcapability "github.com/AlanD20/groundplane/internal/controller/environment"
	"github.com/AlanD20/groundplane/internal/core"
)

func TestHierarchyAPIsExposeDeletionTaskID(t *testing.T) {
	taskID := "task_01K3D7R40G0000000000000000"
	tenant := tenantAPI(core.Tenant{DeletionTaskID: &taskID})
	project := projectAPI(core.Project{DeletionTaskID: &taskID})
	environment := environmentReadResponse(environmentcapability.Environment{DeletionTaskID: &taskID})
	if tenant.DeletionTaskID == nil || *tenant.DeletionTaskID != taskID {
		t.Fatalf("Tenant deletion_task_id = %#v", tenant.DeletionTaskID)
	}
	if project.DeletionTaskID == nil || *project.DeletionTaskID != taskID {
		t.Fatalf("Project deletion_task_id = %#v", project.DeletionTaskID)
	}
	if environment.DeletionTaskID == nil || *environment.DeletionTaskID != taskID {
		t.Fatalf("Environment deletion_task_id = %#v", environment.DeletionTaskID)
	}
}
