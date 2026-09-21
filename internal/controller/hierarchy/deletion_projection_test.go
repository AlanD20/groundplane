package hierarchy

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func TestDeletionTaskIDSurvivesHierarchyPersistenceConversion(t *testing.T) {
	taskID := "task_01K3D7R40G0000000000000000"
	tenant := tenantFromEtcd(
		testkeyvalue.Versioned[testhierarchy.TenantRecord]{Record: testhierarchy.TenantRecord{DeletionTaskID: taskID}},
	)
	project := projectFromEtcd(
		testkeyvalue.Versioned[testhierarchy.ProjectRecord]{
			Record: testhierarchy.ProjectRecord{DeletionTaskID: taskID},
		},
	)
	if tenant.Record.DeletionTaskID == nil || *tenant.Record.DeletionTaskID != taskID {
		t.Fatalf("Tenant deletion_task_id = %#v", tenant.Record.DeletionTaskID)
	}
	if project.Record.DeletionTaskID == nil || *project.Record.DeletionTaskID != taskID {
		t.Fatalf("Project deletion_task_id = %#v", project.Record.DeletionTaskID)
	}
	if stored := tenantToEtcd(core.Tenant{DeletionTaskID: &taskID}); stored.DeletionTaskID != taskID {
		t.Fatalf("stored Tenant deletion_task_id = %q", stored.DeletionTaskID)
	}
	if stored := projectToEtcd(core.Project{DeletionTaskID: &taskID}); stored.DeletionTaskID != taskID {
		t.Fatalf("stored Project deletion_task_id = %q", stored.DeletionTaskID)
	}
}
