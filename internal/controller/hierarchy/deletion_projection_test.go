package hierarchy

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	etcdinfra "github.com/AlanD20/groundplane/internal/infra/etcd"
)

func TestDeletionTaskIDSurvivesHierarchyPersistenceConversion(t *testing.T) {
	taskID := "task_01K3D7R40G0000000000000000"
	tenant := tenantFromEtcd(
		etcdinfra.Versioned[etcdinfra.TenantRecord]{Record: etcdinfra.TenantRecord{DeletionTaskID: taskID}},
	)
	project := projectFromEtcd(
		etcdinfra.Versioned[etcdinfra.ProjectRecord]{Record: etcdinfra.ProjectRecord{DeletionTaskID: taskID}},
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
