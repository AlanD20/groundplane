package app

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

// Rationale: an Entry that never reached an applied projection must still use
// the durable deletion lifecycle while dispatching only the exact Controller
// finalizer whose acknowledgement owns metadata and generation removal.
func TestPrepareControllerEntryRemovalTaskBindsExactFinalizer(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, time.August, 23, 7, 0, 0, 0, time.UTC)
	entryID := ids.NewAt(ids.KindEnvEntry, at, 1)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 2)
	task := etcd.TaskRecord{
		ID: ids.NewAt(ids.KindTask, at, 3), OperationID: ids.NewAt(ids.KindOperation, at, 4),
		PlanID: ids.NewAt(ids.KindPlan, at, 5), Type: etcd.TaskRemove, Target: entryID,
		Status: etcd.TaskStatusPending, NextEventSequence: 1, CreatedAt: at,
	}
	intent, err := etcd.NewEntryRemovalIntent(task.ID, environmentID, entryID, 17, nil, at)
	if err != nil {
		t.Fatalf("NewEntryRemovalIntent() error = %v", err)
	}
	prepared, err := prepareControllerEntryRemovalTask(task, intent)
	if err != nil {
		t.Fatalf("prepareControllerEntryRemovalTask() error = %v", err)
	}
	if prepared.Executor != etcd.TaskExecutorController || prepared.RenderGeneration != 1 ||
		prepared.TimeoutSeconds != entryRemovalControllerTimeoutSeconds || len(prepared.Steps) != 1 ||
		ids.Validate(ids.KindStep, prepared.Steps[0].ID) != nil || len(prepared.Params) != 2 ||
		prepared.Params[etcd.TaskResourceKindParam] != etcd.TaskResourceEntry ||
		prepared.Params[etcd.TaskEntryEnvironmentParam] != environmentID || len(prepared.PlanHash) != 64 {
		t.Fatalf("prepared Controller Entry removal Task = %#v", prepared)
	}
	again, err := controllerEntryRemovalPlanHash(intent)
	if err != nil || again != prepared.PlanHash {
		t.Fatalf("controllerEntryRemovalPlanHash() = %q, %v; want %q", again, err, prepared.PlanHash)
	}
}
