package app

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

func TestPrepareControllerRouteRemovalTaskBindsExactFinalizer(t *testing.T) {
	// Rationale: a Route that never reached an enabled Caddy projection must
	// still use the durable deletion lifecycle without dispatching host work.
	t.Parallel()
	at := time.Date(2026, time.August, 23, 3, 0, 0, 0, time.UTC)
	routeID := ids.NewAt(ids.KindRoute, at, 1)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 2)
	task := etcd.TaskRecord{
		ID: ids.NewAt(ids.KindTask, at, 3), OperationID: ids.NewAt(ids.KindOperation, at, 4),
		PlanID: ids.NewAt(ids.KindPlan, at, 5), Type: etcd.TaskRemove, Target: routeID,
		Status: etcd.TaskStatusPending, NextEventSequence: 1, CreatedAt: at,
	}
	intent, err := etcd.NewRouteRemovalIntent(task.ID, environmentID, routeID, 17, nil, at)
	if err != nil {
		t.Fatalf("NewRouteRemovalIntent() error = %v", err)
	}
	prepared, err := prepareControllerRouteRemovalTask(task, intent)
	if err != nil {
		t.Fatalf("prepareControllerRouteRemovalTask() error = %v", err)
	}
	if prepared.Executor != etcd.TaskExecutorController || prepared.RenderGeneration != 1 ||
		prepared.TimeoutSeconds != routeRemovalControllerTimeoutSeconds || len(prepared.Steps) != 1 ||
		ids.Validate(ids.KindStep, prepared.Steps[0].ID) != nil || len(prepared.Params) != 2 ||
		prepared.Params[etcd.TaskResourceKindParam] != etcd.TaskResourceRoute ||
		prepared.Params[etcd.TaskRouteEnvironmentParam] != environmentID || len(prepared.PlanHash) != 64 {
		t.Fatalf("prepared Controller Route removal Task = %#v", prepared)
	}
	again, err := controllerRouteRemovalPlanHash(intent)
	if err != nil || again != prepared.PlanHash {
		t.Fatalf("controllerRouteRemovalPlanHash() = %q, %v; want %q", again, err, prepared.PlanHash)
	}
}
