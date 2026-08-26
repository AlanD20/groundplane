package entry

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

func TestPrepareControllerRemovalPlanBindsRestartReproducibleFinalizer(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, time.August, 23, 7, 0, 0, 0, time.UTC)
	request := RemovalPlanRequest{
		TaskID: ids.NewAt(ids.KindTask, at, 3), PlanID: ids.NewAt(ids.KindPlan, at, 5),
		EntryID:       ids.NewAt(ids.KindEnvEntry, at, 1),
		EnvironmentID: ids.NewAt(ids.KindEnvironment, at, 2), EntryRevision: 17, CreatedAt: at,
	}
	prepared, err := prepareControllerRemovalPlan(request)
	if err != nil {
		t.Fatalf("prepareControllerRemovalPlan() error = %v", err)
	}
	if prepared.Executor != RemovalExecutorController || prepared.RenderGeneration != 1 ||
		prepared.TimeoutSeconds != controllerRemovalTimeoutSeconds || len(prepared.Steps) != 1 ||
		ids.Validate(ids.KindStep, prepared.Steps[0].ID) != nil ||
		prepared.EnvironmentID != request.EnvironmentID || len(prepared.PlanHash) != 64 {
		t.Fatalf("prepared Controller Entry removal plan = %#v", prepared)
	}
	again, err := prepareControllerRemovalPlan(request)
	if err != nil || again.PlanHash != prepared.PlanHash {
		t.Fatalf("recreated plan hash = %q, %v; want %q", again.PlanHash, err, prepared.PlanHash)
	}
	changed := request
	changed.EntryRevision++
	later, err := prepareControllerRemovalPlan(changed)
	if err != nil || later.PlanHash == prepared.PlanHash {
		t.Fatalf("later revision plan hash = %q, %v; must differ from %q", later.PlanHash, err, prepared.PlanHash)
	}
}

func TestPrepareControllerRemovalPlanRejectsAppliedProjection(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, time.August, 23, 8, 0, 0, 0, time.UTC)
	request := RemovalPlanRequest{
		TaskID: ids.NewAt(ids.KindTask, at, 2), PlanID: ids.NewAt(ids.KindPlan, at, 3),
		EnvironmentID: ids.NewAt(ids.KindEnvironment, at, 4),
		EntryID:       ids.NewAt(ids.KindEnvEntry, at, 1), EntryRevision: 1,
		ProjectionRevision: 9, CreatedAt: at,
	}
	if _, err := prepareControllerRemovalPlan(request); err == nil {
		t.Fatal("prepareControllerRemovalPlan() error = nil, want applied projection rejection")
	}
}
