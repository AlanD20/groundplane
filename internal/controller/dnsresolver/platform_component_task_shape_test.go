package dnsresolver

import (
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testplatformcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

func TestComponentTaskEnsureServiceUsesSealedProcedureShape(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		count     int
		want      bool
		wantError bool
	}{
		{name: "in-place update", count: 2},
		{name: "service ensure", count: 4, want: true},
		{name: "invalid", count: 3, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			task := etcd.TaskRecord{Steps: make([]testtaskjournal.TaskStepRecord, test.count)}
			got, err := componentTaskEnsureService(task)
			if (err != nil) != test.wantError || got != test.want {
				t.Fatalf("componentTaskEnsureService() = %t, %v", got, err)
			}
		})
	}
}

// Rationale: enable, update, and disable publication all persist the generic
// execution hash only after the final registered render input is sealed.
func TestFinalizePlatformComponentTaskBindsEveryOperatorProcedure(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0).UTC()
	componentID := ids.NewAt(ids.KindComponent, now, 1)
	for _, test := range []struct {
		name           string
		ensureService  bool
		disableService bool
	}{
		{name: "enable", ensureService: true},
		{name: "update", ensureService: true},
		{name: "disable", disableService: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			task := newPlatformComponentLifecycleTask(
				componentID, "operator-plan-hash-0001", now, test.ensureService, test.disableService,
			)
			input := testplatformcomponents.PlatformComponentTaskRenderInput{
				TaskID: task.ID, PlanID: task.PlanID, ComponentID: task.Target,
				ExecutionPlanSHA256: strings.Repeat("a", 64),
			}
			finalized, err := finalizePlatformComponentTask(task, input)
			if err != nil || finalized.PlanHash != input.ExecutionPlanSHA256 {
				t.Fatalf("finalizePlatformComponentTask() = %q, %v", finalized.PlanHash, err)
			}
		})
	}
}
