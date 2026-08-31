package dnsresolver

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

func TestStartupResolverTaskSealsAutomaticProcedure(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name          string
		ensureService bool
		wantSteps     int
	}{
		{name: "update", wantSteps: 2},
		{name: "ensure", ensureService: true, wantSteps: 4},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			task := startupResolverTask(
				"cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
				time.Date(2026, time.August, 30, 12, 0, 0, 0, time.UTC),
				test.ensureService,
			)
			if len(task.Steps) != test.wantSteps {
				t.Fatalf("len(Steps) = %d, want %d", len(task.Steps), test.wantSteps)
			}
			if len(task.Params) != 2 || task.Params[etcd.TaskResourceKindParam] != etcd.TaskResourceComponent ||
				task.Params[etcd.TaskAutomaticReconcileParam] != "true" {
				t.Fatalf("Params = %#v, want sealed automatic Component shape", task.Params)
			}
			if !etcd.IsAutomaticReconcileTask(task) {
				t.Fatal("startup Task is not recognized as automatic reconciliation")
			}
		})
	}
}
