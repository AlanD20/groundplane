package services

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

func TestPrepareControllerServiceLifecycleTaskBindsNeverAppliedIntent(t *testing.T) {
	// Rationale: a Service absent from the current applied projection still
	// needs an observable durable Task without dispatching unnecessary host work.
	t.Parallel()
	at := time.Date(2026, time.August, 23, 12, 30, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	task := etcd.TaskRecord{
		ID: ids.NewAt(ids.KindTask, at, 2), OperationID: ids.NewAt(ids.KindOperation, at, 3),
		PlanID: ids.NewAt(ids.KindPlan, at, 4), Type: testtaskjournal.TaskStop,
		Target: ids.NewAt(ids.KindService, at, 5), Status: testtaskjournal.TaskStatusPending,
		NextEventSequence: 1, CreatedAt: at,
	}
	prepared, err := prepareControllerServiceLifecycleTask(task, environmentID, 17)
	if err != nil || prepared.Executor != testtaskjournal.TaskExecutorController || prepared.RenderGeneration != 1 ||
		prepared.TimeoutSeconds != serviceLifecycleControlTimeoutSeconds || len(prepared.Steps) != 1 ||
		len(
			prepared.Params,
		) != 2 || prepared.Params[testtaskjournal.TaskResourceKindParam] != testtaskjournal.TaskResourceService ||
		prepared.Params[testtaskjournal.TaskServiceEnvironmentParam] != environmentID || len(prepared.PlanHash) != 64 {
		t.Fatalf("prepareControllerServiceLifecycleTask() = %#v, %v", prepared, err)
	}
	again, err := prepareControllerServiceLifecycleTask(task, environmentID, 17)
	if err != nil || again.PlanHash != prepared.PlanHash {
		t.Fatalf("deterministic Controller plan hash = %q / %q, %v", prepared.PlanHash, again.PlanHash, err)
	}
}

func TestServiceInComposeProjectionUsesDesiredAndAddressedGeneratedServices(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, time.August, 23, 13, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	desiredID := ids.NewAt(ids.KindService, at, 2)
	generatedID := ids.NewAt(ids.KindService, at, 3)
	transitionalID := ids.NewAt(ids.KindService, at, 4)
	projection := testenvironmentprojection.EnvironmentComposeProjection{
		DesiredServices: []testservices.EnvironmentServiceProjection{{
			EnvironmentID: environmentID, Desired: core.Service{ID: desiredID, Name: "api"},
		}},
		Components: []testcomponents.Record{{
			Runtime: testcomponents.RuntimeRecord{GeneratedServices: []string{generatedID}},
		}},
	}
	if !serviceInComposeProjection(projection, desiredID) || !serviceInComposeProjection(projection, generatedID) ||
		serviceInComposeProjection(projection, transitionalID) {
		t.Fatalf("service membership did not follow authoritative desired topology")
	}
}
