package blueprintrelease

import (
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestSealedCandidateSelectsRunningChangedSingletonsInDependencyOrder(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	databaseID := ids.NewAt(ids.KindService, at, 2)
	apiID := ids.NewAt(ids.KindService, at, 3)
	workerID := ids.NewAt(ids.KindService, at, 4)
	stoppedID := ids.NewAt(ids.KindService, at, 5)
	current := func(id, name, image string, intent core.ServiceRuntimeIntent) *etcd.Versioned[etcd.ServiceRecord] {
		record, err := etcd.NewServiceRecord(environmentID, core.Service{
			ID: id, Name: name, Image: image, Strategy: core.StrategyRecreate, Replicas: 1,
		}, "")
		if err != nil {
			t.Fatal(err)
		}
		record.Runtime.RuntimeIntent = intent
		versioned := etcd.Versioned[etcd.ServiceRecord]{Record: record, Revision: 7, ReadRevision: 9}
		return &versioned
	}
	change := func(existing *etcd.Versioned[etcd.ServiceRecord], image string) etcd.EnvironmentBlueprintServiceChange {
		desired := existing.Record.Desired
		desired.Image = image
		record, err := etcd.ReplaceServiceDesired(existing.Record, desired)
		if err != nil {
			t.Fatal(err)
		}
		return etcd.EnvironmentBlueprintServiceChange{Current: existing, Record: record}
	}
	database, err := etcd.NewServiceRecord(environmentID, core.Service{
		ID: databaseID, Name: "database", Image: "registry.example/database:v1",
		Strategy: core.StrategyRecreate, Replicas: 1,
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	api := current(apiID, "api", "registry.example/api:v1", core.ServiceRuntimeIntentRunning)
	worker := current(workerID, "worker", "registry.example/worker:v1", core.ServiceRuntimeIntentRunning)
	stopped := current(stoppedID, "stopped", "registry.example/stopped:v1", core.ServiceRuntimeIntentStopped)
	changes := []etcd.EnvironmentBlueprintServiceChange{
		change(api, "registry.example/api:v2"),
		{Record: database},
		change(worker, "registry.example/worker:v2"),
		change(stopped, "registry.example/stopped:v2"),
	}
	projection := etcd.EnvironmentComposeProjection{
		EnvironmentID: environmentID,
		ServiceDependencyPlans: core.ServiceDependencyPlans{
			DeployDependencyPlan: core.ServiceDependencyPhasePlan{
				Phase:           core.ServiceLifecycleDeploy,
				OrderedServices: []string{"database", "api", "stopped", "worker"},
				Edges: []core.ServiceDependencyEdge{{
					Service: "api", Dependency: "database",
					Condition: core.ServiceDependencyStarted,
				}},
			},
		},
	}

	selected, err := selectCandidates(projection, changes, map[string]struct{}{workerID: {}})
	if err != nil {
		t.Fatalf("selectCandidates() error = %v", err)
	}
	if len(selected) != 2 || selected[0].Record.Desired.ID != databaseID || selected[1].Record.Desired.ID != apiID {
		t.Fatalf("selected candidates = %#v", selected)
	}
}

func TestPostDeployHookBoundsRejectBeforePublication(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		executions int
		bodyBytes  uint64
	}{
		{name: "seventeenth execution", executions: 17, bodyBytes: 17},
		{name: "aggregate body", executions: 16, bodyBytes: 1<<20 + 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validatePostDeployHookBounds(test.executions, test.bodyBytes)
			if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("validatePostDeployHookBounds() error = %v, want validation.failed", err)
			}
		})
	}
}

func TestPostDeployScriptSelectionIncludesNewServiceAndExcludesManual(t *testing.T) {
	t.Parallel()
	serviceID := "svc_new"
	selected := postDeployScriptsByService([]etcd.ScriptRecord{
		{
			ServiceID: serviceID,
			Desired:   core.Script{ID: "scr_post", Slug: "migrate", When: core.ScriptPostDeploy},
		},
		{
			ServiceID: serviceID,
			Desired:   core.Script{ID: "scr_manual", Slug: "manual", When: core.ScriptManual},
		},
	})
	if len(selected) != 1 || len(selected[serviceID]) != 1 ||
		selected[serviceID][0].Desired.ID != "scr_post" {
		t.Fatalf("selected post-deploy Scripts = %#v", selected)
	}
}
