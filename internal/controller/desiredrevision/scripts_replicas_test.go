package desiredrevision

import (
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: reconciliation selects a Script once for its logical Service,
// independent of the Service's operator-authored replica count.
func TestReconcileBlueprintScriptsSelectsOnceForReplicatedLogicalService(t *testing.T) {
	at := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	service := desiredRevisionScriptService(t, environmentID, ids.NewAt(ids.KindService, at, 2), "worker")
	service.Desired.Replicas = 3

	result, err := ReconcileBlueprintScripts(
		environmentID,
		map[string]core.ScriptSpec{
			"release-hook": {Slug: "release", Service: "worker", When: core.ScriptPostDeploy, Script: "printf release"},
		},
		[]etcd.ServiceRecord{service},
		nil,
		BlueprintScriptResources{},
		func(kind ids.Kind, purpose string) string {
			return ids.DeriveAt(kind, at, ids.NewAt(ids.KindTask, at, 3), purpose)
		},
	)
	if err != nil {
		t.Fatalf("ReconcileBlueprintScripts() error = %v", err)
	}
	if len(result.Current) != 1 || len(result.BodyGenerations) != 1 ||
		result.Current[0].ServiceID != service.Desired.ID {
		t.Fatalf("replicated logical Script selection = %#v", result)
	}
	replayed, err := ReconcileBlueprintScripts(
		environmentID,
		map[string]core.ScriptSpec{
			"release-hook": {Slug: "release", Service: "worker", When: core.ScriptPostDeploy, Script: "printf release"},
		},
		[]etcd.ServiceRecord{service},
		result.Current,
		BlueprintScriptResources{},
		func(ids.Kind, string) string {
			t.Fatal("reapplying a replicated Script target allocated a new id")
			return ""
		},
	)
	if err != nil {
		t.Fatalf("ReconcileBlueprintScripts(reapply) error = %v", err)
	}
	if len(replayed.Current) != 1 || replayed.Current[0] != result.Current[0] || len(replayed.BodyGenerations) != 0 {
		t.Fatalf("replayed replicated logical Script selection = %#v", replayed)
	}
}

// Rationale: only positive, operator-owned workload Services are eligible;
// zero/negative desired sets and adapter-managed backing targets remain closed.
func TestReconcileBlueprintScriptsRejectsIneligibleLogicalServices(t *testing.T) {
	at := time.Date(2026, 8, 31, 13, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 10)
	tests := map[string]func(*etcd.ServiceRecord){
		"zero replicas":     func(service *etcd.ServiceRecord) { service.Desired.Replicas = 0 },
		"negative replicas": func(service *etcd.ServiceRecord) { service.Desired.Replicas = -1 },
		"managed backing": func(service *etcd.ServiceRecord) {
			service.Desired.Adapter = "postgres:16"
			service.BackingNetworkID = ids.NewAt(ids.KindNetwork, at, 14)
		},
	}
	for name, change := range tests {
		t.Run(name, func(t *testing.T) {
			service := desiredRevisionScriptService(t, environmentID, ids.NewAt(ids.KindService, at, 11), "api")
			change(&service)
			_, err := ReconcileBlueprintScripts(
				environmentID,
				map[string]core.ScriptSpec{
					"release-hook": {Slug: "release", Service: "api", When: core.ScriptManual, Script: "true"},
				},
				[]etcd.ServiceRecord{service},
				nil,
				BlueprintScriptResources{},
				func(kind ids.Kind, purpose string) string {
					return ids.DeriveAt(kind, at, ids.NewAt(ids.KindTask, at, 12), purpose)
				},
			)
			if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("ReconcileBlueprintScripts() error = %v, want validation.failed", err)
			}
		})
	}
}
