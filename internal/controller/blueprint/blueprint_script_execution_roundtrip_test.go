package blueprint

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	testcomposeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	testreleasegroup "github.com/AlanD20/groundplane/internal/controller/releasegroup"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testentries "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	groupstore "github.com/AlanD20/groundplane/internal/infra/etcd/releasegroup"
	testscripts "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
)

// Rationale: the real Blueprint read path must export a human-edited context,
// using current resource keys, that parses and reconciles to the same authority.
func TestGetBlueprintScriptExecutionReconcilesWithoutChangingGeneration(t *testing.T) {
	at := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	tenantID, projectID := ids.NewAt(ids.KindTenant, at, 1), ids.NewAt(ids.KindProject, at, 2)
	environmentID, revisionID := ids.NewAt(ids.KindEnvironment, at, 3), ids.NewAt(ids.KindTask, at, 4)
	serviceID, volumeID, entryID := ids.NewAt(
		ids.KindService,
		at,
		5,
	), ids.NewAt(
		ids.KindVolume,
		at,
		6,
	), ids.NewAt(
		ids.KindEnvEntry,
		at,
		7,
	)
	service, err := testservices.NewServiceRecord(
		environmentID,
		core.Service{ID: serviceID, Name: "web", Image: "example/web:1", Replicas: 1},
		"",
	)
	if err != nil {
		t.Fatal(err)
	}
	script, err := testscripts.NewRecord(environmentID, serviceID, core.Script{ID: ids.NewAt(ids.KindScript, at, 8),
		Slug: "renamed-setup", ServiceName: "web", Body: "echo setup", When: core.ScriptPreDeploy, Order: 20,
		Execution: &core.ScriptExecution{Mode: core.ScriptExecutionExplicit, User: "0:0",
			Image: "example.invalid/setup@sha256:" + strings.Repeat("a", 64),
			Volumes: []core.ScriptVolumeGrant{
				{VolumeID: volumeID, Target: "/data", ReadOnly: false},
			}, EntryIDs: []string{entryID}},
	})
	if err != nil {
		t.Fatal(err)
	}
	script.Origin, script.ReconciliationKey = "blueprint", "setup-hook"
	store := &blueprintTestStore{values: map[string]testkeyvalue.KeyValue{}, revision: 1}
	hierarchy, err := etcd.NewEnvironmentBlueprintRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	groups, err := groupstore.New(store)
	if err != nil {
		t.Fatal(err)
	}
	planner, err := testreleasegroup.NewReleaseGroupBlueprintPlanner(groups, hierarchy.HierarchyRepository)
	if err != nil {
		t.Fatal(err)
	}
	repository := &authoringRoundtripRepository{
		blueprintPreflightRepository: &blueprintPreflightRepository{
			tenant: testhierarchy.TenantRecord{ID: tenantID, Slug: "tenant"},
			project: testhierarchy.ProjectRecord{
				ID:       projectID,
				TenantID: tenantID,
				Slug:     "project",
				Kind:     testhierarchy.ProjectKindTenant,
			},
			environment: testhierarchy.EnvironmentRecord{
				ID:          environmentID,
				ProjectID:   projectID,
				Name:        "production",
				NetworkPool: "10.40.0.0/16",
			},
		},
		projection: testenvironmentprojection.EnvironmentComposeProjection{
			EnvironmentID: environmentID,
			RevisionID:    revisionID,
			NormalizedCompose: []byte(
				"services:\n  web:\n    image: example/web:1\n    volumes: [data:/data:ro]\nvolumes:\n  data: {}\n",
			),
			DesiredServices: []testservices.EnvironmentServiceProjection{
				{EnvironmentID: environmentID, Desired: service.Desired},
			},
			Volumes: []testenvironmentprojection.EnvironmentVolumeIdentity{
				{ID: volumeID, Key: "data", Slug: "renamed-data"},
			},
			Entries: []testentries.Record{
				{EnvironmentID: environmentID, BlueprintKey: "SETUP_INPUT", Entry: core.EnvEntry{
					ID: entryID, Kind: core.EntryKindEnv, Key: "SETUP_INPUT", Exposure: []string{"web"},
					Source: core.EntrySource{Kind: core.SourceLiteral, Literal: "input"},
				}},
			},
		},
		scripts: []testscripts.Record{script},
	}
	blueprints := &Service{repository: repository, releaseGroups: planner}
	document, err := blueprints.GetBlueprint(t.Context(), environmentID)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(document.Document, volumeID) || strings.Contains(document.Document, entryID) ||
		!strings.Contains(document.Document, "read_only: false") {
		t.Fatal("export changed authored resource authority")
	}
	parsed, err := blueprintparser.Parse(t.Context(), blueprintparser.EnvironmentScope{
		EnvironmentID: environmentID, Tenant: "tenant", Project: "project", Environment: "production",
	}, core.BlueprintBundle{RootPath: "groundplane.yaml", ComposeSources: []string{"groundplane.yaml"},
		Files: []core.BlueprintFile{{Path: "groundplane.yaml", Content: []byte(document.Document)}}})
	if err != nil {
		t.Fatal(err)
	}
	reconciled, err := desiredrevision.ReconcileBlueprintScripts(environmentID, parsed.Extensions.Scripts,
		[]testservices.ServiceRecord{service}, []testscripts.Record{script}, desiredrevision.BlueprintScriptResources{
			Volumes: []testcomposeidentity.Resource{
				{ID: volumeID, Name: "data"},
			}, Entries: repository.projection.Entries,
		}, func(ids.Kind, string) string { t.Fatal("export reapply allocated an identity"); return "" })
	if err != nil || len(reconciled.Current) != 1 || len(reconciled.BodyGenerations) != 0 ||
		!reflect.DeepEqual(reconciled.Current[0], script) {
		t.Fatalf("export reapply: %#v, %v", reconciled, err)
	}
}
