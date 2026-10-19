package blueprint

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	testcomposeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	testcomposerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/core/blueprintreconcile"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// TASK-103: select changed desired resources without depending on a materialized
// runtime. Applied fingerprints below are supplied, not execution receipts.
func TestAuthoredServiceSelectionDetectsNativeComposeChanges(t *testing.T) {
	for _, test := range []struct {
		name, fields string
		selected     bool
	}{
		{"unchanged", "", false},
		{"environment", "    environment: {QA_PROOF_REVISION: postgres-attach}\n", true},
		{"healthcheck", "    healthcheck: {test: [CMD, php, -v]}\n", true},
		{"resources", "    mem_limit: 128m\n", true},
		{"mount", "    volumes: [scratch:/proof]\n", true},
		{"entrypoint", "    entrypoint: [/bin/sh, -ec]\n", true},
		{"logging", "    logging: {driver: json-file, options: {max-size: 10m}}\n", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			before := planAuthoredServiceFixture(t, authoredServiceDocument(""), nil)
			after := planAuthoredServiceFixture(t, authoredServiceDocument(test.fields), nil)
			selection := selectAuthoredServiceFixture(t, before, after)
			worker := after.services["worker"]
			if containsAuthoredFixtureTarget(selection.Ready, worker) != test.selected {
				t.Fatalf("worker ready = %t, want %t; selection = %#v",
					containsAuthoredFixtureTarget(selection.Ready, worker), test.selected, selection)
			}
		})
	}
}

func TestAuthoredServiceSelectionWaitsForNewDependency(t *testing.T) {
	before := planAuthoredServiceFixture(t, authoredServiceEnvelope+
		"services:\n  api: {image: registry.example/api:v1}\n  worker: {image: registry.example/worker:v1}\n", nil)
	after := planAuthoredServiceFixture(t, authoredServiceEnvelope+
		"services:\n  database: {image: registry.example/database:v1}\n"+
		"  api:\n    image: registry.example/api:v2\n    depends_on: [database]\n"+
		"  worker: {image: registry.example/worker:v2}\n", nil)
	selection := selectAuthoredServiceFixture(t, before, after)
	if !containsAuthoredFixtureTarget(selection.Ready, after.services["database"]) ||
		!containsAuthoredFixtureTarget(selection.Ready, after.services["worker"]) ||
		!containsAuthoredFixtureTarget(selection.Waiting, after.services["api"]) {
		t.Fatalf("dependency selection = %#v", selection)
	}
}

func TestAuthoredServiceSelectionDetectsProfileActivation(t *testing.T) {
	before := planAuthoredServiceFixture(t, authoredServiceEnvelope+
		"services:\n  api: {image: registry.example/api:v1}\n"+
		"  worker: {image: registry.example/worker:v1, profiles: [jobs]}\n", nil)
	after := planAuthoredServiceFixture(t, authoredServiceEnvelope+
		"services:\n  api: {image: registry.example/api:v1}\n  worker: {image: registry.example/worker:v1}\n", nil)
	selection := selectAuthoredServiceFixture(t, before, after)
	if len(selection.Ready) != 1 || !containsAuthoredFixtureTarget(selection.Ready, after.services["worker"]) {
		t.Fatalf("profile activation selection = %#v", selection)
	}
}

// Profile-disabled definitions still require configuration settlement. Ready
// means a unit can settle, not that it must dispatch an Agent deployment.
// Non-deployment is owned by the Service configuration settlement journey.
func TestAuthoredServiceSelectionIncludesProfileDisabledConfiguration(t *testing.T) {
	before := planAuthoredServiceFixture(t, authoredServiceEnvelope+
		"services:\n  api: {image: registry.example/api:v1}\n"+
		"  worker: {image: registry.example/worker:v1, profiles: [jobs]}\n", nil)
	after := planAuthoredServiceFixture(t, authoredServiceEnvelope+
		"services:\n  frontend: {image: registry.example/frontend:v1}\n"+
		"  api: {image: registry.example/api:v2}\n"+
		"  migrate: {image: registry.example/migrate:v1, profiles: [jobs]}\n"+
		"  worker: {image: registry.example/worker:v2, profiles: [jobs]}\n", nil)
	selection := selectAuthoredServiceFixture(t, before, after)
	for _, name := range []string{"frontend", "api"} {
		if !containsAuthoredFixtureTarget(selection.Ready, after.services[name]) {
			t.Fatalf("active Service %q was not selected: %#v", name, selection)
		}
	}
	for _, name := range []string{"migrate", "worker"} {
		if !containsAuthoredFixtureTarget(selection.Ready, after.services[name]) {
			t.Fatalf("profile-disabled configuration %q was not selected: %#v", name, selection)
		}
	}
}

func TestAuthoredServiceSelectionDetectsReplicaChange(t *testing.T) {
	before := planAuthoredServiceFixture(t, authoredServiceEnvelope+
		"services:\n  api:\n    image: app:v1\n    deploy: {replicas: 1}\n", nil)
	after := planAuthoredServiceFixture(t, authoredServiceEnvelope+
		"services:\n  api:\n    image: app:v1\n    deploy: {replicas: 2}\n", nil)
	selection := selectAuthoredServiceFixture(t, before, after)
	if len(selection.Ready) != 1 || !containsAuthoredFixtureTarget(selection.Ready, after.services["api"]) {
		t.Fatalf("replica selection = %#v", selection)
	}
}

// Changing Release Group membership must settle every member's configuration.
// Deployment remains the separate group operation, not the pure unit selector.
func TestAuthoredServiceSelectionIncludesReleaseGroupConfiguration(t *testing.T) {
	before := planAuthoredServiceFixture(t, authoredServiceEnvelope+
		"services:\n  api:\n    image: app:v1\n    deploy: {replicas: 1}\n"+
		"  worker: {image: worker:v1}\n", nil)
	after := planAuthoredServiceFixture(t, authoredServiceEnvelope+
		"x-gp-release-groups:\n  application: {services: [api, worker]}\n"+
		"services:\n  api:\n    image: app:v1\n    deploy: {replicas: 2}\n"+
		"  worker: {image: worker:v1}\n", nil)
	selection := selectAuthoredServiceFixture(t, before, after)
	for _, name := range []string{"api", "worker"} {
		if !containsAuthoredFixtureTarget(selection.Ready, after.services[name]) {
			t.Fatalf("Release Group configuration %q was not selected: %#v", name, selection)
		}
	}
}

func TestAuthoredServicePlannerRejectsMissingAuthoredService(t *testing.T) {
	planAuthoredServiceFixture(t, authoredServiceDocument(""), map[string]bool{"worker": true})
}

const authoredServiceEnvelope = "kind: environment\nschema: 1\nmetadata: {tenant: qa, project: proof, environment: production}\n" +
	"x-gp-network-pool: 10.95.0.0/16\n"

func authoredServiceDocument(fields string) string {
	return authoredServiceEnvelope + "services:\n  worker:\n    image: example.invalid/worker:fixture\n" +
		"    command: [sleep, infinity]\n    deploy: {replicas: 2}\n" + fields + "volumes: {scratch: {}}\n"
}

type authoredServiceFixturePlan struct {
	environmentID string
	parentID      string
	units         []blueprintunits.Unit
	services      map[string]blueprintunits.ResourceKey
}

func planAuthoredServiceFixture(
	t *testing.T,
	document string,
	omitDesiredService map[string]bool,
) authoredServiceFixturePlan {
	t.Helper()
	ctx := context.Background()
	at := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	environmentID := ids.DeriveAt(ids.KindEnvironment, at, "authored-service-selection", "environment")
	parentID := ids.DeriveAt(ids.KindTask, at, "authored-service-selection", "parent")
	parsed, err := blueprintparser.Parse(ctx, blueprintparser.EnvironmentScope{
		EnvironmentID: environmentID, Tenant: "qa", Project: "proof", Environment: "production",
	}, core.BlueprintBundle{
		RootPath: "groundplane.yaml", ComposeSources: []string{"groundplane.yaml"},
		Files: []core.BlueprintFile{{Path: "groundplane.yaml", Content: []byte(document)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	normalized, err := testcomposerender.MarshalNormalizedEnvironmentProject(parsed.Project)
	if err != nil {
		t.Fatal(err)
	}
	input, err := blueprintparser.BuildDesiredInput(parsed, normalized, nil)
	if err != nil {
		t.Fatal(err)
	}
	desired, err := testenvironmentprojection.NewEnvironmentDesiredInput(environmentID, parentID, 1, input)
	if err != nil {
		t.Fatal(err)
	}
	identities, _ := authoredServiceFixtureIdentities(t, environmentID, parentID, parsed)
	targets, err := selectAuthoredOwnedUnitTargets(identities)
	if err != nil {
		t.Fatal(err)
	}
	for name := range omitDesiredService {
		delete(parsed.Project.Services, name)
	}
	if omitDesiredService != nil {
		desired.Input.NormalizedCompose, err = testcomposerender.MarshalNormalizedEnvironmentProject(parsed.Project)
		if err != nil {
			t.Fatal(err)
		}
	}
	planner, err := newAuthoredUnitPlanner(ctx, authoredParentInput{
		desired: desired, identities: identities,
	}, targets, nil, blueprintunits.Snapshot{EnvironmentID: environmentID})
	if err != nil {
		if omitDesiredService != nil {
			return authoredServiceFixturePlan{}
		}
		t.Fatal(err)
	}
	units, complete, err := planner.plan()
	if omitDesiredService != nil {
		if err == nil {
			t.Fatal("owned Service identity without its authored definition was accepted")
		}
		return authoredServiceFixturePlan{}
	}
	if err != nil || !complete {
		t.Fatalf("authored unit plan: complete=%t error=%v", complete, err)
	}
	result := authoredServiceFixturePlan{
		environmentID: environmentID, parentID: parentID, units: units,
		services: make(map[string]blueprintunits.ResourceKey, len(identities.Services)),
	}
	for _, identity := range identities.Services {
		result.services[identity.Name] = blueprintunits.ResourceKey{Kind: ids.KindService, ID: identity.ID}
	}
	return result
}

func authoredServiceFixtureIdentities(
	t *testing.T,
	environmentID, parentID string,
	parsed blueprintparser.Result,
) (testenvironmentprojection.EnvironmentOwnedIdentities, testcomposeidentity.Snapshot) {
	t.Helper()
	at := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	sequence := int64(0)
	changes, err := testcomposeidentity.ReconcileOwned(
		parsed.Project,
		testcomposeidentity.Snapshot{},
		func(kind ids.Kind) string {
			sequence++
			return ids.NewAt(kind, at, sequence)
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	result := testenvironmentprojection.EnvironmentOwnedIdentities{
		EnvironmentID: environmentID, RevisionID: parentID, RenderGeneration: 1,
	}
	for index := range changes.Current.Services {
		service := &changes.Current.Services[index]
		service.ID = ids.DeriveAt(ids.KindService, at, "authored-service-selection", service.Name)
		result.Services = append(result.Services, testenvironmentprojection.OwnedIdentity{
			ID: service.ID, Name: service.Name, BirthRevisionID: parentID,
		})
	}
	for index := range changes.Current.Networks {
		network := &changes.Current.Networks[index]
		network.ID = ids.DeriveAt(ids.KindNetwork, at, "authored-service-selection", network.Name)
		result.Networks = append(result.Networks, testenvironmentprojection.OwnedIdentity{
			ID: network.ID, Name: network.Name, BirthRevisionID: parentID,
		})
	}
	for index := range changes.Current.Volumes {
		volume := &changes.Current.Volumes[index]
		volume.ID = ids.DeriveAt(ids.KindVolume, at, "authored-service-selection", volume.Name)
		result.Volumes = append(result.Volumes, testenvironmentprojection.OwnedIdentity{
			ID: volume.ID, Name: volume.Name, Slug: volume.Name, BirthRevisionID: parentID,
		})
	}
	return result, changes.Current
}

func selectAuthoredServiceFixture(
	t *testing.T,
	before, after authoredServiceFixturePlan,
) blueprintreconcile.Selection {
	t.Helper()
	snapshot := blueprintunits.Snapshot{
		EnvironmentID: after.environmentID, HeadTaskID: after.parentID,
		Desired: &testkeyvalue.Versioned[blueprintunits.DesiredPlan]{Record: blueprintunits.DesiredPlan{
			EnvironmentID: after.environmentID, ParentTaskID: after.parentID, Complete: true, Units: after.units,
		}},
	}
	// These Applied records simulate the exact acknowledged fingerprints from
	// the preceding authored revision. They are selection inputs only, not
	// evidence that a child execution ran in this fixture.
	appliedTargets := make(map[blueprintunits.ResourceKey]bool, len(before.units))
	for _, unit := range before.units {
		appliedTargets[unit.Target] = true
		snapshot.Applied = append(snapshot.Applied, testkeyvalue.Versioned[blueprintunits.AppliedRecord]{
			Revision: 1,
			Record: blueprintunits.AppliedRecord{
				EnvironmentID: before.environmentID, Target: unit.Target, State: blueprintunits.Applied,
				Fingerprint: unit.Fingerprint,
			},
		})
	}
	// Newly authored targets carry the explicit absence seeded by publication.
	// This models the selection input, not execution or host absence proof.
	for _, unit := range after.units {
		if !appliedTargets[unit.Target] {
			snapshot.Applied = append(snapshot.Applied, testkeyvalue.Versioned[blueprintunits.AppliedRecord]{
				Record: blueprintunits.AppliedRecord{
					EnvironmentID: after.environmentID, Target: unit.Target, State: blueprintunits.Absent,
				},
			})
		}
	}
	selection, err := blueprintunits.Select(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return selection
}

func containsAuthoredFixtureTarget(values []blueprintreconcile.ResourceKey, target blueprintunits.ResourceKey) bool {
	for _, value := range values {
		if value.Kind == target.Kind && value.ID == target.ID {
			return true
		}
	}
	return false
}
