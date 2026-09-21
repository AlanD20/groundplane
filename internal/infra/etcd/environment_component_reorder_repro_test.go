package etcd

import (
	context "context"
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	core "github.com/AlanD20/groundplane/internal/core"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testcomponentplanning "github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	testcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testnetworkreservations "github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	testzones "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	errs "github.com/AlanD20/groundplane/pkg/errs"
	testing "testing"
	time "time"
)

func TestPrepareEnvironmentComponentTaskReorderedMultiComponentUsesDesiredSecondaryZone(t *testing.T) {

	t.Parallel()
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	repository, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	project, environment := createEnvironmentBlueprintOwners(t, repository)
	now := environment.Record.CreatedAt.Add(time.Hour)
	frontend, err := testzones.NewRecord(environment.Record.ID, core.Zone{
		ID: ids.NewAt(ids.KindNetwork, now, 1601), Name: "frontend", Subnet: "10.40.10.0/29",
		OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environment.Record.ID,
	})
	if err != nil {
		t.Fatalf("NewZoneRecord(frontend) error = %v", err)
	}
	secondary, err := testzones.NewRecord(environment.Record.ID, core.Zone{
		ID: ids.NewAt(ids.KindNetwork, now, 1602), Name: "secondary", Subnet: "10.40.11.0/29",
		OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environment.Record.ID,
	})
	if err != nil {
		t.Fatalf("NewZoneRecord(secondary) error = %v", err)
	}
	previousTask := environmentBlueprintTestTask(t, project.Record, environment.Record, 1603)
	desiredTask := environmentBlueprintTestTask(t, project.Record, environment.Record, 1604)
	desiredProjection := withTestEnvironmentComposeArtifact(testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: environment.Record.ID, RevisionID: desiredTask.ID, RenderGeneration: 2,
		DesiredZones: []testenvironmentprojection.EnvironmentZoneProjection{
			{EnvironmentID: environment.Record.ID, Desired: frontend.Desired},
			{EnvironmentID: environment.Record.ID, Desired: secondary.Desired},
		},
	})
	stageEnvironmentBlueprintForPublicationTest(
		t, repository, 0,
		environmentBlueprintTestRevision(environment.Record.ID, desiredTask, "services: {}\n"),
		desiredProjection,
		environmentBlueprintTestMarker(desiredTask, environment.Record.ID),
	)
	headValue, err := testidempotency.EncodeTaskReference(desiredTask.ID)
	if err != nil {
		t.Fatalf("encodeTaskReference() error = %v", err)
	}
	seedEnvironmentComponentCandidateValue(
		t, store, testblueprints.EnvironmentBlueprintHeadKey(environment.Record.ID), headValue,
	)
	appliedProjection := withTestEnvironmentComposeArtifact(testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: environment.Record.ID, RevisionID: previousTask.ID, RenderGeneration: 1,
		DesiredZones: []testenvironmentprojection.EnvironmentZoneProjection{{
			EnvironmentID: environment.Record.ID, Desired: frontend.Desired,
		}},
	})
	appliedValue, err := testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(appliedProjection)
	if err != nil {
		t.Fatalf("encodeEnvironmentComposeProjection() error = %v", err)
	}
	seedEnvironmentComponentCandidateValue(
		t, store, testenvironmentprojection.EnvironmentComposeProjectionStorageKey(environment.Record.ID), appliedValue,
	)
	zones, err := newZoneRepository(store)
	if err != nil {
		t.Fatalf("newZoneRepository() error = %v", err)
	}
	listed, err := zones.ListZones(ctx, environment.Record.ID, testkeyvalue.PageRequest{})
	if err != nil || len(listed.Items) != 2 {
		t.Fatalf("ListZones() = %#v, %v", listed, err)
	}
	zoneChanges := make([]testblueprints.EnvironmentBlueprintZoneChange, len(listed.Items))
	for index := range listed.Items {
		current := listed.Items[index]
		zoneChanges[index] = testblueprints.EnvironmentBlueprintZoneChange{Current: &current, Record: current.Record}
	}
	components, err := newComponentRepository(store)
	if err != nil {
		t.Fatalf("newComponentRepository() error = %v", err)
	}
	caddyCurrent, err := testcomponents.NewRecord(core.Component{
		ID: ids.NewAt(ids.KindComponent, now, 1605), Owner: core.ComponentOwnerEnvironment,
		OwnerID: environment.Record.ID, Kind: core.ComponentKindIngressCaddy, Enabled: true,
		Config: core.ComponentConfig{
			Caddy: &core.CaddyComponentConfig{ZoneIDs: []string{frontend.Desired.ID}},
		},
		GeneratedServices: []string{ids.NewAt(ids.KindService, now, 1606)},
		PinnedIPv4:        "10.40.10.6", Healthy: true,
	})
	if err != nil {
		t.Fatalf("NewComponentRecord(caddy) error = %v", err)
	}
	caddyVersion, err := components.CreateEnvironmentComponent(ctx, environment, project, caddyCurrent)
	if err != nil {
		t.Fatalf("CreateEnvironmentComponent(caddy) error = %v", err)
	}
	registryValue, err := testnetworkreservations.EncodeComponentAddressRegistry(
		frontend,
		testnetworkreservations.ComponentAddressRegistry{
			Reservations: map[string]string{caddyCurrent.Desired.ID: caddyCurrent.Runtime.PinnedIPv4},
		},
	)
	if err != nil {
		t.Fatalf("encodeComponentAddressRegistry() error = %v", err)
	}
	seedEnvironmentComponentCandidateValue(
		t, store, testnetworkreservations.ComponentAddressRegistryKey(frontend.Desired.ID), registryValue,
	)
	tunnelCurrent, err := testcomponents.NewRecord(core.Component{
		ID: ids.NewAt(ids.KindComponent, now, 1607), Owner: core.ComponentOwnerEnvironment,
		OwnerID: environment.Record.ID, Kind: core.ComponentKindEdgeCloudflare,
	})
	if err != nil {
		t.Fatalf("NewComponentRecord(tunnel) error = %v", err)
	}
	tunnelVersion, err := components.CreateEnvironmentComponent(ctx, environment, project, tunnelCurrent)
	if err != nil {
		t.Fatalf("CreateEnvironmentComponent(tunnel) error = %v", err)
	}

	caddyVersion.ReadRevision = tunnelVersion.ReadRevision
	inputs := []testcomponentplanning.EnvironmentComponentCandidateInput{
		{
			Current: caddyVersion,
			Candidate: core.Component{
				ID: caddyCurrent.Desired.ID, Owner: core.ComponentOwnerEnvironment,
				OwnerID: environment.Record.ID, Kind: core.ComponentKindIngressCaddy, Enabled: true,
				Config: core.ComponentConfig{Caddy: &core.CaddyComponentConfig{
					ZoneIDs: []string{secondary.Desired.ID, frontend.Desired.ID},
				}},
				GeneratedServices: []string{ids.NewAt(ids.KindService, now, 1608)},
			},
		},
		{
			Current: tunnelVersion,
			Candidate: core.Component{
				ID: tunnelCurrent.Desired.ID, Owner: core.ComponentOwnerEnvironment,
				OwnerID: environment.Record.ID, Kind: core.ComponentKindEdgeCloudflare, Enabled: true,
				Config: core.ComponentConfig{CloudflareTunnel: &core.CloudflareTunnelComponentConfig{
					ZoneIDs:  []string{secondary.Desired.ID, frontend.Desired.ID},
					SecretID: ids.NewAt(ids.KindSecret, now, 1609),
				}},
				GeneratedServices: []string{ids.NewAt(ids.KindService, now, 1610)},
			},
		},
	}
	_, err = repository.PrepareEnvironmentComponentTask(
		ctx,
		desiredTask.ID,
		environment.Record.ID,
		zoneChanges,
		inputs,
		now,
	)
	if err != nil {
		t.Fatalf("PrepareEnvironmentComponentTask() rejected reordered desired secondary Zone: %v", err)
	}
	changedRecord := zoneChanges[0]
	changedCurrent := *changedRecord.Current
	changedCurrent.Record.Desired.Internal = !changedCurrent.Record.Desired.Internal
	changedRecord.Current = &changedCurrent
	if _, err = repository.PrepareEnvironmentComponentTask(
		ctx, desiredTask.ID, environment.Record.ID, []testblueprints.EnvironmentBlueprintZoneChange{changedRecord, zoneChanges[1]}, inputs, now,
	); !isKind(err, errs.KindStateConflict) {
		t.Fatalf("PrepareEnvironmentComponentTask(changed desired Zone) error = %v, want state conflict", err)
	}
	changedRevision := zoneChanges[0]
	currentCopy := *changedRevision.Current
	currentCopy.Revision++
	changedRevision.Current = &currentCopy
	if _, err = repository.PrepareEnvironmentComponentTask(
		ctx, desiredTask.ID, environment.Record.ID, []testblueprints.EnvironmentBlueprintZoneChange{changedRevision, zoneChanges[1]}, inputs, now,
	); !isKind(err, errs.KindStateConflict) {
		t.Fatalf("PrepareEnvironmentComponentTask(changed Zone revision) error = %v, want state conflict", err)
	}
}
