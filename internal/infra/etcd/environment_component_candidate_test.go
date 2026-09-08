package etcd

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestPrepareEnvironmentComponentTaskReservesWithoutPublishing(t *testing.T) {
	testEnvironmentComponentTaskPublication(t, false)
}

func TestEnvironmentBlueprintPublishesComponentWithSharedDesiredHeadCompare(t *testing.T) {
	testEnvironmentComponentTaskPublication(t, true)
}

func testEnvironmentComponentTaskPublication(t *testing.T, combined bool) {
	t.Helper()
	// Rationale: rendering needs the exact Caddy address before Task creation,
	// but a failed Blueprint transaction must not leak that reservation.
	t.Parallel()
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	repository, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	project, environment := createEnvironmentBlueprintOwners(t, repository)
	now := environment.Record.CreatedAt.Add(time.Hour)
	zone, err := NewZoneRecord(environment.Record.ID, core.Zone{
		ID: ids.NewAt(ids.KindNetwork, now, 1401), Name: "frontend", Subnet: "10.40.10.0/29",
		OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environment.Record.ID,
	})
	if err != nil {
		t.Fatalf("NewZoneRecord() error = %v", err)
	}
	previousTask := environmentBlueprintTestTask(t, project.Record, environment.Record, 1390)
	previousProjection := withTestEnvironmentComposeArtifact(EnvironmentComposeProjection{
		EnvironmentID:    environment.Record.ID,
		RevisionID:       previousTask.ID,
		RenderGeneration: 1,
		DesiredZones: []EnvironmentZoneProjection{{
			EnvironmentID: environment.Record.ID,
			Desired:       zone.Desired,
		}},
	})
	stageEnvironmentBlueprintForPublicationTest(
		t,
		repository,
		0,
		environmentBlueprintTestRevision(environment.Record.ID, previousTask, "services: {}\n"),
		previousProjection,
		environmentBlueprintTestMarker(previousTask, environment.Record.ID),
	)
	headValue, err := encodeTaskReference(previousTask.ID)
	if err != nil {
		t.Fatalf("encodeTaskReference() error = %v", err)
	}
	seedEnvironmentComponentCandidateValue(
		t, store, environmentBlueprintHeadKey(environment.Record.ID), headValue,
	)
	selectedProjection := previousProjection
	selectedProjection.DesiredZones = append([]EnvironmentZoneProjection(nil), previousProjection.DesiredZones...)
	selectedProjection.DesiredZones[0].Desired.Internal = true
	appliedValue, err := encodeEnvironmentComposeProjection(selectedProjection)
	if err != nil {
		t.Fatalf("encodeEnvironmentComposeProjection() error = %v", err)
	}
	seedEnvironmentComponentCandidateValue(
		t, store, environmentComposeProjectionKey(environment.Record.ID), appliedValue,
	)
	selectedState, err := store.GetMany(ctx, GetManyRequest{
		Keys: []string{environmentComposeProjectionKey(environment.Record.ID)},
	})
	if err != nil || selectedState == nil || len(selectedState.Values) != 1 || selectedState.Values[0] == nil {
		t.Fatalf("read applied Environment projection = %#v, %v", selectedState, err)
	}
	zones, err := newZoneRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	listedZones, err := zones.ListZones(ctx, environment.Record.ID, PageRequest{})
	if err != nil || len(listedZones.Items) != 1 {
		t.Fatalf("ListZones() = %#v, %v", listedZones, err)
	}
	selectedZone := listedZones.Items[0]
	if selectedZone.Revision == selectedState.Values[0].ModRevision {
		t.Fatal("fixture failed to separate desired and applied revisions")
	}
	components, err := newComponentRepository(store)
	if err != nil {
		t.Fatalf("newComponentRepository() error = %v", err)
	}
	component, err := NewComponentRecord(core.Component{
		ID: ids.NewAt(ids.KindComponent, now, 1402), Owner: core.ComponentOwnerEnvironment,
		OwnerID: environment.Record.ID, Kind: core.ComponentKindIngressCaddy,
	})
	if err != nil {
		t.Fatalf("NewComponentRecord() error = %v", err)
	}
	current, err := components.CreateEnvironmentComponent(ctx, environment, project, component)
	if err != nil {
		t.Fatalf("CreateEnvironmentComponent() error = %v", err)
	}
	taskID := ids.NewAt(ids.KindTask, now, 1403)
	preparation, err := repository.PrepareEnvironmentComponentTask(
		ctx,
		taskID,
		environment.Record.ID,
		[]EnvironmentBlueprintZoneChange{{Current: &selectedZone, Record: zone}},
		[]EnvironmentComponentCandidateInput{{
			Current: current,
			Candidate: core.Component{
				ID: current.Record.Desired.ID, Owner: core.ComponentOwnerEnvironment,
				OwnerID: environment.Record.ID, Kind: core.ComponentKindIngressCaddy, Enabled: true,
				Config: core.ComponentConfig{Caddy: &core.CaddyComponentConfig{
					ZoneIDs: []string{zone.Desired.ID},
				}},
				GeneratedServices: []string{ids.NewAt(ids.KindService, now, 1404)},
			},
		}},
		now,
	)
	if err != nil {
		t.Fatalf("PrepareEnvironmentComponentTask() error = %v", err)
	}
	if preparation.Intent.TaskID != taskID || len(preparation.Intent.Candidates) != 1 ||
		preparation.Intent.Candidates[0].Candidate.Runtime.PinnedIPv4 != "10.40.10.6" ||
		len(preparation.addresses) != 1 || !preparation.addresses[0].Mutates ||
		preparation.addresses[0].Next.Reservations[current.Record.Desired.ID] != "10.40.10.6" {
		t.Fatalf("Component preparation = %#v", preparation)
	}
	if !reflect.DeepEqual(preparation.Intent.Candidates[0].Current, current.Record) ||
		preparation.Intent.Candidates[0].CurrentRevision != current.Revision {
		t.Fatalf("Component preparation changed its active base = %#v", preparation.Intent.Candidates[0])
	}
	for _, key := range []string{
		componentAddressRegistryKey(zone.Desired.ID),
		componentTaskIntentKey(taskID),
		componentTaskActiveEnvironmentKey(environment.Record.ID),
	} {
		result, getErr := store.Get(ctx, key)
		if getErr != nil || result.Entry != nil {
			t.Fatalf("preparation published %q = %#v, %v", key, result, getErr)
		}
	}
	publicationTask := environmentBlueprintTestTask(t, project.Record, environment.Record, 1403)
	publicationTask.ID = taskID
	publicationTask.CreatedAt = now
	if combined {
		publicationTask.UpdatedAt = now
		publicationTask.RenderGeneration = 2
		publicationTask.Params[EnvironmentDesiredRevisionParam] = taskID
		nextProjection := previousProjection
		nextProjection.RevisionID, nextProjection.RenderGeneration = taskID, 2
		nextProjection.ComposeArtifact = nil
		nextProjection = withTestEnvironmentComposeArtifact(nextProjection)
		result := publishEnvironmentBlueprintTestRevision(t, repository, project, environment, selectedZone.Revision,
			environmentBlueprintTestRevision(environment.Record.ID, publicationTask, "services: {}\n"), nextProjection,
			[]EnvironmentBlueprintZoneChange{{Current: &selectedZone, Record: zone}}, nil, nil, preparation,
			publicationTask, environmentBlueprintTestMarker(publicationTask, environment.Record.ID))
		outcome, _, conflict, err := result.Classify()
		if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
			t.Fatalf("combined Component publication = %v, %v, %v", outcome, conflict, err)
		}
		return
	}
	publication, err := repository.prepareComponentTaskPublication(ctx, environment, publicationTask,
		[]EnvironmentBlueprintZoneChange{{Current: &selectedZone, Record: zone}}, preparation)
	if err != nil {
		t.Fatal(err)
	}
	defer clearPreparedComponentTaskPublication(publication)
	if preparation.addresses[0].Zone.Revision != selectedZone.Revision ||
		preparation.appliedProjectionRevision != selectedState.Values[0].ModRevision {
		t.Fatal("desired Zone and applied projection revision authorities were confused")
	}
	changed, err := store.Transact(ctx,
		[]Condition{{Key: environmentBlueprintHeadKey(environment.Record.ID), ModRevision: selectedZone.Revision}},
		[]Mutation{{Type: MutationPut, Key: environmentBlueprintHeadKey(environment.Record.ID), Value: headValue}})
	if err != nil || !changed.Succeeded {
		t.Fatalf("advance desired head = %#v, %v", changed, err)
	}
	transaction, err := store.Transact(ctx, publication.conditions, publication.mutations)
	if err != nil || transaction.Succeeded {
		t.Fatalf("Component publication accepted changed desired head: %#v, %v", transaction, err)
	}

	seedEnvironmentComponentCandidateValue(
		t,
		store,
		componentTaskActiveEnvironmentKey(environment.Record.ID),
		[]byte(ids.NewAt(ids.KindTask, now, 1405)),
	)
	_, err = repository.PrepareEnvironmentComponentTask(
		ctx,
		ids.NewAt(ids.KindTask, now, 1406),
		environment.Record.ID,
		[]EnvironmentBlueprintZoneChange{{Current: &selectedZone, Record: zone}},
		[]EnvironmentComponentCandidateInput{{Current: current, Candidate: core.Component{
			ID: current.Record.Desired.ID, Owner: core.ComponentOwnerEnvironment,
			OwnerID: environment.Record.ID, Kind: core.ComponentKindIngressCaddy, Enabled: true,
			Config: core.ComponentConfig{Caddy: &core.CaddyComponentConfig{
				ZoneIDs: []string{zone.Desired.ID},
			}},
			GeneratedServices: []string{ids.NewAt(ids.KindService, now, 1407)},
		}}},
		now,
	)
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("PrepareEnvironmentComponentTask(active) error = %v", err)
	}
}

func TestPrepareEnvironmentComponentTaskAllowsInitialProjectionAbsence(t *testing.T) {
	// Rationale: the first Blueprint publication must prepare an enabled
	// Component against its newly staged Zone before any applied Environment
	// projection exists, while an existing Zone still requires that projection.
	t.Parallel()
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	repository, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	project, environment := createEnvironmentBlueprintOwners(t, repository)
	components, err := newComponentRepository(store)
	if err != nil {
		t.Fatalf("newComponentRepository() error = %v", err)
	}
	now := environment.Record.CreatedAt.Add(time.Hour)
	component, err := NewComponentRecord(core.Component{
		ID: ids.NewAt(ids.KindComponent, now, 1501), Owner: core.ComponentOwnerEnvironment,
		OwnerID: environment.Record.ID, Kind: core.ComponentKindIngressCaddy,
	})
	if err != nil {
		t.Fatalf("NewComponentRecord() error = %v", err)
	}
	current, err := components.CreateEnvironmentComponent(ctx, environment, project, component)
	if err != nil {
		t.Fatalf("CreateEnvironmentComponent() error = %v", err)
	}
	zone, err := NewZoneRecord(environment.Record.ID, core.Zone{
		ID: ids.NewAt(ids.KindNetwork, now, 1502), Name: "frontend", Subnet: "10.40.10.0/29",
		OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environment.Record.ID,
	})
	if err != nil {
		t.Fatalf("NewZoneRecord() error = %v", err)
	}
	candidate := core.Component{
		ID: current.Record.Desired.ID, Owner: core.ComponentOwnerEnvironment,
		OwnerID: environment.Record.ID, Kind: core.ComponentKindIngressCaddy, Enabled: true,
		Config: core.ComponentConfig{Caddy: &core.CaddyComponentConfig{
			ZoneIDs: []string{zone.Desired.ID}, CaddyfileTemplate: "{routes}\n",
		}},
		GeneratedServices: []string{ids.NewAt(ids.KindService, now, 1503)},
	}

	preparation, err := repository.PrepareEnvironmentComponentTask(
		ctx,
		ids.NewAt(ids.KindTask, now, 1504),
		environment.Record.ID,
		[]EnvironmentBlueprintZoneChange{{Record: zone}},
		[]EnvironmentComponentCandidateInput{{
			Current:   current,
			Candidate: candidate,
		}},
		now,
	)
	if err != nil {
		t.Fatalf("PrepareEnvironmentComponentTask() error = %v", err)
	}
	if len(preparation.Intent.Candidates) != 1 ||
		preparation.Intent.Candidates[0].Candidate.Runtime.PinnedIPv4 != "10.40.10.6" ||
		len(preparation.addresses) != 1 || !preparation.addresses[0].Mutates ||
		preparation.addresses[0].Zone.Revision != 0 {
		t.Fatalf("Component preparation = %#v", preparation)
	}

	currentZone := Versioned[ZoneRecord]{
		Record: zone, Revision: current.Revision, ReadRevision: current.ReadRevision,
	}
	_, err = repository.PrepareEnvironmentComponentTask(
		ctx,
		ids.NewAt(ids.KindTask, now, 1505),
		environment.Record.ID,
		[]EnvironmentBlueprintZoneChange{{Current: &currentZone, Record: zone}},
		[]EnvironmentComponentCandidateInput{{Current: current, Candidate: candidate}},
		now,
	)
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("PrepareEnvironmentComponentTask(existing Zone) error = %v", err)
	}
}

func TestPrepareEnvironmentComponentTaskUsesAppliedProjectionAuthority(t *testing.T) {
	// Rationale: desired publication may precede runtime application, so a new
	// Zone is fenced against the applied projection and that absence must remain
	// unchanged through atomic Component publication.
	t.Parallel()
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	repository, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	project, environment := createEnvironmentBlueprintOwners(t, repository)
	task := environmentBlueprintTestTask(t, project.Record, environment.Record, 1520)
	zone, err := NewZoneRecord(environment.Record.ID, core.Zone{
		ID: ids.NewAt(ids.KindNetwork, task.CreatedAt, 1521), Name: "frontend", Subnet: "10.40.12.0/29",
		OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environment.Record.ID,
	})
	if err != nil {
		t.Fatalf("NewZoneRecord() error = %v", err)
	}
	desired := withTestEnvironmentComposeArtifact(EnvironmentComposeProjection{
		EnvironmentID: environment.Record.ID, RevisionID: task.ID, RenderGeneration: 1,
		DesiredZones: []EnvironmentZoneProjection{{EnvironmentID: environment.Record.ID, Desired: zone.Desired}},
	})
	stageEnvironmentBlueprintForPublicationTest(
		t, repository, 0, environmentBlueprintTestRevision(environment.Record.ID, task, "services: {}\n"),
		desired, environmentBlueprintTestMarker(task, environment.Record.ID),
	)
	headValue, err := encodeTaskReference(task.ID)
	if err != nil {
		t.Fatalf("encodeTaskReference() error = %v", err)
	}
	seedEnvironmentComponentCandidateValue(t, store, environmentBlueprintHeadKey(environment.Record.ID), headValue)
	components, err := newComponentRepository(store)
	if err != nil {
		t.Fatalf("newComponentRepository() error = %v", err)
	}
	component, err := NewComponentRecord(core.Component{
		ID: ids.NewAt(ids.KindComponent, task.CreatedAt, 1522), Owner: core.ComponentOwnerEnvironment,
		OwnerID: environment.Record.ID, Kind: core.ComponentKindIngressCaddy,
	})
	if err != nil {
		t.Fatalf("NewComponentRecord() error = %v", err)
	}
	current, err := components.CreateEnvironmentComponent(ctx, environment, project, component)
	if err != nil {
		t.Fatalf("CreateEnvironmentComponent() error = %v", err)
	}
	candidate := core.Component{
		ID: component.Desired.ID, Owner: core.ComponentOwnerEnvironment,
		OwnerID: environment.Record.ID, Kind: core.ComponentKindIngressCaddy, Enabled: true,
		Config:            core.ComponentConfig{Caddy: &core.CaddyComponentConfig{ZoneIDs: []string{zone.Desired.ID}}},
		GeneratedServices: []string{ids.NewAt(ids.KindService, task.CreatedAt, 1523)},
	}
	zoneChanges := []EnvironmentBlueprintZoneChange{{Record: zone}}
	preparation, err := repository.PrepareEnvironmentComponentTask(
		ctx, task.ID, environment.Record.ID, zoneChanges,
		[]EnvironmentComponentCandidateInput{{Current: current, Candidate: candidate}}, task.CreatedAt,
	)
	if err != nil || preparation.appliedProjectionPresent || preparation.appliedProjectionRevision != 0 {
		t.Fatalf("PrepareEnvironmentComponentTask() = %#v, %v", preparation, err)
	}
	publication, err := repository.prepareComponentTaskPublication(
		ctx, environment, task, zoneChanges, preparation,
	)
	if err != nil {
		t.Fatalf("prepareComponentTaskPublication() error = %v", err)
	}
	defer clearPreparedComponentTaskPublication(publication)
	appliedValue, err := encodeEnvironmentComposeProjection(desired)
	if err != nil {
		t.Fatalf("encodeEnvironmentComposeProjection() error = %v", err)
	}
	seedEnvironmentComponentCandidateValue(
		t,
		store,
		environmentComposeProjectionKey(environment.Record.ID),
		appliedValue,
	)
	transaction, err := store.Transact(ctx, publication.conditions, publication.mutations)
	if err != nil || transaction.Succeeded {
		t.Fatalf("Component publication after applied projection change = %#v, %v", transaction, err)
	}
}

func TestPrepareEnvironmentComponentTaskRetainsPrimaryReservationWhenAddingSecondaryZone(t *testing.T) {
	// Rationale: one Blueprint can move Caddy from an exactly applied Zone to a
	// new Zone, but the existing half of that mixed transition remains fenced to
	// the applied projection record and revision.
	t.Parallel()
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	repository, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	project, environment := createEnvironmentBlueprintOwners(t, repository)
	now := environment.Record.CreatedAt.Add(2 * time.Hour)
	existingZone, err := NewZoneRecord(environment.Record.ID, core.Zone{
		ID: ids.NewAt(ids.KindNetwork, now, 1530), Name: "current", Subnet: "10.40.14.0/29",
		OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environment.Record.ID,
	})
	if err != nil {
		t.Fatalf("NewZoneRecord(existing) error = %v", err)
	}
	newZone, err := NewZoneRecord(environment.Record.ID, core.Zone{
		ID: ids.NewAt(ids.KindNetwork, now, 1531), Name: "next", Subnet: "10.40.15.0/29",
		OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environment.Record.ID,
	})
	if err != nil {
		t.Fatalf("NewZoneRecord(new) error = %v", err)
	}
	previousTask := environmentBlueprintTestTask(t, project.Record, environment.Record, 1532)
	applied := withTestEnvironmentComposeArtifact(EnvironmentComposeProjection{
		EnvironmentID: environment.Record.ID, RevisionID: previousTask.ID, RenderGeneration: 1,
		DesiredZones: []EnvironmentZoneProjection{
			{EnvironmentID: environment.Record.ID, Desired: existingZone.Desired},
		},
	})
	appliedValue, err := encodeEnvironmentComposeProjection(applied)
	if err != nil {
		t.Fatalf("encodeEnvironmentComposeProjection() error = %v", err)
	}
	seedEnvironmentComponentCandidateValue(
		t,
		store,
		environmentComposeProjectionKey(environment.Record.ID),
		appliedValue,
	)
	stageEnvironmentBlueprintForPublicationTest(t, repository, 0,
		environmentBlueprintTestRevision(environment.Record.ID, previousTask, "services: {}\n"), applied,
		environmentBlueprintTestMarker(previousTask, environment.Record.ID))
	headValue, err := encodeTaskReference(previousTask.ID)
	if err != nil {
		t.Fatal(err)
	}
	seedEnvironmentComponentCandidateValue(t, store, environmentBlueprintHeadKey(environment.Record.ID), headValue)
	appliedRead, err := store.GetMany(
		ctx,
		GetManyRequest{Keys: []string{environmentComposeProjectionKey(environment.Record.ID)}},
	)
	if err != nil || appliedRead == nil || len(appliedRead.Values) != 1 || appliedRead.Values[0] == nil {
		t.Fatalf("read applied projection = %#v, %v", appliedRead, err)
	}
	zones, err := newZoneRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	selectedZone, err := zones.GetZone(ctx, existingZone.Desired.ID)
	if err != nil {
		t.Fatalf("GetZone() error = %v", err)
	}
	serviceID := ids.NewAt(ids.KindService, now, 1533)
	component, err := NewComponentRecord(core.Component{
		ID: ids.NewAt(ids.KindComponent, now, 1534), Owner: core.ComponentOwnerEnvironment,
		OwnerID: environment.Record.ID, Kind: core.ComponentKindIngressCaddy, Enabled: true,
		Config: core.ComponentConfig{
			Caddy: &core.CaddyComponentConfig{ZoneIDs: []string{existingZone.Desired.ID}},
		},
		GeneratedServices: []string{serviceID}, PinnedIPv4: "10.40.14.6", Healthy: true,
	})
	if err != nil {
		t.Fatalf("NewComponentRecord() error = %v", err)
	}
	components, err := newComponentRepository(store)
	if err != nil {
		t.Fatalf("newComponentRepository() error = %v", err)
	}
	current, err := components.CreateEnvironmentComponent(ctx, environment, project, component)
	if err != nil {
		t.Fatalf("CreateEnvironmentComponent() error = %v", err)
	}
	registryValue, err := encodeComponentAddressRegistry(existingZone, componentAddressRegistry{
		Reservations: map[string]string{component.Desired.ID: component.Runtime.PinnedIPv4},
	})
	if err != nil {
		t.Fatalf("encodeComponentAddressRegistry() error = %v", err)
	}
	seedEnvironmentComponentCandidateValue(
		t,
		store,
		componentAddressRegistryKey(existingZone.Desired.ID),
		registryValue,
	)
	preparation, err := repository.PrepareEnvironmentComponentTask(
		ctx, ids.NewAt(ids.KindTask, now, 1535), environment.Record.ID,
		[]EnvironmentBlueprintZoneChange{{Current: &selectedZone, Record: existingZone}, {Record: newZone}},
		[]EnvironmentComponentCandidateInput{{Current: current, Candidate: core.Component{
			ID: component.Desired.ID, Owner: core.ComponentOwnerEnvironment,
			OwnerID: environment.Record.ID, Kind: core.ComponentKindIngressCaddy, Enabled: true,
			Config: core.ComponentConfig{Caddy: &core.CaddyComponentConfig{ZoneIDs: []string{
				existingZone.Desired.ID, newZone.Desired.ID,
			}}},
			GeneratedServices: []string{serviceID},
		}}}, now,
	)
	if err != nil || !preparation.appliedProjectionPresent ||
		preparation.appliedProjectionRevision != appliedRead.Values[0].ModRevision || len(preparation.addresses) != 1 ||
		preparation.Intent.Candidates[0].Candidate.Runtime.PinnedIPv4 != component.Runtime.PinnedIPv4 ||
		preparation.addresses[0].Mutates {
		t.Fatalf("PrepareEnvironmentComponentTask(mixed Zones) = %#v, %v", preparation, err)
	}
}

func seedEnvironmentComponentCandidateValue(
	t *testing.T,
	store *memoryHierarchyStore,
	key string,
	value []byte,
) {
	t.Helper()
	result, err := store.Transact(
		context.Background(),
		[]Condition{{Key: key}},
		[]Mutation{{Type: MutationPut, Key: key, Value: value}},
	)
	if err != nil || !result.Succeeded {
		t.Fatalf("seed %q = %#v, %v", key, result, err)
	}
}
