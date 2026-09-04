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
	appliedValue, err := encodeEnvironmentComposeProjection(previousProjection)
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
	selected := Versioned[EnvironmentComposeProjection]{
		Record:       previousProjection,
		Revision:     selectedState.Values[0].ModRevision,
		ReadRevision: selectedState.ReadRevision,
	}
	selectedZone, err := joinEnvironmentZone(selected, selected.Record.DesiredZones[0])
	if err != nil {
		t.Fatalf("joinEnvironmentZone() error = %v", err)
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
					ZoneID: zone.Desired.ID,
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
				ZoneID: zone.Desired.ID,
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
			ZoneID: zone.Desired.ID, CaddyfileTemplate: "{routes}\n",
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
		Config:            core.ComponentConfig{Caddy: &core.CaddyComponentConfig{ZoneID: zone.Desired.ID}},
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
	seedEnvironmentComponentCandidateValue(t, store, environmentComposeProjectionKey(environment.Record.ID), appliedValue)
	transaction, err := store.Transact(ctx, publication.conditions, publication.mutations)
	if err != nil || transaction.Succeeded {
		t.Fatalf("Component publication after applied projection change = %#v, %v", transaction, err)
	}
}

func TestPrepareEnvironmentComponentTaskAllowsMixedAppliedAndNewZones(t *testing.T) {
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
	applied := withTestEnvironmentComposeArtifact(EnvironmentComposeProjection{
		EnvironmentID: environment.Record.ID, RevisionID: ids.NewAt(ids.KindTask, now, 1532), RenderGeneration: 1,
		DesiredZones: []EnvironmentZoneProjection{{EnvironmentID: environment.Record.ID, Desired: existingZone.Desired}},
	})
	appliedValue, err := encodeEnvironmentComposeProjection(applied)
	if err != nil {
		t.Fatalf("encodeEnvironmentComposeProjection() error = %v", err)
	}
	seedEnvironmentComponentCandidateValue(t, store, environmentComposeProjectionKey(environment.Record.ID), appliedValue)
	appliedRead, err := store.GetMany(ctx, GetManyRequest{Keys: []string{environmentComposeProjectionKey(environment.Record.ID)}})
	if err != nil || appliedRead == nil || len(appliedRead.Values) != 1 || appliedRead.Values[0] == nil {
		t.Fatalf("read applied projection = %#v, %v", appliedRead, err)
	}
	selectedZone, err := joinEnvironmentZone(Versioned[EnvironmentComposeProjection]{
		Record: applied, Revision: appliedRead.Values[0].ModRevision, ReadRevision: appliedRead.ReadRevision,
	}, applied.DesiredZones[0])
	if err != nil {
		t.Fatalf("joinEnvironmentZone() error = %v", err)
	}
	serviceID := ids.NewAt(ids.KindService, now, 1533)
	component, err := NewComponentRecord(core.Component{
		ID: ids.NewAt(ids.KindComponent, now, 1534), Owner: core.ComponentOwnerEnvironment,
		OwnerID: environment.Record.ID, Kind: core.ComponentKindIngressCaddy, Enabled: true,
		Config:            core.ComponentConfig{Caddy: &core.CaddyComponentConfig{ZoneID: existingZone.Desired.ID}},
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
	seedEnvironmentComponentCandidateValue(t, store, componentAddressRegistryKey(existingZone.Desired.ID), registryValue)
	preparation, err := repository.PrepareEnvironmentComponentTask(
		ctx, ids.NewAt(ids.KindTask, now, 1535), environment.Record.ID,
		[]EnvironmentBlueprintZoneChange{{Current: &selectedZone, Record: existingZone}, {Record: newZone}},
		[]EnvironmentComponentCandidateInput{{Current: current, Candidate: core.Component{
			ID: component.Desired.ID, Owner: core.ComponentOwnerEnvironment,
			OwnerID: environment.Record.ID, Kind: core.ComponentKindIngressCaddy, Enabled: true,
			Config:            core.ComponentConfig{Caddy: &core.CaddyComponentConfig{ZoneID: newZone.Desired.ID}},
			GeneratedServices: []string{serviceID},
		}}}, now,
	)
	if err != nil || !preparation.appliedProjectionPresent ||
		preparation.appliedProjectionRevision != appliedRead.Values[0].ModRevision || len(preparation.addresses) != 2 {
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
