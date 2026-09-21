package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testenvironmentqueries "github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testhierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	testhierarchydeletionfinalization "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionfinalization"
	testhierarchydeletionplanning "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionplanning"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testnetworkreservations "github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	testrecordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	testzones "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
)

func TestEnvironmentHierarchyDeletionFreezesProjectionZonesAndReleasesPool(t *testing.T) {
	// Rationale: whole-Environment deletion must freeze Zones from the selected
	// immutable projection and release only their operational allocation state.
	ctx := context.Background()
	fixture, operationID := newHierarchyDeletionZoneFixture(t)
	addressesValue, err := testrecordcodec.Encode(
		"component_address_registry",
		testnetworkreservations.ComponentAddressRegistry{
			Reservations: map[string]string{},
		},
	)
	if err != nil {
		t.Fatalf("encode address registry: %v", err)
	}
	defer clear(addressesValue)
	seed, err := fixture.store.Transact(ctx, []testkeyvalue.Condition{{
		Key: testnetworkreservations.ComponentAddressRegistryKey(fixture.zone.Record.Desired.ID),
	}}, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: testnetworkreservations.ComponentAddressRegistryKey(fixture.zone.Record.Desired.ID), Value: addressesValue,
	}})
	if err != nil || !seed.Succeeded {
		t.Fatalf("seed Zone address registry = %#v/%v", seed, err)
	}
	primary, err := fixture.store.Get(ctx, testhierarchy.EnvironmentKey(fixture.environment.Record.ID))
	if err != nil || primary == nil || primary.Entry == nil {
		t.Fatalf("Environment primary = %#v/%v", primary, err)
	}
	defer clear(primary.Entry.Value)
	operation := testhierarchydeletion.HierarchyDeletionOperation{
		Tombstone: testhierarchydeletion.HierarchyDeletionTombstone{
			OperationID:   operationID,
			OperationKind: testhierarchydeletion.HierarchyDeletionOperationEnvironment,
			TargetKind:    testhierarchydeletion.HierarchyDeletionTargetEnvironment,
			TargetID:      fixture.environment.Record.ID, TargetRevision: primary.Entry.ModRevision,
			SnapshotRevision: seed.Revision, DeletionEpoch: 1,
		},
	}
	frozen, err := testhierarchydeletionplanning.NewPlanner(fixture.store).
		FreezeCapturedMembership(ctx, operation.Tombstone)
	if err != nil {
		t.Fatalf("FreezeCapturedMembership() error = %v", err)
	}
	nodes := frozen.Nodes
	var zoneNode *testhierarchydeletionplanning.HierarchyDeletionMembershipNode
	for index := range nodes {
		if nodes[index].ActionKind == testhierarchydeletion.HierarchyDeletionZoneRemove {
			if zoneNode != nil {
				t.Fatalf("duplicate Zone nodes = %#v", nodes)
			}
			zoneNode = &nodes[index]
		}
	}
	if zoneNode == nil || zoneNode.TargetID != fixture.zone.Record.Desired.ID ||
		zoneNode.TargetRevision != fixture.projection.Revision ||
		zoneNode.ProcedureInput.ControllerFinalizer == nil {
		t.Fatalf("projection Zone node = %#v", zoneNode)
	}
	procedure, err := testhierarchydeletionplanning.BindHierarchyDeletionControllerProcedure(
		testhierarchydeletionplanning.HierarchyDeletionPlannedAction{
			NodeID: zoneNode.NodeID, ActionKind: zoneNode.ActionKind,
			TargetKind: zoneNode.TargetKind, TargetID: zoneNode.TargetID,
			TargetRevision: zoneNode.TargetRevision,
		},
		*zoneNode.ProcedureInput.ControllerFinalizer,
	)
	if err != nil {
		t.Fatalf("bindHierarchyDeletionControllerProcedure() error = %v", err)
	}
	action := testhierarchydeletion.HierarchyDeletionAction{
		NodeID: zoneNode.NodeID, ActionKind: zoneNode.ActionKind,
		TargetKind: zoneNode.TargetKind, TargetID: zoneNode.TargetID,
		TargetRevision:      zoneNode.TargetRevision,
		ControllerProcedure: &procedure,
	}
	effects, err := testhierarchydeletionfinalization.NewPreparer(fixture.store).Prepare(ctx, operation, action)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	defer testkeyvalue.ClearByteSlices(effects.Values())
	if zoneNode.ProcedureInput.ControllerFinalizer.FixedInputDigest != effects.FixedInputDigest() {
		t.Fatalf(
			"projection Zone digest = %q, finalizer digest = %q",
			zoneNode.ProcedureInput.ControllerFinalizer.FixedInputDigest,
			effects.FixedInputDigest(),
		)
	}
	transaction, err := fixture.store.Transact(ctx, effects.Conditions(), effects.Mutations())
	if err != nil || !transaction.Succeeded {
		t.Fatalf("Zone finalizer transaction = %#v/%v", transaction, err)
	}
	for _, key := range []string{testnetworkreservations.ZonePoolRegistryKey(fixture.environment.Record.ID), testnetworkreservations.ComponentAddressRegistryKey(fixture.zone.Record.Desired.ID)} {
		if value := fixture.store.valueAt(key, fixture.store.revision); value != nil {
			t.Fatalf("Zone finalizer retained operational key %s", key)
		}
	}
	fixture.assertCurrentHead(t)
	retained, err := fixture.zones.GetZone(ctx, fixture.zone.Record.Desired.ID)
	if err != nil || retained.Revision != fixture.projection.Revision {
		t.Fatalf("GetZone(retained projection) = %#v/%v", retained, err)
	}
}

func newHierarchyDeletionZoneFixture(t *testing.T) (*zoneDeletionProjectionFixture, string) {
	t.Helper()
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	project, environment := zoneDeletionOwners(t, hierarchy, false)
	task := environmentBlueprintTestTask(t, project.Record, environment.Record, 15100)
	record, err := testzones.NewRecord(environment.Record.ID, core.Zone{
		ID: ids.NewAt(ids.KindNetwork, task.CreatedAt, 101), Name: "frontend", Subnet: "10.40.20.0/24",
		Internal: true, OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environment.Record.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	projection := environmentBlueprintTestProjection(environment.Record.ID, task, 1)
	projection.DesiredZones = []testenvironmentprojection.EnvironmentZoneProjection{{
		EnvironmentID: environment.Record.ID, Desired: record.Desired,
	}}
	projection = zonePublicationTestArtifact(t, projection, record)
	seedServiceRepositoryTestDesiredProjection(t, store, projection)
	projectionValue, err := testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(projection)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(projectionValue)
	headValue, err := testidempotency.EncodeTaskReference(projection.RevisionID)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(headValue)
	poolValue, err := testrecordcodec.Encode(
		"zone_pool_registry",
		testnetworkreservations.ZonePoolRegistry{Reservations: map[string]string{
			record.Desired.ID: record.Desired.Subnet,
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(poolValue)
	seed, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testblueprints.EnvironmentBlueprintHeadKey(environment.Record.ID),
			Value: headValue,
		},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testenvironmentprojection.EnvironmentComposeProjectionStorageKey(environment.Record.ID),
			Value: projectionValue,
		},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testnetworkreservations.ZonePoolRegistryKey(environment.Record.ID),
			Value: poolValue,
		},
	})
	if err != nil || !seed.Succeeded {
		t.Fatalf("seed hierarchy Zone projection = %#v/%v", seed, err)
	}
	selected, found, err := hierarchy.GetEnvironmentComposeProjection(ctx, environment.Record.ID)
	if err != nil || !found {
		t.Fatalf("GetEnvironmentComposeProjection() = %#v/%v/%v", selected, found, err)
	}
	zone, err := testenvironmentqueries.JoinZone(selected, selected.Record.DesiredZones[0])
	if err != nil {
		t.Fatal(err)
	}
	zones, err := newZoneRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	return &zoneDeletionProjectionFixture{
		store: store, hierarchy: hierarchy, zones: zones, project: project,
		environment: environment, zone: zone, projection: selected,
	}, ids.NewAt(ids.KindOperation, task.CreatedAt, 102)
}
