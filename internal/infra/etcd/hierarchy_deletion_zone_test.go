package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
)

func TestEnvironmentHierarchyDeletionFreezesProjectionZonesAndReleasesPool(t *testing.T) {
	// Rationale: whole-Environment deletion must freeze Zones from the selected
	// immutable projection and release only their operational allocation state.
	ctx := context.Background()
	fixture, operationID := newHierarchyDeletionZoneFixture(t)
	addressesValue, err := encodeEnvelope("component_address_registry", componentAddressRegistry{
		Reservations: map[string]string{},
	})
	if err != nil {
		t.Fatalf("encode address registry: %v", err)
	}
	defer clear(addressesValue)
	seed, err := fixture.store.Transact(ctx, []Condition{{
		Key: componentAddressRegistryKey(fixture.zone.Record.Desired.ID),
	}}, []Mutation{{
		Type: MutationPut, Key: componentAddressRegistryKey(fixture.zone.Record.Desired.ID), Value: addressesValue,
	}})
	if err != nil || !seed.Succeeded {
		t.Fatalf("seed Zone address registry = %#v/%v", seed, err)
	}
	primary, err := fixture.store.Get(ctx, environmentKey(fixture.environment.Record.ID))
	if err != nil || primary == nil || primary.Entry == nil {
		t.Fatalf("Environment primary = %#v/%v", primary, err)
	}
	defer clear(primary.Entry.Value)
	journal, err := newHierarchyDeletionRepository(fixture.store)
	if err != nil {
		t.Fatalf("newHierarchyDeletionRepository() error = %v", err)
	}
	operation := HierarchyDeletionOperation{Tombstone: HierarchyDeletionTombstone{
		OperationID: operationID, SnapshotRevision: seed.Revision,
	}}
	nodes, err := journal.freezeEnvironmentMembership(
		ctx, operation, fixture.environment.Record.ID, primary.Entry.ModRevision,
		hierarchyDeletionBytesDigest(primary.Entry.Value),
	)
	if err != nil {
		t.Fatalf("freezeEnvironmentMembership() error = %v", err)
	}
	var zoneNode *HierarchyDeletionMembershipNode
	for index := range nodes {
		if nodes[index].ActionKind == HierarchyDeletionZoneRemove {
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
	evidence, err := newHierarchyDeletionZoneEvidence(
		fixture.projection,
		EnvironmentZoneProjection{
			EnvironmentID: fixture.zone.Record.EnvironmentID,
			Desired:       fixture.zone.Record.Desired,
		},
	)
	if err != nil {
		t.Fatalf("newHierarchyDeletionZoneEvidence() error = %v", err)
	}
	zoneValue, err := encodeHierarchyDeletionZoneEvidence(evidence)
	if err != nil {
		t.Fatalf("encodeHierarchyDeletionZoneEvidence() error = %v", err)
	}
	if zoneNode.ProcedureInput.ControllerFinalizer.FixedInputDigest != hierarchyDeletionBytesDigest(zoneValue) {
		clear(zoneValue)
		t.Fatalf("projection Zone digest = %q", zoneNode.ProcedureInput.ControllerFinalizer.FixedInputDigest)
	}
	clear(zoneValue)
	procedure, err := bindHierarchyDeletionControllerProcedure(HierarchyDeletionPlannedAction{
		NodeID: zoneNode.NodeID, ActionKind: zoneNode.ActionKind,
		TargetKind: zoneNode.TargetKind, TargetID: zoneNode.TargetID,
		TargetRevision: zoneNode.TargetRevision,
	}, *zoneNode.ProcedureInput.ControllerFinalizer)
	if err != nil {
		t.Fatalf("bindHierarchyDeletionControllerProcedure() error = %v", err)
	}
	action := HierarchyDeletionAction{
		NodeID: zoneNode.NodeID, ActionKind: zoneNode.ActionKind,
		TargetKind: zoneNode.TargetKind, TargetID: zoneNode.TargetID,
		TargetRevision:      zoneNode.TargetRevision,
		ControllerProcedure: &procedure,
	}
	effects, err := journal.prepareHierarchyDeletionControllerEffects(ctx, operation, action)
	if err != nil {
		t.Fatalf("prepareHierarchyDeletionControllerEffects() error = %v", err)
	}
	transaction, err := fixture.store.Transact(ctx, effects.conditions, effects.mutations)
	if err != nil || !transaction.Succeeded {
		t.Fatalf("Zone finalizer transaction = %#v/%v", transaction, err)
	}
	for _, key := range []string{
		zonePoolRegistryKey(fixture.environment.Record.ID),
		componentAddressRegistryKey(fixture.zone.Record.Desired.ID),
	} {
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
	record, err := NewZoneRecord(environment.Record.ID, core.Zone{
		ID: ids.NewAt(ids.KindNetwork, task.CreatedAt, 101), Name: "frontend", Subnet: "10.40.20.0/24",
		Internal: true, OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environment.Record.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	projection := environmentBlueprintTestProjection(environment.Record.ID, task, 1)
	projection.DesiredZones = []EnvironmentZoneProjection{{
		EnvironmentID: environment.Record.ID, Desired: record.Desired,
	}}
	projection = zonePublicationTestArtifact(t, projection, record)
	seedServiceRepositoryTestDesiredProjection(t, store, projection)
	projectionValue, err := encodeEnvironmentComposeProjection(projection)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(projectionValue)
	headValue, err := encodeTaskReference(projection.RevisionID)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(headValue)
	poolValue, err := encodeEnvelope("zone_pool_registry", zonePoolRegistry{Reservations: map[string]string{
		record.Desired.ID: record.Desired.Subnet,
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer clear(poolValue)
	seed, err := store.Transact(ctx, nil, []Mutation{
		{Type: MutationPut, Key: environmentBlueprintHeadKey(environment.Record.ID), Value: headValue},
		{Type: MutationPut, Key: environmentComposeProjectionKey(environment.Record.ID), Value: projectionValue},
		{Type: MutationPut, Key: zonePoolRegistryKey(environment.Record.ID), Value: poolValue},
	})
	if err != nil || !seed.Succeeded {
		t.Fatalf("seed hierarchy Zone projection = %#v/%v", seed, err)
	}
	selected, found, err := hierarchy.GetEnvironmentComposeProjection(ctx, environment.Record.ID)
	if err != nil || !found {
		t.Fatalf("GetEnvironmentComposeProjection() = %#v/%v/%v", selected, found, err)
	}
	zone, err := joinEnvironmentZone(selected, selected.Record.DesiredZones[0])
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
