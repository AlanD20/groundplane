package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func environmentBlueprintTestZoneChanges(
	t *testing.T,
	repository *HierarchyRepository,
	projection EnvironmentComposeProjection,
) []EnvironmentBlueprintZoneChange {
	t.Helper()
	zones, err := newZoneRepository(repository.store)
	if err != nil {
		t.Fatalf("newZoneRepository() error = %v", err)
	}
	desired := core.Zone{
		ID: projection.Networks[0].ID, Name: projection.Networks[0].Name,
		Subnet: "10.40.10.0/24", Internal: true, OwnedBy: projection.EnvironmentID,
	}
	current, err := zones.GetZone(context.Background(), desired.ID)
	if err == nil {
		currentCopy := current
		return []EnvironmentBlueprintZoneChange{{Current: &currentCopy, Record: current.Record}}
	}
	if !isKind(err, errs.KindZoneNotFound) {
		t.Fatalf("GetZone() error = %v", err)
	}
	record, recordErr := NewZoneRecord(projection.EnvironmentID, desired)
	if recordErr != nil {
		t.Fatalf("NewZoneRecord() error = %v", recordErr)
	}
	return []EnvironmentBlueprintZoneChange{{Record: record}}
}

func assertEnvironmentBlueprintZoneRevision(
	t *testing.T,
	store *memoryHierarchyStore,
	projection EnvironmentComposeProjection,
	wantRevision int64,
) {
	t.Helper()
	zones, err := newZoneRepository(store)
	if err != nil {
		t.Fatalf("newZoneRepository() error = %v", err)
	}
	zone, err := zones.GetZone(context.Background(), projection.Networks[0].ID)
	if err != nil || zone.Record.Desired.Subnet != "10.40.10.0/24" || zone.Revision != wantRevision {
		t.Fatalf("GetZone() = %#v, %v", zone, err)
	}
	registry, err := zones.getZonePoolRegistry(context.Background(), projection.EnvironmentID)
	if err != nil || registry.Record.Reservations[zone.Record.Desired.ID] != zone.Record.Desired.Subnet ||
		registry.Revision != wantRevision {
		t.Fatalf("Zone pool registry = %#v, %v", registry, err)
	}
}

func TestEnvironmentBlueprintZonePreparationRejectsOverlappingSubnets(t *testing.T) {
	// Rationale: a Blueprint transaction must reject sibling overlap before it
	// can publish any partial Zone, Service, Route, Task, or head state.
	t.Parallel()
	repository, err := newHierarchyRepository(newMemoryHierarchyStore())
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	_, environment := createEnvironmentBlueprintOwners(t, repository)
	projection := EnvironmentComposeProjection{
		EnvironmentID: environment.Record.ID,
		Networks: []EnvironmentComposeIdentity{
			{ID: ids.NewAt(ids.KindNetwork, serviceRecordTestTime(), 801), Name: "one"},
			{ID: ids.NewAt(ids.KindNetwork, serviceRecordTestTime(), 802), Name: "two"},
		},
	}
	changes := make([]EnvironmentBlueprintZoneChange, 0, 2)
	for index, identity := range projection.Networks {
		subnet := "10.40.10.0/24"
		if index == 1 {
			subnet = "10.40.10.128/25"
		}
		record, recordErr := NewZoneRecord(environment.Record.ID, core.Zone{
			ID: identity.ID, Name: identity.Name, Subnet: subnet, OwnedBy: environment.Record.ID,
		})
		if recordErr != nil {
			t.Fatalf("NewZoneRecord() error = %v", recordErr)
		}
		changes = append(changes, EnvironmentBlueprintZoneChange{Record: record})
	}
	if _, err := repository.prepareEnvironmentBlueprintZoneChanges(
		context.Background(), environment, projection, changes,
	); err == nil {
		t.Fatal("prepareEnvironmentBlueprintZoneChanges() accepted overlapping subnets")
	}
}
