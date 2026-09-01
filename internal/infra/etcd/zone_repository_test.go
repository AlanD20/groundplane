package etcd

import (
	"context"
	"fmt"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
)

func TestZoneRepositoryReadsAndPagesSelectedProjection(t *testing.T) {
	// Rationale: Zone reads must join only the selected Environment head and
	// preserve the fixed-revision cursor contract.
	t.Parallel()
	ctx := context.Background()
	repository, store, environment := zoneRepositoryTestHierarchy(t)
	zones := []core.Zone{
		zoneRepositoryTestZone(environment.Record.ID, 910, "backend"),
		zoneRepositoryTestZone(environment.Record.ID, 911, "frontend"),
	}
	projection := zoneRepositoryTestProjection(t, environment.Record.ID, 1001, zones...)
	seedServiceRepositoryTestDesiredProjection(t, store, projection)

	stored, err := repository.GetZone(ctx, zones[0].ID)
	if err != nil || stored.Record.Desired != zones[0] || stored.Record.EnvironmentID != environment.Record.ID {
		t.Fatalf("GetZone() = %#v, %v", stored, err)
	}
	first, err := repository.ListZones(ctx, environment.Record.ID, PageRequest{Limit: 1})
	if err != nil || len(first.Items) != 1 || first.NextCursor == "" || first.Items[0].Record.Desired.Name != "backend" {
		t.Fatalf("ListZones(first) = %#v, %v", first, err)
	}
	second, err := repository.ListZones(
		ctx,
		environment.Record.ID,
		PageRequest{Limit: 1, Cursor: first.NextCursor},
	)
	if err != nil || len(second.Items) != 1 || second.NextCursor != "" || second.Revision != first.Revision ||
		second.Items[0].Record.Desired.Name != "frontend" {
		t.Fatalf("ListZones(second) = %#v, %v", second, err)
	}
}

func TestZoneRepositoryListPinsSelectedHeadRevision(t *testing.T) {
	// Rationale: a fixed-revision read must retain the old immutable head even
	// after a newer Environment projection is published.
	t.Parallel()
	ctx := context.Background()
	repository, store, environment := zoneRepositoryTestHierarchy(t)
	initial := []core.Zone{
		zoneRepositoryTestZone(environment.Record.ID, 920, "backend"),
		zoneRepositoryTestZone(environment.Record.ID, 921, "frontend"),
	}
	seedServiceRepositoryTestDesiredProjection(
		t, store, zoneRepositoryTestProjection(t, environment.Record.ID, 1002, initial...),
	)
	head, err := store.Get(ctx, environmentBlueprintHeadKey(environment.Record.ID))
	if err != nil || head == nil || head.Entry == nil {
		t.Fatalf("read initial Environment head = %#v, %v", head, err)
	}
	fixedRevision := head.ReadRevision

	latest := append([]core.Zone(nil), initial...)
	latest = append(latest, zoneRepositoryTestZone(environment.Record.ID, 922, "worker"))
	seedServiceRepositoryTestDesiredProjection(
		t, store, zoneRepositoryTestProjection(t, environment.Record.ID, 1003, latest...),
	)

	current, err := repository.ListZones(ctx, environment.Record.ID, PageRequest{Limit: 10})
	if err != nil || len(current.Items) != 3 {
		t.Fatalf("ListZones(current) = %#v, %v", current, err)
	}
	fixed, err := repository.ListZones(ctx, environment.Record.ID, PageRequest{
		Limit: 10, Revision: fixedRevision,
	})
	if err != nil || len(fixed.Items) != 2 || fixed.Revision != fixedRevision ||
		fixed.Items[0].Record.Desired.Name != "backend" || fixed.Items[1].Record.Desired.Name != "frontend" {
		t.Fatalf("ListZones(fixed) = %#v, %v", fixed, err)
	}
}

func zoneRepositoryTestProjection(
	t *testing.T,
	environmentID string,
	revisionOffset int64,
	zones ...core.Zone,
) EnvironmentComposeProjection {
	t.Helper()
	projection := EnvironmentComposeProjection{
		EnvironmentID:    environmentID,
		RevisionID:       ids.NewAt(ids.KindTask, serviceRecordTestTime(), revisionOffset),
		RenderGeneration: 1,
	}
	for _, zone := range zones {
		projection.DesiredZones = append(projection.DesiredZones, EnvironmentZoneProjection{
			EnvironmentID: environmentID, Desired: zone,
		})
	}
	return withTestEnvironmentComposeArtifact(projection)
}

func zoneRepositoryTestHierarchy(
	t *testing.T,
) (*ZoneRepository, *memoryHierarchyStore, Versioned[EnvironmentRecord]) {
	t.Helper()
	_, store, environment, _ := serviceRepositoryTestHierarchy(t)
	repository, err := newZoneRepository(store)
	if err != nil {
		t.Fatalf("newZoneRepository() error = %v", err)
	}
	return repository, store, environment
}

func zoneRepositoryTestZone(environmentID string, offset int64, name string) core.Zone {
	return core.Zone{
		ID: ids.NewAt(ids.KindNetwork, serviceRecordTestTime(), offset), Name: name,
		Subnet: fmt.Sprintf("10.34.%d.0/24", offset-900), Internal: true,
		OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environmentID,
	}
}
