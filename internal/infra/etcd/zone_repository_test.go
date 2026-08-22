package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestZoneRepositoryCreatesReadsAndPagesScopedRecords(t *testing.T) {
	// Rationale: one atomic write must publish the Zone primary, scoped name,
	// and Environment membership consumed by stable-id and fixed-revision reads.
	t.Parallel()
	ctx := context.Background()
	repository, _, environment, project := zoneRepositoryTestHierarchy(t)
	records := []ZoneRecord{
		zoneRepositoryTestRecord(t, environment.Record.ID, 910, "backend"),
		zoneRepositoryTestRecord(t, environment.Record.ID, 911, "frontend"),
	}
	for _, record := range records {
		if _, err := repository.CreateZone(ctx, environment, project, record); err != nil {
			t.Fatalf("CreateZone(%s) error = %v", record.Desired.Name, err)
		}
	}
	stored, err := repository.GetZone(ctx, records[0].Desired.ID)
	if err != nil || stored.Record != records[0] {
		t.Fatalf("GetZone() = %#v, %v", stored, err)
	}
	first, err := repository.ListZones(ctx, environment.Record.ID, PageRequest{Limit: 1})
	if err != nil || len(first.Items) != 1 || first.NextCursor == "" {
		t.Fatalf("ListZones(first) = %#v, %v", first, err)
	}
	second, err := repository.ListZones(
		ctx,
		environment.Record.ID,
		PageRequest{Limit: 1, Cursor: first.NextCursor},
	)
	if err != nil || len(second.Items) != 1 || second.NextCursor != "" || second.Revision != first.Revision {
		t.Fatalf("ListZones(second) = %#v, %v", second, err)
	}
}

func TestZoneRepositoryEnforcesScopedNameAndAncestorFences(t *testing.T) {
	// Rationale: Zone creation must atomically reject duplicate names and every
	// deletion fence in its immutable hierarchy.
	t.Parallel()
	ctx := context.Background()
	repository, store, environment, project := zoneRepositoryTestHierarchy(t)
	first := zoneRepositoryTestRecord(t, environment.Record.ID, 920, "backend")
	if _, err := repository.CreateZone(ctx, environment, project, first); err != nil {
		t.Fatalf("CreateZone(first) error = %v", err)
	}
	duplicate := zoneRepositoryTestRecord(t, environment.Record.ID, 921, "backend")
	if _, err := repository.CreateZone(ctx, environment, project, duplicate); !isKind(err, errs.KindNameConflict) {
		t.Fatalf("CreateZone(duplicate) error = %v", err)
	}
	if _, err := store.Transact(
		ctx,
		[]Condition{{Key: deletionTombstoneKey("environment", environment.Record.ID)}},
		[]Mutation{{
			Type: MutationPut, Key: deletionTombstoneKey("environment", environment.Record.ID), Value: []byte("fenced"),
		}},
	); err != nil {
		t.Fatalf("install Environment fence: %v", err)
	}
	fenced := zoneRepositoryTestRecord(t, environment.Record.ID, 922, "egress")
	if _, err := repository.CreateZone(ctx, environment, project, fenced); !isKind(err, errs.KindResourceInUse) {
		t.Fatalf("CreateZone(fenced) error = %v", err)
	}
}

func TestZoneRepositoryUpdatesMutableDesiredFieldsByCAS(t *testing.T) {
	// Rationale: Blueprint and direct edits must serialize on one Zone revision
	// without changing its identity, scoped name, or owner.
	t.Parallel()
	ctx := context.Background()
	repository, _, environment, project := zoneRepositoryTestHierarchy(t)
	record := zoneRepositoryTestRecord(t, environment.Record.ID, 930, "backend")
	current, err := repository.CreateZone(ctx, environment, project, record)
	if err != nil {
		t.Fatalf("CreateZone() error = %v", err)
	}
	desired := current.Record.Desired
	desired.Subnet = "10.200.30.0/24"
	desired.Internal = false
	updated, err := repository.ReplaceDesired(ctx, environment, project, current, desired)
	if err != nil || updated.Record.Desired.Subnet != desired.Subnet || updated.Record.Desired.Internal {
		t.Fatalf("ReplaceDesired() = %#v, %v", updated, err)
	}
	if _, err := repository.ReplaceDesired(
		ctx,
		environment,
		project,
		current,
		desired,
	); !isKind(
		err,
		errs.KindStateConflict,
	) {
		t.Fatalf("ReplaceDesired(stale) error = %v", err)
	}
}

func zoneRepositoryTestHierarchy(
	t *testing.T,
) (*ZoneRepository, *memoryHierarchyStore, Versioned[EnvironmentRecord], Versioned[ProjectRecord]) {
	t.Helper()
	_, store, environment, project := serviceRepositoryTestHierarchy(t)
	repository, err := newZoneRepository(store)
	if err != nil {
		t.Fatalf("newZoneRepository() error = %v", err)
	}
	return repository, store, environment, project
}

func zoneRepositoryTestRecord(
	t *testing.T,
	environmentID string,
	offset int64,
	name string,
) ZoneRecord {
	t.Helper()
	record, err := NewZoneRecord(environmentID, core.Zone{
		ID: ids.NewAt(ids.KindNetwork, serviceRecordTestTime(), offset), Name: name,
		Subnet: "10.200.20.0/24", Internal: true, OwnedBy: "console",
	})
	if err != nil {
		t.Fatalf("NewZoneRecord() error = %v", err)
	}
	return record
}
