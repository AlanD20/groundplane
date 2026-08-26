package etcd

import (
	"context"
	"testing"
)

// Rationale: a clean installation has no Zone registry yet; the fixed-
// revision capacity read must project that absence as an empty allocation.
func TestListZoneSubnetReservationsAtRevisionTreatsMissingRegistryAsEmpty(t *testing.T) {
	t.Parallel()
	store := newMemoryHierarchyStore()
	store.revision = 1
	repository, err := newZoneRepository(store)
	if err != nil {
		t.Fatalf("newZoneRepository() error = %v", err)
	}
	got, err := repository.ListZoneSubnetReservationsAtRevision(
		context.Background(), "env_01ARZ3NDEKTSV4RRFFQ69G5FAV", 1,
	)
	if err != nil {
		t.Fatalf("ListZoneSubnetReservationsAtRevision() error = %v", err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("ListZoneSubnetReservationsAtRevision() = %#v", got)
	}
}
