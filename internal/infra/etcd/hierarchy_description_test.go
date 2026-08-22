package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

func TestHierarchyTenantDescriptionRoundTrips(t *testing.T) {
	t.Parallel()

	repository, err := newHierarchyRepository(newMemoryHierarchyStore())
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	record := TenantRecord{
		ID: hierarchyTestID(ids.KindTenant, 101), Slug: "acme", Name: "Acme",
		Description: "Production workloads",
	}
	created, err := repository.CreateTenant(context.Background(), record)
	if err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	read, err := repository.GetTenant(context.Background(), record.ID)
	if err != nil {
		t.Fatalf("GetTenant() error = %v", err)
	}
	if created.Record != record || read.Record != record {
		t.Fatalf("Tenant round trip = %#v/%#v", created.Record, read.Record)
	}
}
