package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
)

func TestHierarchyProjectDescriptionRoundTrips(t *testing.T) {
	t.Parallel()

	repository, err := newHierarchyRepository(newMemoryHierarchyStore())
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	tenant := testhierarchy.TenantRecord{ID: hierarchyTestID(ids.KindTenant, 301), Slug: "acme", Name: "Acme"}
	if _, err := repository.CreateTenant(context.Background(), tenant); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	record := testhierarchy.ProjectRecord{
		ID: hierarchyTestID(ids.KindProject, 302), TenantID: tenant.ID,
		Slug: "console", Name: "Console", Description: "Operator interface", Kind: testhierarchy.ProjectKindTenant,
	}
	created, err := repository.CreateProject(context.Background(), record)
	if err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	read, err := repository.GetProject(context.Background(), record.ID)
	if err != nil || created.Record != record || read.Record != record {
		t.Fatalf("Project round trip = %#v/%#v, %v", created.Record, read.Record, err)
	}
}
