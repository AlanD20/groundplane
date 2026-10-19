package etcd

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func secretMetadataStore(t *testing.T) (*memoryHierarchyStore, keyvalue.Versioned[hierarchy.ProjectRecord]) {
	t.Helper()
	store := newMemoryHierarchyStore()
	repository, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 8, 22, 20, 0, 0, 0, time.UTC)
	tenantID := ids.NewAt(ids.KindTenant, at, 1)
	if _, err := repository.CreateTenant(t.Context(), hierarchy.TenantRecord{
		ID: tenantID, Slug: "secret-tenant", Name: "Secret Tenant",
	}); err != nil {
		t.Fatal(err)
	}
	project, err := repository.CreateProject(t.Context(), hierarchy.ProjectRecord{
		ID: ids.NewAt(ids.KindProject, at, 2), TenantID: tenantID,
		Slug: "secret-project", Name: "Secret Project", Kind: hierarchy.ProjectKindTenant,
	})
	if err != nil {
		t.Fatal(err)
	}
	return store, project
}
