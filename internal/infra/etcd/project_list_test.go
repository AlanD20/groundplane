package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

func TestHierarchyListProjectsFiltersMixedKindsWithStableCursor(t *testing.T) {
	t.Parallel()

	repository, err := newHierarchyRepository(newMemoryHierarchyStore())
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	tenant := TenantRecord{ID: hierarchyTestID(ids.KindTenant, 311), Slug: "acme", Name: "Acme"}
	if _, err := repository.CreateTenant(context.Background(), tenant); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	projects := []ProjectRecord{
		{
			ID:       hierarchyTestID(ids.KindProject, 312),
			TenantID: tenant.ID,
			Slug:     "api",
			Name:     "API",
			Kind:     ProjectKindTenant,
		},
		{ID: hierarchyTestID(ids.KindProject, 313), Slug: "postgres", Name: "Postgres", Kind: ProjectKindBacking},
		{ID: hierarchyTestID(ids.KindProject, 314), Slug: "valkey", Name: "Valkey", Kind: ProjectKindBacking},
	}
	for _, project := range projects {
		if _, err := repository.CreateProject(context.Background(), project); err != nil {
			t.Fatalf("CreateProject(%s) error = %v", project.Slug, err)
		}
	}
	first, err := repository.ListProjects(
		context.Background(), ProjectFilter{Kind: ProjectKindBacking}, PageRequest{Limit: 1},
	)
	if err != nil || len(first.Items) != 1 || first.Items[0].Record.Slug != "postgres" || first.NextCursor == "" {
		t.Fatalf("ListProjects(first) = %#v, %v", first, err)
	}
	second, err := repository.ListProjects(
		context.Background(), ProjectFilter{Kind: ProjectKindBacking},
		PageRequest{Limit: 1, Cursor: first.NextCursor},
	)
	if err != nil || len(second.Items) != 1 || second.Items[0].Record.Slug != "valkey" || second.NextCursor != "" ||
		second.Revision != first.Revision {
		t.Fatalf("ListProjects(second) = %#v, %v", second, err)
	}
}
