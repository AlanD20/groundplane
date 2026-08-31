package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: only the atomic singleton bootstrap transaction may authorize a
// missing-observation first activation, and any later record write invalidates
// that authority even when runtime fields still look empty.
func TestPlatformComponentBootstrapProvenanceIsRepositoryOwned(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repository, err := newComponentRepository(newMemoryTaskStore())
	if err != nil {
		t.Fatalf("NewComponentRepository() error = %v", err)
	}
	records, err := DefaultPlatformComponents(false)
	if err != nil {
		t.Fatalf("DefaultPlatformComponents() error = %v", err)
	}
	created, err := repository.EnsurePlatformComponents(ctx, records)
	if err != nil {
		t.Fatalf("EnsurePlatformComponents() error = %v", err)
	}
	found, err := repository.HasPlatformComponentBootstrapProvenance(ctx, created[0])
	if err != nil || !found {
		t.Fatalf("HasPlatformComponentBootstrapProvenance() = %t, %v, want true, nil", found, err)
	}

	updated, err := repository.ReplacePlatformRuntime(ctx, created[0], nil, "", false)
	if err != nil {
		t.Fatalf("ReplacePlatformRuntime() error = %v", err)
	}
	_, err = repository.HasPlatformComponentBootstrapProvenance(ctx, updated)
	kind, found := errs.KindOf(err)
	if !found || kind != errs.KindStateConflict {
		t.Fatalf("HasPlatformComponentBootstrapProvenance() error = %v, want stale state conflict", err)
	}

	directRepository, err := newComponentRepository(newMemoryTaskStore())
	if err != nil {
		t.Fatalf("NewComponentRepository(direct) error = %v", err)
	}
	direct, err := directRepository.CreatePlatformComponent(ctx, records[0])
	if err != nil {
		t.Fatalf("CreatePlatformComponent() error = %v", err)
	}
	found, err = directRepository.HasPlatformComponentBootstrapProvenance(ctx, direct)
	if err != nil || found {
		t.Fatalf("HasPlatformComponentBootstrapProvenance(direct) = %t, %v, want false, nil", found, err)
	}
}
