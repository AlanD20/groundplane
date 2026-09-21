package etcd

import (
	"context"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"net/http"
	"testing"
)

// Rationale: an Attach rename must move the Environment-scoped name index and publish the completed
// direct-mutation replay marker in the same transaction, never exposing two names or an untracked rename.
func TestRenameAttachIdempotentMovesTheScopedNameAtomically(t *testing.T) {
	ctx := context.Background()
	store := newAttachTestStore()
	repository, err := NewAttachRepository(store)
	if err != nil {
		t.Fatalf("NewAttachRepository() error = %v", err)
	}
	scope := seedAttachScope(t, ctx, store)
	record, facts := testPendingAttach(t, scope, 91, "original-database", nil)
	current := createTestAttach(t, ctx, repository, scope, record, &facts)
	hierarchy, err := NewHierarchyRepository(store)
	if err != nil {
		t.Fatalf("NewHierarchyRepository() error = %v", err)
	}
	environment, err := hierarchy.GetEnvironment(ctx, current.Record.EnvironmentID)
	if err != nil {
		t.Fatalf("GetEnvironment() error = %v", err)
	}
	project, err := hierarchy.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		t.Fatalf("GetProject() error = %v", err)
	}
	marker := testDirectMarker()
	marker.Locator = testidempotency.IdempotencyLocator{
		ScopeKind: testidempotency.IdempotencyScopeEnvironment,
		ScopeID:   current.Record.EnvironmentID,
		Method:    http.MethodPost,
		Route:     "/attaches/{id}/rename",
		Key:       "attach-rename-key-0001",
	}
	marker.ReplayTarget = &testidempotency.IdempotencyReplayTarget{
		Kind: testidempotency.IdempotencyReplayTargetAttach,
		ID:   current.Record.ID,
	}
	result, err := repository.RenameAttachIdempotent(
		ctx, environment, project, current, "renamed-database", marker,
	)
	if err != nil {
		t.Fatalf("RenameAttachIdempotent() error = %v", err)
	}
	if result.kind != idempotencyTransactionApplied {
		t.Fatalf("RenameAttachIdempotent() kind = %d, conflict = %v", result.kind, result.conflict)
	}
	renamed, err := repository.ResolveAttach(ctx, current.Record.EnvironmentID, "renamed-database")
	if err != nil || renamed.Record.ID != current.Record.ID || renamed.Record.Name != "renamed-database" {
		t.Fatalf("ResolveAttach(renamed) = %#v, %v", renamed, err)
	}
	if _, err := repository.ResolveAttach(ctx, current.Record.EnvironmentID, "original-database"); err == nil {
		t.Fatal("ResolveAttach(original) succeeded after rename")
	}
}
