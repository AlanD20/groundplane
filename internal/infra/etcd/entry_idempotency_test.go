package etcd

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
)

// Rationale: Entry create and its immutable generation must be committed in
// the same transaction as the exact replay marker, never by check then write.
func TestEntryCreateIdempotentCommitsMetadataGenerationAndMarker(t *testing.T) {
	t.Parallel()
	store, environment, project := testEntryRepositoryHierarchy(t)
	repository, err := newEntryRepository(store)
	if err != nil {
		t.Fatalf("newEntryRepository() error = %v", err)
	}
	now := time.Date(2026, 8, 22, 20, 0, 0, 0, time.UTC)
	entryID := ids.NewAt(ids.KindEnvEntry, now, 41)
	generationID := ids.NewAt(ids.KindConfig, now, 42)
	entry := core.EnvEntry{
		ID: entryID, Kind: core.EntryKindEnv, Key: "APP_ENV",
		Source:   core.EntrySource{Kind: core.SourceLiteral, Literal: "production"},
		Exposure: []string{"all"},
	}
	record, err := NewEntryRecord(environment.Record.ID, entry, generationID)
	if err != nil {
		t.Fatalf("NewEntryRecord() error = %v", err)
	}
	generation := testPlainGeneration(environment.Record.ID, entryID, generationID, "production", now)
	marker := testDirectMarker()
	marker.Locator = IdempotencyLocator{
		ScopeKind: IdempotencyScopeEnvironment, ScopeID: environment.Record.ID,
		Method: http.MethodPost, Route: "/entries", Key: "entry-create-key-0001",
	}
	marker.Response = IdempotencyResponse{
		Status: http.StatusCreated, ContentKind: "application/json", Body: []byte(`{"id":"entry"}`),
	}
	result, err := repository.CreateEntryIdempotent(
		context.Background(), environment, project, record, EntryValueGeneration{Plain: &generation}, marker,
	)
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("CreateEntryIdempotent() = %#v, %v", result, err)
	}
	replayed, err := repository.CreateEntryIdempotent(
		context.Background(), environment, project, record, EntryValueGeneration{Plain: &generation}, marker,
	)
	if err != nil || replayed.kind != idempotencyTransactionExisting {
		t.Fatalf("CreateEntryIdempotent(replay) = %#v, %v", replayed, err)
	}
	stored, err := repository.GetEntry(context.Background(), entryID)
	if err != nil || !equalEntryRecord(stored.Record, record) {
		t.Fatalf("GetEntry() = %#v, %v", stored, err)
	}
}
