package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testentries "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	testentryvalues "github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestEntryRepositoryPersistsAndReplacesPlainGenerationAtomically(t *testing.T) {
	// Rationale: an Entry primary must never select bytes that were not
	// committed with it, and an edit must preserve the older generation used by retries.
	store, environment, project := testEntryRepositoryHierarchy(t)
	repository, err := newEntryRepository(store)
	if err != nil {
		t.Fatalf("newEntryRepository() error = %v", err)
	}
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	entryID := ids.NewAt(ids.KindEnvEntry, now, 10)
	firstID := ids.NewAt(ids.KindConfig, now, 11)
	entry := core.EnvEntry{
		ID: entryID, Kind: core.EntryKindEnv, Key: "APP_ENV",
		Source:   core.EntrySource{Kind: core.SourceLiteral, Literal: "staging"},
		Exposure: []string{"all"},
	}
	record, err := testentries.NewRecord(environment.Record.ID, entry, firstID)
	if err != nil {
		t.Fatalf("NewEntryRecord() error = %v", err)
	}
	first := testPlainGeneration(environment.Record.ID, entryID, firstID, "staging", now)
	created, err := repository.CreateEntry(
		context.Background(), environment, project, record, testentries.EntryValueGeneration{Plain: &first},
	)
	if err != nil {
		t.Fatalf("CreateEntry() error = %v", err)
	}

	secondID := ids.NewAt(ids.KindConfig, now.Add(time.Second), 12)
	entry.Source.Literal = "production"
	second := testPlainGeneration(
		environment.Record.ID, entryID, secondID, "production", now.Add(time.Second),
	)
	replaced, err := repository.ReplaceEntry(
		context.Background(),
		environment,
		project,
		created,
		entry,
		secondID,
		testentries.EntryValueGeneration{Plain: &second},
	)
	if err != nil {
		t.Fatalf("ReplaceEntry() error = %v", err)
	}
	if replaced.Record.CurrentValueGenerationID != secondID ||
		replaced.Record.Entry.Source.Literal != "production" {
		t.Fatalf("ReplaceEntry() = %#v", replaced.Record)
	}
	valueRepository, err := testentryvalues.New(&releaseLogMemoryStore{store})
	if err != nil {
		t.Fatalf("newEntryValueGenerationRepository() error = %v", err)
	}
	old, found, err := valueRepository.GetPlain(context.Background(), entryID, firstID)
	if err != nil || !found || string(old.Content) != "staging" {
		t.Fatalf("old generation = found %v record %#v error %v", found, old, err)
	}
	clear(old.Content)
	page, err := repository.ListEntries(
		context.Background(), environment.Record.ID, testkeyvalue.PageRequest{Limit: 10},
	)
	if err != nil || len(page.Items) != 1 || page.Items[0].Record.Entry.ID != entryID {
		t.Fatalf("ListEntries() = %#v, %v", page, err)
	}
}

// Rationale: an Entry edit must publish its replacement primary, immutable
// value generation, and exact replay evidence in one transaction so neither
// a retry nor a crash can select bytes without the matching desired metadata.
func TestEntryRepositoryReplacesGenerationIdempotently(t *testing.T) {
	t.Parallel()
	store, environment, project := testEntryRepositoryHierarchy(t)
	repository, err := newEntryRepository(store)
	if err != nil {
		t.Fatalf("newEntryRepository() error = %v", err)
	}
	now := time.Date(2026, 8, 23, 4, 10, 0, 0, time.UTC)
	entryID := ids.NewAt(ids.KindEnvEntry, now, 40)
	firstID := ids.NewAt(ids.KindConfig, now, 41)
	entry := core.EnvEntry{
		ID: entryID, Kind: core.EntryKindEnv, Key: "APP_ENV",
		Source:   core.EntrySource{Kind: core.SourceLiteral, Literal: "staging"},
		Exposure: []string{"all"},
	}
	record, err := testentries.NewRecord(environment.Record.ID, entry, firstID)
	if err != nil {
		t.Fatalf("NewEntryRecord() error = %v", err)
	}
	first := testPlainGeneration(environment.Record.ID, entryID, firstID, "staging", now)
	created, err := repository.CreateEntry(
		context.Background(), environment, project, record, testentries.EntryValueGeneration{Plain: &first},
	)
	if err != nil {
		t.Fatalf("CreateEntry() error = %v", err)
	}
	secondID := ids.NewAt(ids.KindConfig, now.Add(time.Second), 42)
	desired := entry
	desired.Source.Literal = "production"
	second := testPlainGeneration(
		environment.Record.ID, entryID, secondID, "production", now.Add(time.Second),
	)
	marker := testDirectMarker()
	marker.Locator = testidempotency.IdempotencyLocator{
		ScopeKind: testidempotency.IdempotencyScopeEnvironment, ScopeID: environment.Record.ID,
		Method: http.MethodPatch, Route: "/entries/{id}", Key: "entry-edit-key-0001",
	}
	marker.Response = testidempotency.IdempotencyResponse{
		Status: http.StatusOK, ContentKind: "application/json", Body: []byte(`{"id":"` + entryID + `"}`),
	}
	marker.ReplayTarget = &testidempotency.IdempotencyReplayTarget{
		Kind: testidempotency.IdempotencyReplayTargetEntry,
		ID:   entryID,
	}
	result, err := repository.ReplaceEntryIdempotent(
		context.Background(),
		environment,
		project,
		created,
		desired,
		secondID,
		testentries.EntryValueGeneration{Plain: &second},
		marker,
	)
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("ReplaceEntryIdempotent() = %#v, %v", result, err)
	}
	replayed, err := repository.ReplaceEntryIdempotent(
		context.Background(),
		environment,
		project,
		created,
		desired,
		secondID,
		testentries.EntryValueGeneration{Plain: &second},
		marker,
	)
	if err != nil || replayed.kind != idempotencyTransactionExisting ||
		string(replayed.marker.Response.Body) != string(marker.Response.Body) {
		t.Fatalf("ReplaceEntryIdempotent(replay) = %#v, %v", replayed, err)
	}
	current, err := repository.GetEntry(context.Background(), entryID)
	if err != nil || current.Record.CurrentValueGenerationID != secondID ||
		current.Record.Entry.Source.Literal != "production" {
		t.Fatalf("GetEntry() = %#v, %v", current.Record, err)
	}
}

func TestEntryRepositoryDeleteRemovesEverySecretGenerationAtomically(t *testing.T) {
	// Rationale: deleting secret metadata while retaining an older immutable
	// ciphertext generation violates Entry ownership and the reveal/delete contract.
	store, environment, project := testEntryRepositoryHierarchy(t)
	repository, err := newEntryRepository(store)
	if err != nil {
		t.Fatalf("newEntryRepository() error = %v", err)
	}
	now := time.Date(2026, 8, 22, 13, 0, 0, 0, time.UTC)
	entryID := ids.NewAt(ids.KindEnvEntry, now, 20)
	firstID := ids.NewAt(ids.KindConfig, now, 21)
	entry := core.EnvEntry{
		ID: entryID, Kind: core.EntryKindEnv, Key: "API_TOKEN",
		Source: core.EntrySource{Kind: core.SourceLiteral}, Exposure: []string{"api"}, Secret: true,
	}
	record, err := testentries.NewRecord(environment.Record.ID, entry, firstID)
	if err != nil {
		t.Fatalf("NewEntryRecord() error = %v", err)
	}
	first := testSecretGeneration(environment.Record.ID, entryID, firstID, "cipher-one", now)
	created, err := repository.CreateEntry(
		context.Background(), environment, project, record, testentries.EntryValueGeneration{Secret: &first},
	)
	if err != nil {
		t.Fatalf("CreateEntry() error = %v", err)
	}
	secondID := ids.NewAt(ids.KindConfig, now.Add(time.Second), 22)
	second := testSecretGeneration(
		environment.Record.ID, entryID, secondID, "cipher-two", now.Add(time.Second),
	)
	current, err := repository.ReplaceEntry(
		context.Background(),
		environment,
		project,
		created,
		entry,
		secondID,
		testentries.EntryValueGeneration{Secret: &second},
	)
	if err != nil {
		t.Fatalf("ReplaceEntry() error = %v", err)
	}
	if _, err := repository.DeleteEntry(context.Background(), environment, project, current); err != nil {
		t.Fatalf("DeleteEntry() error = %v", err)
	}
	_, err = repository.GetEntry(context.Background(), entryID)
	if kind, ok := errs.KindOf(err); !ok || kind != errs.KindEntryNotFound {
		t.Fatalf("GetEntry() after delete error = %v", err)
	}
	valueRepository, err := testentryvalues.New(&releaseLogMemoryStore{store})
	if err != nil {
		t.Fatalf("newEntryValueGenerationRepository() error = %v", err)
	}
	for _, generationID := range []string{firstID, secondID} {
		value, found, getErr := valueRepository.GetSecret(context.Background(), entryID, generationID)
		clear(value.Ciphertext)
		if getErr != nil || found {
			t.Fatalf("GetSecret(%s) = found %v error %v", generationID, found, getErr)
		}
	}
}

func TestEntryRecordRejectsSecretPlaintextMetadata(t *testing.T) {
	// Rationale: encrypted subordinate storage is meaningless if the same
	// plaintext is also serialized into the listable Entry primary.
	now := time.Date(2026, 8, 22, 14, 0, 0, 0, time.UTC)
	entry := core.EnvEntry{
		ID: ids.NewAt(ids.KindEnvEntry, now, 30), Kind: core.EntryKindEnv, Key: "PASSWORD",
		Source:   core.EntrySource{Kind: core.SourceLiteral, Literal: "leaked"},
		Exposure: []string{"all"}, Secret: true,
	}
	_, err := testentries.NewRecord(
		ids.NewAt(ids.KindEnvironment, now, 31), entry, ids.NewAt(ids.KindConfig, now, 32),
	)
	if kind, ok := errs.KindOf(err); !ok || kind != errs.KindValidationFailed {
		t.Fatalf("NewEntryRecord() error = %v", err)
	}
}

func testEntryRepositoryHierarchy(
	t *testing.T,
) (*memoryHierarchyStore, testkeyvalue.Versioned[testhierarchy.EnvironmentRecord], testkeyvalue.Versioned[testhierarchy.ProjectRecord]) {
	t.Helper()
	_, store, environment, project, _ := routeRepositoryTestHierarchy(t)
	return store, environment, project
}

func testPlainGeneration(
	environmentID string,
	entryID string,
	generationID string,
	content string,
	createdAt time.Time,
) testentryvalues.PlainGeneration {
	digest := sha256.Sum256([]byte(content))
	return testentryvalues.PlainGeneration{
		EnvironmentID: environmentID, EntryID: entryID, GenerationID: generationID,
		Content: []byte(content), PlaintextSHA256: hex.EncodeToString(digest[:]), CreatedAt: createdAt,
	}
}

func testSecretGeneration(
	environmentID string,
	entryID string,
	generationID string,
	ciphertext string,
	createdAt time.Time,
) testentryvalues.SecretGeneration {
	digest := sha256.Sum256([]byte(ciphertext))
	return testentryvalues.SecretGeneration{
		EnvironmentID: environmentID, EntryID: entryID, GenerationID: generationID,
		EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
		CiphertextSHA256: hex.EncodeToString(digest[:]), Ciphertext: []byte(ciphertext), CreatedAt: createdAt,
	}
}
