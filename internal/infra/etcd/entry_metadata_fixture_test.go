package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testentries "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	testentryvalues "github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func entryMetadataFixture(
	t *testing.T,
) (*EntryRepository, *memoryHierarchyStore, testkeyvalue.Versioned[testhierarchy.EnvironmentRecord], testkeyvalue.Versioned[testhierarchy.ProjectRecord], testkeyvalue.Versioned[testentries.Record], []string) {
	t.Helper()
	_, store, environment, project, _ := routeRepositoryTestHierarchy(t)
	repository, err := newEntryRepository(store)
	if err != nil {
		t.Fatalf("newEntryRepository() error = %v", err)
	}
	now := serviceRecordTestTime().Add(4 * time.Hour)
	entryID := ids.NewAt(ids.KindEnvEntry, now, 9100)
	firstID := ids.NewAt(ids.KindConfig, now, 9101)
	entry := core.EnvEntry{
		ID: entryID, Kind: core.EntryKindEnv, Key: "APP_ENV",
		Source: core.EntrySource{Kind: core.SourceLiteral, Literal: "staging"}, Exposure: []string{"all"},
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
	secondID := ids.NewAt(ids.KindConfig, now.Add(time.Second), 9102)
	entry.Source.Literal = "production"
	second := testPlainGeneration(environment.Record.ID, entryID, secondID, "production", now.Add(time.Second))
	current, err := repository.ReplaceEntry(
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
	return repository, store, environment, project, current, []string{firstID, secondID}
}

func seedAppliedEntryProjection(
	t *testing.T,
	store *memoryHierarchyStore,
	entry testkeyvalue.Versioned[testentries.Record],
) testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection] {
	t.Helper()
	projection := withTestEnvironmentComposeArtifact(testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID:    entry.Record.EnvironmentID,
		RevisionID:       ids.NewAt(ids.KindTask, serviceRecordTestTime().Add(5*time.Hour), 9150),
		RenderGeneration: 4,
		Entries:          []testentries.Record{testentries.CloneRecord(entry.Record)},
	})
	value, err := testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(projection)
	if err != nil {
		t.Fatalf("encodeEnvironmentComposeProjection() error = %v", err)
	}
	result, err := store.Transact(context.Background(), []testkeyvalue.Condition{{
		Key: testenvironmentprojection.EnvironmentComposeProjectionStorageKey(entry.Record.EnvironmentID),
	}}, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: testenvironmentprojection.EnvironmentComposeProjectionStorageKey(entry.Record.EnvironmentID), Value: value,
	}})
	clear(value)
	if err != nil || !result.Succeeded {
		t.Fatalf("seed Environment projection = %#v/%v", result, err)
	}
	return testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
		Record: projection, Revision: result.Revision, ReadRevision: result.Revision,
	}
}

func assertEntryMetadataRetained(
	t *testing.T,
	repository *EntryRepository,
	store *memoryHierarchyStore,
	entry testkeyvalue.Versioned[testentries.Record],
	generationIDs []string,
) {
	t.Helper()
	visible, err := repository.GetEntry(context.Background(), entry.Record.Entry.ID)
	if err != nil || !testentries.EqualRecord(visible.Record, entry.Record) {
		t.Fatalf("GetEntry(retained) = %#v/%v", visible, err)
	}
	assertEntryGenerations(t, store, entry.Record.Entry.ID, generationIDs, true)
}

func assertEntryGenerations(
	t *testing.T,
	store *memoryHierarchyStore,
	entryID string,
	generationIDs []string,
	want bool,
) {
	t.Helper()
	for _, generationID := range generationIDs {
		stored, err := store.Get(context.Background(), testentryvalues.PlainKey(entryID, generationID))
		if err != nil || (stored.Entry != nil) != want {
			t.Fatalf("Get(Entry generation %s) = %#v/%v, want present %t", generationID, stored, err, want)
		}
	}
}
