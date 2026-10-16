package etcd

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// A failed or fact-waiting Apply must not hide Entry metadata. The immutable
// revision also has to remain pageable after a newer head changes declarations.
func TestEntryDeclarationsSurvivePendingRuntimeAndHeadChange(t *testing.T) {
	_, store, environment, _ := serviceRepositoryTestHierarchy(t)
	projection := serviceRecordTestProjection(t, environment.Record.ID, serviceRepositoryTestDesired(901, "api"))
	entry, err := entries.NewBlueprintRecord(environment.Record.ID, "MODE", core.EnvEntry{
		ID: ids.New(ids.KindEnvEntry), Kind: core.EntryKindEnv, Key: "MODE",
		Source: core.EntrySource{Kind: core.SourceLiteral, Literal: "original"}, Exposure: []string{"all"},
	}, ids.New(ids.KindConfig))
	if err != nil {
		t.Fatal(err)
	}
	projection.Entries = []entries.Record{entry}
	seedServiceRepositoryTestDesiredProjection(t, store, projection)
	if _, err := store.Transact(t.Context(), nil, []keyvalue.Mutation{{Type: keyvalue.MutationDelete,
		Key: blueprints.EnvironmentBlueprintEffectiveProjectionKey(projection.EnvironmentID, projection.RevisionID)}}); err != nil {
		t.Fatal(err)
	}
	reader := environmentqueries.NewProjectionReader(store)
	lookup, found, err := reader.FindDeclaredEntry(t.Context(), entry.Entry.ID)
	if err != nil || !found || lookup.Record.Entry.Source.Literal != "original" {
		t.Fatalf("pending Entry lookup without a settled index: %v, found=%t", err, found)
	}
	first, found, err := reader.GetEnvironmentEntryDeclarations(t.Context(), environment.Record.ID)
	if err != nil || !found || len(first.Record.Entries) != 1 ||
		first.Record.Entries[0].Entry.Source.Literal != "original" {
		t.Fatalf("pending Entry read: %v, found=%t", err, found)
	}
	projection.RevisionID = ids.New(ids.KindTask)
	projection.RenderGeneration++
	projection.Entries[0].Entry.Source.Literal = "changed"
	seedServiceRepositoryTestDesiredProjection(t, store, projection)
	pinned, found, err := reader.GetEnvironmentEntryDeclarationsRevision(
		t.Context(),
		environment.Record.ID,
		first.Record.RevisionID,
	)
	if err != nil || !found || len(pinned.Record.Entries) != 1 ||
		pinned.Record.Entries[0].Entry.Source.Literal != "original" {
		t.Fatalf("pinned Entry read: %v, found=%t", err, found)
	}
	current, found, err := reader.GetEnvironmentEntryDeclarations(t.Context(), environment.Record.ID)
	if err != nil || !found || current.Record.Entries[0].Entry.Source.Literal != "changed" {
		t.Fatalf("current Entry read: %v, found=%t", err, found)
	}
}
