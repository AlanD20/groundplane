package taskplanning

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testentries "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
)

// Rationale: Blueprint export includes direct Entries, so Apply must adopt
// included direct Entries without losing their value generation and remove
// omitted Entries regardless of which operator surface created them.
func TestReconcileBlueprintEntriesAdoptsDirectAndRemovesOmittedRecords(t *testing.T) {
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	direct, err := testentries.NewRecord(environmentID, core.EnvEntry{
		ID: ids.NewAt(ids.KindEnvEntry, now, 2), Kind: core.EntryKindEnv, Key: "DIRECT",
		Source: core.EntrySource{Kind: core.SourceLiteral, Literal: "kept"}, Exposure: []string{"all"},
	}, ids.NewAt(ids.KindConfig, now, 3))
	if err != nil {
		t.Fatal(err)
	}
	removed, err := testentries.NewBlueprintRecord(environmentID, "old", core.EnvEntry{
		ID: ids.NewAt(ids.KindEnvEntry, now, 4), Kind: core.EntryKindEnv, Key: "old",
		Source: core.EntrySource{Kind: core.SourceLiteral, Literal: "old"}, Exposure: []string{"all"},
	}, ids.NewAt(ids.KindConfig, now, 5))
	if err != nil {
		t.Fatal(err)
	}
	sequence := int64(10)
	allocate := func(kind ids.Kind, _ string) string {
		sequence++
		return ids.NewAt(kind, now, sequence)
	}
	got, err := ReconcileBlueprintEntries(environmentID, map[string]core.EntrySpec{
		"APP_MODE": {
			Kind:     core.EntryKindEnv,
			Source:   core.EntrySourceSpec{Literal: "production"},
			Exposure: []string{"all"},
		},
	}, []testentries.Record{direct, removed}, allocate)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Current) != 1 || len(got.Values) != 1 || len(got.Removed) != 2 {
		t.Fatalf("reconciliation = %#v", got)
	}
	for _, record := range got.Removed {
		if record.Entry.ID != direct.Entry.ID && record.Entry.ID != removed.Entry.ID {
			t.Fatalf("unexpected removed Entry = %#v", record)
		}
	}
	got, err = ReconcileBlueprintEntries(environmentID, map[string]core.EntrySpec{
		"DIRECT": {
			Kind: core.EntryKindEnv, Source: core.EntrySourceSpec{Literal: "kept"}, Exposure: []string{"all"},
		},
	}, []testentries.Record{direct}, allocate)
	if err != nil || len(got.Current) != 1 || len(got.Values) != 0 || len(got.Removed) != 0 ||
		got.Current[0].Entry.ID != direct.Entry.ID || got.Current[0].CurrentValueGenerationID != direct.CurrentValueGenerationID ||
		got.Current[0].BlueprintKey != "DIRECT" {
		t.Fatalf("direct Entry adoption = %#v, %v", got, err)
	}
	got, err = ReconcileBlueprintEntries(environmentID, map[string]core.EntrySpec{
		"DIRECT": {
			Kind:     core.EntryKindEnv,
			Source:   core.EntrySourceSpec{Literal: "kept"},
			Exposure: []string{"all"},
		},
	}, got.Current, allocate)
	if err != nil || len(got.Values) != 0 || len(got.Removed) != 0 {
		t.Fatalf("unchanged reconciliation = %#v, %v", got, err)
	}
}
