package controller

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

func TestReconcileBlueprintEntriesPreservesIdentityAndRemovesOnlyBlueprintOwnedRecords(t *testing.T) {
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	direct, err := etcd.NewEntryRecord(environmentID, core.EnvEntry{
		ID: ids.NewAt(ids.KindEnvEntry, now, 2), Kind: core.EntryKindEnv, Key: "DIRECT",
		Source: core.EntrySource{Kind: core.SourceLiteral, Literal: "kept"}, Exposure: []string{"all"},
	}, ids.NewAt(ids.KindConfig, now, 3))
	if err != nil {
		t.Fatal(err)
	}
	removed, err := etcd.NewBlueprintEntryRecord(environmentID, "old", core.EnvEntry{
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
	}, []etcd.EntryRecord{direct, removed}, allocate)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Current) != 2 || len(got.Values) != 1 || len(got.Removed) != 1 ||
		got.Removed[0].Entry.ID != removed.Entry.ID {
		t.Fatalf("reconciliation = %#v", got)
	}
	foundDirect := false
	for _, record := range got.Current {
		foundDirect = foundDirect || record.Entry.ID == direct.Entry.ID
	}
	if !foundDirect {
		t.Fatal("direct Entry was not preserved")
	}
	got, err = ReconcileBlueprintEntries(environmentID, map[string]core.EntrySpec{
		"APP_MODE": {
			Kind:     core.EntryKindEnv,
			Source:   core.EntrySourceSpec{Literal: "production"},
			Exposure: []string{"all"},
		},
	}, got.Current, allocate)
	if err != nil || len(got.Values) != 0 || len(got.Removed) != 0 {
		t.Fatalf("unchanged reconciliation = %#v, %v", got, err)
	}
}
