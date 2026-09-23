package entry

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testentries "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// Rationale: candidate Script references and Blueprint export use immutable
// Entry keys, so normalization must never discard them.
func TestEnvironmentEntryProjectionPreservesBlueprintKey(t *testing.T) {
	now := time.Date(2026, 8, 29, 14, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 11)
	record, err := testentries.NewBlueprintRecord(environmentID, "APP_MODE", core.EnvEntry{
		ID: ids.NewAt(ids.KindEnvEntry, now, 12), Kind: core.EntryKindEnv, Key: "APP_MODE",
		Source: core.EntrySource{Kind: core.SourceLiteral, Literal: "acceptance"}, Exposure: []string{"all"},
	}, ids.NewAt(ids.KindConfig, now, 13))
	if err != nil {
		t.Fatal(err)
	}
	projected, err := BlueprintProjection(
		[]testkeyvalue.Versioned[testentries.Record]{{Record: record, Revision: 1, ReadRevision: 1}},
	)
	if err != nil || len(projected) != 1 || projected[0].BlueprintKey != record.BlueprintKey {
		t.Fatalf("BlueprintProjection() = %#v, %v", projected, err)
	}
}

// Rationale: a direct Entry must appear in the canonical Environment Blueprint,
// while a secret literal must never disclose its selected value generation.
func TestBlueprintAuthoringIncludesDirectEntriesWithoutSecretValues(t *testing.T) {
	at := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	uid, gid := uint32(1000), uint32(1000)
	plain, err := testentries.NewRecord(environmentID, core.EnvEntry{
		ID: ids.NewAt(ids.KindEnvEntry, at, 2), Kind: core.EntryKindFile, Path: "app/config.yaml",
		UID: &uid, GID: &gid, Source: core.EntrySource{Kind: core.SourceLiteral, Literal: "enabled: true"},
		Exposure: []string{"api"},
	}, ids.NewAt(ids.KindConfig, at, 3))
	if err != nil {
		t.Fatal(err)
	}
	secret, err := testentries.NewRecord(environmentID, core.EnvEntry{
		ID: ids.NewAt(ids.KindEnvEntry, at, 4), Kind: core.EntryKindEnv, Key: "TOKEN",
		Source: core.EntrySource{Kind: core.SourceLiteral}, Exposure: []string{"api"}, Secret: true,
	}, ids.NewAt(ids.KindConfig, at, 5))
	if err != nil {
		t.Fatal(err)
	}
	authored, err := BlueprintAuthoring([]testentries.Record{plain, secret})
	if err != nil || len(authored) != 2 || authored["file:app/config.yaml"].Source.Literal != "enabled: true" ||
		authored["TOKEN"].Source.Literal != "" || !authored["TOKEN"].Secret {
		t.Fatalf("Blueprint authoring = %#v, %v", authored, err)
	}
}
