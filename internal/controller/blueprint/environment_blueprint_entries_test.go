package blueprint

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testtaskmaterialization "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	testcomposeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	entryoperations "github.com/AlanD20/groundplane/internal/controller/entry/operations"
	"github.com/AlanD20/groundplane/internal/core"
	testentries "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
)

func TestEnvironmentBlueprintEntryRemovalsDeleteOnlyStaleOutputs(t *testing.T) {
	now := time.Date(2026, 8, 29, 13, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	uid, gid := uint32(1000), uint32(1000)
	file, err := testentries.NewBlueprintRecord(environmentID, "config", core.EnvEntry{
		ID: ids.NewAt(ids.KindEnvEntry, now, 2), Kind: core.EntryKindFile, Path: "config/app.env",
		UID: &uid, GID: &gid, Source: core.EntrySource{Kind: core.SourceLiteral}, Exposure: []string{"api"},
	}, ids.NewAt(ids.KindConfig, now, 3))
	if err != nil {
		t.Fatal(err)
	}
	environment, err := testentries.NewBlueprintRecord(environmentID, "mode", core.EnvEntry{
		ID: ids.NewAt(ids.KindEnvEntry, now, 4), Kind: core.EntryKindEnv, Key: "MODE",
		Source: core.EntrySource{Kind: core.SourceLiteral, Literal: "old"}, Exposure: []string{"api"},
	}, ids.NewAt(ids.KindConfig, now, 5))
	if err != nil {
		t.Fatal(err)
	}
	removals, err := entryoperations.PlanEntryRemovals(
		environmentID,
		[]testentries.Record{file, environment},
		nil,
		[]testcomposeidentity.Resource{{ID: ids.NewAt(ids.KindService, now, 6), Name: "api"}},
	)
	if err != nil || len(removals) != 2 || removals[0].Destination != "config/app.env" ||
		removals[1].OutputKind != testtaskmaterialization.OutputRemoveGeneratedEnv {
		t.Fatalf("PlanEntryRemovals() = %#v, %v", removals, err)
	}
}
