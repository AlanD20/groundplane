package app

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

func TestEnvironmentBlueprintEntryRemovalsDeleteOnlyStaleOutputs(t *testing.T) {
	now := time.Date(2026, 8, 29, 13, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	uid, gid := uint32(1000), uint32(1000)
	file, err := etcd.NewBlueprintEntryRecord(environmentID, "config", core.EnvEntry{
		ID: ids.NewAt(ids.KindEnvEntry, now, 2), Kind: core.EntryKindFile, Path: "config/app.env",
		UID: &uid, GID: &gid, Source: core.EntrySource{Kind: core.SourceLiteral}, Exposure: []string{"api"},
	}, ids.NewAt(ids.KindConfig, now, 3))
	if err != nil {
		t.Fatal(err)
	}
	environment, err := etcd.NewBlueprintEntryRecord(environmentID, "mode", core.EnvEntry{
		ID: ids.NewAt(ids.KindEnvEntry, now, 4), Kind: core.EntryKindEnv, Key: "MODE",
		Source: core.EntrySource{Kind: core.SourceLiteral, Literal: "old"}, Exposure: []string{"api"},
	}, ids.NewAt(ids.KindConfig, now, 5))
	if err != nil {
		t.Fatal(err)
	}
	removals, err := environmentBlueprintEntryRemovals(
		environmentID,
		[]etcd.EntryRecord{file, environment},
		nil,
		[]controller.ComposeResourceIdentity{{ID: ids.NewAt(ids.KindService, now, 6), Name: "api"}},
	)
	if err != nil || len(removals) != 2 || removals[0].Destination != "config/app.env" ||
		removals[1].OutputKind != etcd.TaskMaterializationOutputRemoveGeneratedEnv {
		t.Fatalf("environmentBlueprintEntryRemovals() = %#v, %v", removals, err)
	}
}

func TestEnvironmentEntryProjectionPreservesBlueprintKey(t *testing.T) {
	now := time.Date(2026, 8, 29, 14, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 11)
	record, err := etcd.NewBlueprintEntryRecord(environmentID, "APP_MODE", core.EnvEntry{
		ID: ids.NewAt(ids.KindEnvEntry, now, 12), Kind: core.EntryKindEnv, Key: "APP_MODE",
		Source: core.EntrySource{Kind: core.SourceLiteral, Literal: "acceptance"}, Exposure: []string{"all"},
	}, ids.NewAt(ids.KindConfig, now, 13))
	if err != nil {
		t.Fatal(err)
	}
	projected, err := environmentEntryProjection([]etcd.Versioned[etcd.EntryRecord]{{Record: record, Revision: 1, ReadRevision: 1}})
	if err != nil || len(projected) != 1 || projected[0].BlueprintKey != record.BlueprintKey {
		t.Fatalf("environmentEntryProjection() = %#v, %v", projected, err)
	}
}
