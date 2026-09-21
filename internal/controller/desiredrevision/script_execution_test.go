package desiredrevision

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testcomposeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	"github.com/AlanD20/groundplane/internal/core"
	testentries "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
)

// Rationale: names resolve against this candidate, never a stale live primary;
// changing context preserves the body generation and replaces all grants.
func TestReconcileBlueprintScriptExecutionUsesCandidateIdentities(t *testing.T) {
	at := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	service := desiredRevisionScriptService(t, environmentID, ids.NewAt(ids.KindService, at, 2), "api")
	volumeID := ids.NewAt(ids.KindVolume, at, 3)
	entryID := ids.NewAt(ids.KindEnvEntry, at, 4)
	readOnly := false
	authored := map[string]core.ScriptSpec{"setup": {
		Slug: "setup", Service: "api", When: core.ScriptPreDeploy, Script: "echo setup", Order: 10,
		Execution: &core.ScriptExecutionSpec{
			Mode: core.ScriptExecutionExplicit, Image: "example.invalid/setup@sha256:" + strings.Repeat("a", 64), User: "0:0",
			Volumes: []core.ScriptVolumeGrantSpec{{Volume: "storage", Target: "/data", ReadOnly: &readOnly}},
			Entries: []string{"SETUP_INPUT"},
		},
	}}
	resources := BlueprintScriptResources{
		Volumes: []testcomposeidentity.Resource{{ID: volumeID, Name: "storage"}},
		Entries: []testentries.Record{{EnvironmentID: environmentID, BlueprintKey: "SETUP_INPUT",
			Entry: core.EnvEntry{ID: entryID, Kind: core.EntryKindEnv, Key: "SETUP_INPUT", Exposure: []string{"api"},
				Source: core.EntrySource{Kind: core.SourceLiteral, Literal: "input"}},
		}},
	}
	allocate := func(kind ids.Kind, purpose string) string {
		return ids.DeriveAt(kind, at, environmentID, purpose)
	}
	first, err := ReconcileBlueprintScripts(
		environmentID,
		authored,
		[]testservices.ServiceRecord{service},
		nil,
		resources,
		allocate,
	)
	if err != nil {
		t.Fatal(err)
	}
	want := &core.ScriptExecution{
		Mode: core.ScriptExecutionExplicit, Image: authored["setup"].Execution.Image, User: "0:0",
		Volumes: []core.ScriptVolumeGrant{
			{VolumeID: volumeID, Target: "/data", ReadOnly: false},
		}, EntryIDs: []string{entryID},
	}
	if len(first.Current) != 1 || !reflect.DeepEqual(first.Current[0].Desired.Execution, want) {
		t.Fatalf("resolved context: %#v", first)
	}
	// The candidate now owns a replacement Entry identity under the same key.
	newEntryID := ids.NewAt(ids.KindEnvEntry, at, 5)
	resources.Entries[0].Entry.ID = newEntryID
	next, err := ReconcileBlueprintScripts(
		environmentID,
		authored,
		[]testservices.ServiceRecord{service},
		first.Current,
		resources,
		allocate,
	)
	if err != nil || len(next.BodyGenerations) != 0 || next.Current[0].ActiveGeneration != 1 ||
		next.Current[0].Desired.Execution.EntryIDs[0] != newEntryID {
		t.Fatalf("context-only candidate change: %#v, %v", next, err)
	}
	// An authored inherited context replaces the whole explicit authority.
	reset := authored["setup"]
	reset.Execution = &core.ScriptExecutionSpec{Mode: core.ScriptExecutionInherited}
	authored["setup"] = reset
	last, err := ReconcileBlueprintScripts(
		environmentID,
		authored,
		[]testservices.ServiceRecord{service},
		next.Current,
		resources,
		allocate,
	)
	if err != nil || last.Current[0].Desired.Execution != nil || len(last.BodyGenerations) != 0 {
		t.Fatalf("inherited reset: %#v, %v", last, err)
	}
}

// Rationale: Blueprint reconciliation is a pure boundary too; direct callers
// cannot rely on a prior YAML parser to prove ownership or exact grants.
func TestReconcileBlueprintScriptExecutionRejectsInvalidCandidateGrants(t *testing.T) {
	at := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 10)
	service := desiredRevisionScriptService(t, environmentID, ids.NewAt(ids.KindService, at, 11), "api")
	for _, name := range []string{"missing volume", "duplicate volume", "foreign entry", "unexposed entry", "missing entry", "file overlap"} {
		t.Run(name, func(t *testing.T) {
			readOnly, uid := false, uint32(0)
			resources := BlueprintScriptResources{
				Volumes: []testcomposeidentity.Resource{{ID: ids.NewAt(ids.KindVolume, at, 12), Name: "storage"}},
				Entries: []testentries.Record{{EnvironmentID: environmentID, BlueprintKey: "SETUP_INPUT",
					Entry: core.EnvEntry{ID: ids.NewAt(ids.KindEnvEntry, at, 13), Kind: core.EntryKindEnv,
						Key: "SETUP_INPUT", Exposure: []string{"api"}, Source: core.EntrySource{Kind: core.SourceLiteral, Literal: "input"}},
				}},
			}
			switch name {
			case "missing volume":
				resources.Volumes = nil
			case "duplicate volume":
				resources.Volumes = append(resources.Volumes, resources.Volumes[0])
			case "foreign entry":
				resources.Entries[0].EnvironmentID = ids.NewAt(ids.KindEnvironment, at, 14)
			case "unexposed entry":
				resources.Entries[0].Entry.Exposure = []string{"worker"}
			case "missing entry":
				resources.Entries = nil
			case "file overlap":
				resources.Entries[0].Entry.Kind, resources.Entries[0].Entry.Key = core.EntryKindFile, ""
				resources.Entries[0].Entry.Path, resources.Entries[0].Entry.UID, resources.Entries[0].Entry.GID = "data/input", &uid, &uid
			}
			authored := map[string]core.ScriptSpec{
				"setup": {Slug: "setup", Service: "api", When: core.ScriptPreDeploy, Script: "echo setup",
					Execution: &core.ScriptExecutionSpec{Mode: core.ScriptExecutionExplicit,
						Image: "example.invalid/setup@sha256:" + strings.Repeat("a", 64), User: "0:0",
						Volumes: []core.ScriptVolumeGrantSpec{
							{Volume: "storage", Target: "/data", ReadOnly: &readOnly},
						}, Entries: []string{"SETUP_INPUT"}},
				},
			}
			_, err := ReconcileBlueprintScripts(
				environmentID,
				authored,
				[]testservices.ServiceRecord{service},
				nil,
				resources,
				func(kind ids.Kind, purpose string) string {
					return ids.DeriveAt(kind, at, environmentID, purpose)
				},
			)
			if err == nil {
				t.Fatal("accepted invalid candidate resources")
			}
		})
	}
}
