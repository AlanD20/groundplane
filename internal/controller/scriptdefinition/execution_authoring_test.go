package scriptdefinition

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

// Rationale: mutable Volume slugs and current file destinations are not authored
// identity. Export must preserve exact grant decisions by immutable resource keys.
func TestAuthoringScriptExecutionResolvesStableIDsToKeys(t *testing.T) {
	at := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	volumeID, entryID := ids.NewAt(ids.KindVolume, at, 2), ids.NewAt(ids.KindEnvEntry, at, 3)
	record := etcd.ScriptRecord{EnvironmentID: environmentID, Origin: "blueprint", ReconciliationKey: "setup-hook",
		Desired: core.Script{Slug: "setup", ServiceName: "api", When: core.ScriptPreDeploy, Body: "echo setup",
			Execution: &core.ScriptExecution{Mode: core.ScriptExecutionExplicit, User: "0:0",
				Image: "example.invalid/setup@sha256:" + strings.Repeat("a", 64),
				Volumes: []core.ScriptVolumeGrant{
					{VolumeID: volumeID, Target: "/data", ReadOnly: false},
				}, EntryIDs: []string{entryID}},
		},
	}
	volumes := []etcd.EnvironmentVolumeIdentity{{ID: volumeID, Key: "storage", Slug: "renamed-storage"}}
	entries := []etcd.EntryRecord{{EnvironmentID: environmentID, BlueprintKey: "SETUP_INPUT",
		Entry: core.EnvEntry{ID: entryID, Kind: core.EntryKindEnv, Key: "SETUP_INPUT", Exposure: []string{"api"},
			Source: core.EntrySource{Kind: core.SourceLiteral, Literal: "not exported by Script"}},
	}}
	result, err := Authoring([]etcd.Versioned[etcd.ScriptRecord]{{Record: record}}, volumes, entries)
	if err != nil {
		t.Fatal(err)
	}
	readOnly := false
	want := &core.ScriptExecutionSpec{
		Mode:  core.ScriptExecutionExplicit,
		Image: record.Desired.Execution.Image,
		User:  "0:0",
		Volumes: []core.ScriptVolumeGrantSpec{
			{Volume: "storage", Target: "/data", ReadOnly: &readOnly},
		},
		Entries: []string{"SETUP_INPUT"},
	}
	if !reflect.DeepEqual(result["setup-hook"].Execution, want) {
		t.Fatalf("exported context: %#v", result["setup-hook"].Execution)
	}
	*result["setup-hook"].Execution.Volumes[0].ReadOnly = true
	result["setup-hook"].Execution.Entries[0] = "changed"
	if record.Desired.Execution.Volumes[0].ReadOnly || record.Desired.Execution.EntryIDs[0] != entryID {
		t.Fatal("export aliased stored context")
	}

	for _, name := range []string{"missing volume", "missing entry", "foreign entry", "no Blueprint key"} {
		t.Run(name, func(t *testing.T) {
			selectedVolumes, selectedEntries := volumes, append([]etcd.EntryRecord(nil), entries...)
			switch name {
			case "missing volume":
				selectedVolumes = nil
			case "missing entry":
				selectedEntries = nil
			case "foreign entry":
				selectedEntries[0].EnvironmentID = ids.NewAt(ids.KindEnvironment, at, 4)
			case "no Blueprint key":
				selectedEntries[0].BlueprintKey = ""
			}
			if _, err := Authoring([]etcd.Versioned[etcd.ScriptRecord]{{Record: record}}, selectedVolumes, selectedEntries); err == nil {
				t.Fatal("silently dropped an unrepresentable grant")
			}
		})
	}
}
