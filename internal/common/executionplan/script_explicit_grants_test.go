package executionplan

import (
	"crypto/sha256"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	explicitVolumeID = "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	explicitEntryID  = "ev_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

// Rationale: grants must be exact on the machine boundary even if a hostile
// projection is internally consistent and all of its hashes are recomputed.
func TestExplicitScriptRejectsRehashedResourceExpansion(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*agentpb.ResolvedRunnerSnapshot)
	}{
		{"ungranted volume", func(snapshot *agentpb.ResolvedRunnerSnapshot) {
			snapshot.ExplicitExecution.Context.Volumes = nil
		}},
		{"missing volume", func(snapshot *agentpb.ResolvedRunnerSnapshot) { snapshot.Mounts = nil }},
		{"wrong volume identity", func(snapshot *agentpb.ResolvedRunnerSnapshot) {
			snapshot.ExplicitExecution.Context.Volumes[0].VolumeId = "vol_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		}},
		{"wrong mount source", func(snapshot *agentpb.ResolvedRunnerSnapshot) {
			snapshot.Mounts[0].RenderedMount.Source = "unmanaged-volume"
		}},
		{"abbreviated managed source", func(snapshot *agentpb.ResolvedRunnerSnapshot) {
			snapshot.Mounts[0].RenderedMount.Source = "gp_vol_01arz3ndektsv4rrffq69g5fav"
		}},
		{"changed writable grant", func(snapshot *agentpb.ResolvedRunnerSnapshot) {
			snapshot.ExplicitExecution.Context.Volumes[0].ReadOnly = true
		}},
		{"host bind", func(snapshot *agentpb.ResolvedRunnerSnapshot) {
			snapshot.Mounts[0].RenderedMount.Type = "bind"
			snapshot.Mounts[0].RenderedMount.Source = "/var/lib/application"
		}},
		{"implicit image copy", func(snapshot *agentpb.ResolvedRunnerSnapshot) {
			snapshot.Mounts[0].RenderedMount.VolumeNoCopy = false
		}},
		{"mount options", func(snapshot *agentpb.ResolvedRunnerSnapshot) {
			snapshot.Mounts[0].RenderedMount.VolumeSubpath = "ambient"
		}},
		{"ungranted entry", func(snapshot *agentpb.ResolvedRunnerSnapshot) {
			snapshot.ExplicitExecution.Context.EntryIds = nil
		}},
		{"missing entry", func(snapshot *agentpb.ResolvedRunnerSnapshot) { snapshot.EntryBindings = nil }},
		{"wrong entry identity", func(snapshot *agentpb.ResolvedRunnerSnapshot) {
			snapshot.ExplicitExecution.Context.EntryIds[0] = "ev_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		}},
		{"entry shadows interpreter", func(snapshot *agentpb.ResolvedRunnerSnapshot) {
			snapshot.EntryBindings[0].FileTarget = "/bin/sh"
		}},
		{"entry below volume", func(snapshot *agentpb.ResolvedRunnerSnapshot) {
			snapshot.EntryBindings[0].FileTarget = "/storage/key"
		}},
		{"entry above volume", func(snapshot *agentpb.ResolvedRunnerSnapshot) {
			snapshot.ExplicitExecution.Context.Volumes[0].Target = "/application/tls/data"
			snapshot.Mounts[0].RenderedMount.Target = "/application/tls/data"
		}},
		{"overlapping entry files", func(snapshot *agentpb.ResolvedRunnerSnapshot) {
			first := snapshot.EntryBindings[0]
			second := &agentpb.ScriptRunnerEntryBinding{
				EntryId: "ev_01ARZ3NDEKTSV4RRFFQ69G5FAW", ValueGenerationId: first.ValueGenerationId,
				Sha256: first.Sha256, Kind: first.Kind, FileTarget: first.FileTarget + "/child", Mode: first.Mode,
			}
			snapshot.EntryBindings = append(snapshot.EntryBindings, second)
			snapshot.ExplicitExecution.Context.EntryIds = append(snapshot.ExplicitExecution.Context.EntryIds, second.EntryId)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan := explicitScriptGrantedPlanForTest(t)
			if _, err := Seal(plan); err != nil {
				t.Fatalf("valid exact grants rejected: %v", err)
			}
			test.edit(plan.ScriptRunnerSnapshots[0])
			refreshExplicitScriptGrantsForTest(t, plan)
			if _, err := Seal(plan); err == nil {
				t.Fatal("rehashing authorized an undeclared or unsafe resource projection")
			}
		})
	}
}

// Rationale: both grant modes remain meaningful; a secret environment Entry
// is selected without becoming an ambient plaintext environment pair.
func TestExplicitScriptAcceptsReadOnlyVolumeAndEnvironmentEntry(t *testing.T) {
	plan := explicitScriptGrantedPlanForTest(t)
	snapshot := plan.ScriptRunnerSnapshots[0]
	snapshot.ExplicitExecution.Context.Volumes[0].ReadOnly = true
	snapshot.Mounts[0].RenderedMount.ReadOnly = true
	entry := snapshot.EntryBindings[0]
	entry.Kind, entry.Secret, entry.EnvironmentKey = agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_ENV, true, "TOKEN"
	entry.FileTarget, entry.Mode = "", 0
	refreshExplicitScriptGrantsForTest(t, plan)
	if _, err := Seal(plan); err != nil {
		t.Fatalf("valid exact grants rejected: %v", err)
	}
}

func explicitScriptGrantedPlanForTest(t *testing.T) *agentpb.ExecutionPlan {
	t.Helper()
	plan := explicitScriptPlanForTest(t, validManualScriptPlan(t))
	snapshot := plan.ScriptRunnerSnapshots[0]
	snapshot.ExplicitExecution.Context.Volumes = []*agentpb.ScriptExplicitVolumeGrant{{
		VolumeId: explicitVolumeID, Target: "/storage", ReadOnly: false,
	}}
	snapshot.Mounts = []*agentpb.ScriptRunnerMount{{
		SourceId: explicitVolumeID, Source: existingScriptSourceAuthorityForTest(15),
		RenderedMount: &agentpb.ScriptMount{
			Type: "volume", Source: "gp_vol_vol_01arz3ndektsv4rrffq69g5fav", Target: "/storage", VolumeNoCopy: true,
		},
	}}
	digest := sha256.Sum256([]byte("test public file"))
	snapshot.ExplicitExecution.Context.EntryIds = []string{explicitEntryID}
	snapshot.EntryBindings = []*agentpb.ScriptRunnerEntryBinding{{
		EntryId: explicitEntryID, ValueGenerationId: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV", Sha256: digest[:],
		Kind: agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_FILE, FileTarget: "/application/tls", Mode: 0o444,
	}}
	refreshExplicitScriptGrantsForTest(t, plan)
	return plan
}

func refreshExplicitScriptGrantsForTest(t *testing.T, plan *agentpb.ExecutionPlan) {
	t.Helper()
	snapshot, projection := plan.ScriptRunnerSnapshots[0], plan.ScriptRunnerProjections[0]
	projection.Mounts, projection.EntryBindings = snapshot.Mounts, snapshot.EntryBindings
	digest, err := scriptMessageDigest(snapshot.ExplicitExecution.Context)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.ExplicitExecution.ContextSha256 = digest
	refreshScriptProjectionImageDigestForTest(t, plan)
}
