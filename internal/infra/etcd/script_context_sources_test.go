package etcd

import (
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: a rehashed machine context must still match the actual selected
// Script primary and real consumer Release, not merely its own declared digest.
func TestScriptContextSourcesRejectRehashedSubstitution(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*ScriptExecutionSources, *agentpb.ResolvedRunnerSnapshot)
	}{
		{"missing context", func(_ *ScriptExecutionSources, snapshot *agentpb.ResolvedRunnerSnapshot) {
			snapshot.ExplicitExecution = nil
		}},
		{"inherited source", func(sources *ScriptExecutionSources, _ *agentpb.ResolvedRunnerSnapshot) {
			sources.Script.Record.Desired.Execution = nil
		}},
		{"changed primary", func(sources *ScriptExecutionSources, _ *agentpb.ResolvedRunnerSnapshot) {
			sources.Script.Revision++
		}},
		{"changed capture revision", func(_ *ScriptExecutionSources, snapshot *agentpb.ResolvedRunnerSnapshot) {
			snapshot.ExplicitExecution.ScriptModRevision++
		}},
		{"another release image", func(_ *ScriptExecutionSources, snapshot *agentpb.ResolvedRunnerSnapshot) {
			snapshot.ExplicitExecution.ReleaseLocalImageId = "sha256:" + strings.Repeat("c", 64)
		}},
		{"another setup image", func(_ *ScriptExecutionSources, snapshot *agentpb.ResolvedRunnerSnapshot) {
			snapshot.ExplicitExecution.Context.ImageReference = "example/setup@sha256:" + strings.Repeat("c", 64)
		}},
		{"another user", func(_ *ScriptExecutionSources, snapshot *agentpb.ResolvedRunnerSnapshot) {
			snapshot.ExplicitExecution.Context.User = "1000:1000"
		}},
		{"another target", func(_ *ScriptExecutionSources, snapshot *agentpb.ResolvedRunnerSnapshot) {
			snapshot.ExplicitExecution.Context.Volumes[0].Target = "/elsewhere"
		}},
		{"writable substitution", func(_ *ScriptExecutionSources, snapshot *agentpb.ResolvedRunnerSnapshot) {
			snapshot.ExplicitExecution.Context.Volumes[0].ReadOnly = false
		}},
		{"another entry", func(_ *ScriptExecutionSources, snapshot *agentpb.ResolvedRunnerSnapshot) {
			snapshot.ExplicitExecution.Context.EntryIds[0] = "ev_01ARZ3NDEKTSV4RRFFQ69G5FAT"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			sources, snapshot := scriptContextSourcesForTest(t)
			if err := validateScriptContextSources(sources, snapshot); err != nil {
				t.Fatalf("valid captured context rejected: %v", err)
			}
			test.edit(&sources, snapshot)
			if snapshot.ExplicitExecution != nil {
				digest, err := executionplan.ScriptExecutionContextDigest(snapshot.ExplicitExecution.Context)
				if err != nil {
					t.Fatal(err)
				}
				snapshot.ExplicitExecution.ContextSha256 = digest
			}
			if err := validateScriptContextSources(sources, snapshot); err == nil {
				t.Fatal("rehashed capture no longer matching the Script source was accepted")
			}
		})
	}
}

// Rationale: omitted/inherited contexts preserve existing capture behavior;
// actual explicit desired list order has no semantic effect on canonical capture.
func TestScriptContextSourcesAcceptInheritedAndCanonicalCapture(t *testing.T) {
	sources, snapshot := scriptContextSourcesForTest(t)
	if err := validateScriptContextSources(sources, snapshot); err != nil {
		t.Fatal(err)
	}
	if sources.Script.Record.Desired.Execution.Volumes[0].VolumeID != "vol_01ARZ3NDEKTSV4RRFFQ69G5FAW" ||
		sources.Script.Record.Desired.Execution.EntryIDs[0] != "ev_01ARZ3NDEKTSV4RRFFQ69G5FAW" {
		t.Fatal("canonical capture mutated the authored grant order")
	}
	for _, choice := range []*core.ScriptExecution{nil, {Mode: core.ScriptExecutionInherited}} {
		sources.Script.Record.Desired.Execution, snapshot.ExplicitExecution = choice, nil
		if err := validateScriptContextSources(sources, snapshot); err != nil {
			t.Fatalf("inherited choice rejected: %v", err)
		}
	}
}

func scriptContextSourcesForTest(t *testing.T) (ScriptExecutionSources, *agentpb.ResolvedRunnerSnapshot) {
	t.Helper()
	firstVolume, secondVolume := "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV", "vol_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	firstEntry, secondEntry := "ev_01ARZ3NDEKTSV4RRFFQ69G5FAV", "ev_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	image := "example/setup@sha256:" + strings.Repeat("b", 64)
	sources := ScriptExecutionSources{Revision: 100}
	sources.Script.Revision, sources.Script.ReadRevision = 42, 100
	sources.Script.Record.Desired.Execution = &core.ScriptExecution{
		Mode: core.ScriptExecutionExplicit, Image: image, User: "0:0",
		Volumes: []core.ScriptVolumeGrant{
			{VolumeID: secondVolume, Target: "/right"}, {VolumeID: firstVolume, Target: "/left", ReadOnly: true},
		},
		EntryIDs: []string{secondEntry, firstEntry},
	}
	sources.Release.Intent.CandidateWorkload.LocalImageID = "sha256:" + strings.Repeat("a", 64)
	context := &agentpb.ScriptExplicitExecutionContext{
		ImageReference: image, User: "0:0",
		Volumes: []*agentpb.ScriptExplicitVolumeGrant{
			{VolumeId: firstVolume, Target: "/left", ReadOnly: true}, {VolumeId: secondVolume, Target: "/right"},
		},
		EntryIds: []string{firstEntry, secondEntry},
	}
	digest, err := executionplan.ScriptExecutionContextDigest(context)
	if err != nil {
		t.Fatal(err)
	}
	return withScriptContextResourceSources(sources, &agentpb.ResolvedRunnerSnapshot{
		LocalImageId: "sha256:" + strings.Repeat("b", 64),
		ExplicitExecution: &agentpb.ScriptExplicitExecutionAuthority{
			Context: context, ContextSha256: digest, ScriptModRevision: 42,
			ReleaseLocalImageId: sources.Release.Intent.CandidateWorkload.LocalImageID,
		},
	})
}

// Source-publication tests need a real wire snapshot, not the checkpoint-only
// fixture's opaque byte sentinel. This does not pretend to build a runner plan.
func withScriptContextSnapshot(t *testing.T, record ScriptExecutionRecord) ScriptExecutionRecord {
	t.Helper()
	value, err := proto.Marshal(&agentpb.ResolvedRunnerSnapshot{
		SnapshotId: record.SnapshotID, ScriptExecutionId: record.ID,
		EnvironmentId: record.EnvironmentID, ServiceId: record.ServiceID, ReleaseId: record.ReleaseID,
	})
	if err != nil {
		t.Fatal(err)
	}
	record.Snapshot, record.SnapshotSHA256 = value, scriptSourceReferenceBytesDigest(value)
	return record
}
