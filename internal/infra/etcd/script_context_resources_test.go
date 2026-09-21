package etcd

import (
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	testentries "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testscriptsourcequeries "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcequeries"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: an authentic context names resources, but cannot authorize changed
// Entry destinations/owners or a resource absent from the fixed projection.
func TestScriptContextSourcesRejectResourceSubstitutions(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*testscriptsourcequeries.ScriptExecutionSources, *agentpb.ResolvedRunnerSnapshot)
	}{
		{"foreign projection", func(s *testscriptsourcequeries.ScriptExecutionSources, _ *agentpb.ResolvedRunnerSnapshot) {
			s.DesiredProjection.Record.EnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		}},
		{"another projection read", func(s *testscriptsourcequeries.ScriptExecutionSources, _ *agentpb.ResolvedRunnerSnapshot) {
			s.DesiredProjection.ReadRevision++
		}},
		{"missing Volume", func(s *testscriptsourcequeries.ScriptExecutionSources, _ *agentpb.ResolvedRunnerSnapshot) {
			s.DesiredProjection.Record.Volumes = nil
		}},
		{"duplicate Volume", func(s *testscriptsourcequeries.ScriptExecutionSources, _ *agentpb.ResolvedRunnerSnapshot) {
			s.DesiredProjection.Record.Volumes = append(s.DesiredProjection.Record.Volumes, s.DesiredProjection.Record.Volumes[0])
		}},
		{"missing Entry", func(s *testscriptsourcequeries.ScriptExecutionSources, _ *agentpb.ResolvedRunnerSnapshot) {
			s.DesiredProjection.Record.Entries = nil
		}},
		{"foreign Entry", func(s *testscriptsourcequeries.ScriptExecutionSources, _ *agentpb.ResolvedRunnerSnapshot) {
			s.DesiredProjection.Record.Entries[0].EnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		}},
		{"unexposed Entry", func(s *testscriptsourcequeries.ScriptExecutionSources, _ *agentpb.ResolvedRunnerSnapshot) {
			s.DesiredProjection.Record.Entries[0].Entry.Exposure = []string{"other"}
		}},
		{"another Entry generation", func(_ *testscriptsourcequeries.ScriptExecutionSources, p *agentpb.ResolvedRunnerSnapshot) {
			p.EntryBindings[0].ValueGenerationId = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAX"
		}},
		{"another Entry destination", func(_ *testscriptsourcequeries.ScriptExecutionSources, p *agentpb.ResolvedRunnerSnapshot) {
			p.EntryBindings[0].FileTarget = "/etc/tls/another.conf"
		}},
		{"another Entry owner", func(_ *testscriptsourcequeries.ScriptExecutionSources, p *agentpb.ResolvedRunnerSnapshot) {
			p.EntryBindings[0].Uid++
		}},
		{"another Entry storage class", func(_ *testscriptsourcequeries.ScriptExecutionSources, p *agentpb.ResolvedRunnerSnapshot) {
			p.EntryBindings[0].Secret = true
			p.EntryBindings[0].Mode = 0o600
		}},
		{"another Entry kind", func(_ *testscriptsourcequeries.ScriptExecutionSources, p *agentpb.ResolvedRunnerSnapshot) {
			p.EntryBindings[0].Kind = agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_ENV
			p.EntryBindings[0].FileTarget = ""
			p.EntryBindings[0].Uid, p.EntryBindings[0].Gid, p.EntryBindings[0].Mode = 0, 0, 0
			p.EntryBindings[0].EnvironmentKey = "REPLACEMENT"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			sources, snapshot := scriptContextSourcesForTest(t)
			if err := testscriptsourcequeries.ValidateScriptContextSources(sources, snapshot); err != nil {
				t.Fatalf("valid exact resources rejected: %v", err)
			}
			test.edit(&sources, snapshot)
			if err := testscriptsourcequeries.ValidateScriptContextSources(sources, snapshot); err == nil {
				t.Fatal("substituted explicit resource source was accepted")
			}
		})
	}
}

func withScriptContextResourceSources(
	sources testscriptsourcequeries.ScriptExecutionSources, snapshot *agentpb.ResolvedRunnerSnapshot,
) (testscriptsourcequeries.ScriptExecutionSources, *agentpb.ResolvedRunnerSnapshot) {
	const environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	const serviceID = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	sources.Environment.Record.ID = environmentID
	sources.Service.Record.EnvironmentID, sources.Service.Record.Desired.ID = environmentID, serviceID
	sources.Service.Record.Desired.Name = "worker"
	sources.Script.Record.EnvironmentID, sources.Script.Record.ServiceID = environmentID, serviceID
	sources.DesiredProjection.ReadRevision = sources.Revision
	sources.DesiredProjection.Record.EnvironmentID = environmentID
	for _, grant := range snapshot.ExplicitExecution.Context.Volumes {
		sources.DesiredProjection.Record.Volumes = append(
			sources.DesiredProjection.Record.Volumes, testenvironmentprojection.EnvironmentVolumeIdentity{
				ID: grant.VolumeId, Key: grant.VolumeId, Slug: grant.VolumeId,
			},
		)
	}
	for _, id := range snapshot.ExplicitExecution.Context.EntryIds {
		uid, gid := uint32(1001), uint32(1002)
		generation := "cfg_" + strings.TrimPrefix(id, "ev_")
		destination := "etc/tls/" + id
		sources.DesiredProjection.Record.Entries = append(sources.DesiredProjection.Record.Entries, testentries.Record{
			EnvironmentID: environmentID, CurrentValueGenerationID: generation,
			Entry: core.EnvEntry{ID: id, Kind: core.EntryKindFile, Path: destination, UID: &uid, GID: &gid,
				Source: core.EntrySource{Kind: core.SourceLiteral, Literal: "fixture"}, Exposure: []string{"worker"}},
		})
		snapshot.EntryBindings = append(snapshot.EntryBindings, &agentpb.ScriptRunnerEntryBinding{
			EntryId: id, ValueGenerationId: generation, Sha256: make([]byte, 32),
			Kind: agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_FILE, FileTarget: "/" + destination,
			Uid: uid, Gid: gid, Mode: 0o444,
		})
	}
	return sources, snapshot
}
