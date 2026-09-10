package controller

import (
	"context"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type explicitScriptEntryValues struct {
	ids      []string
	returned [][]byte
}

func (values *explicitScriptEntryValues) ResolveTaskMaterializationSource(
	_ context.Context, _ string, source etcd.TaskMaterializationSource,
) ([]byte, error) {
	values.ids = append(values.ids, source.EntryValue.EntryID)
	value := []byte("private fixture bytes")
	values.returned = append(values.returned, value)
	return value, nil
}

// Rationale: explicit mode grants exact Entries, never every exposed credential;
// omitted lists mean no access. Resolved temporary values are still discarded.
func TestExplicitScriptEntriesResolveOnlyExactGrants(t *testing.T) {
	for _, grant := range []bool{false, true} {
		sources := explicitScriptEntrySources()
		if !grant {
			sources.Script.Record.Desired.Execution.EntryIDs = nil
		}
		values := &explicitScriptEntryValues{}
		service := &ScriptArtifactService{values: values}
		bindings, err := service.BuildScriptEntryBindings(context.Background(), sources)
		if err != nil {
			t.Fatal(err)
		}
		want := len(sources.Script.Record.Desired.Execution.EntryIDs)
		if len(bindings) != want || len(values.ids) != want {
			t.Fatalf("resolved undeclared Entries: bindings=%d reads=%d want=%d", len(bindings), len(values.ids), want)
		}
		for _, id := range values.ids {
			if id == sources.DesiredProjection.Record.Entries[2].Entry.ID {
				t.Fatal("unselected exposed secret was resolved")
			}
		}
		for _, value := range values.returned {
			for _, octet := range value {
				if octet != 0 {
					t.Fatal("resolved temporary Entry bytes were retained")
				}
			}
		}
		if grant && (bindings[0].Kind != agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_ENV ||
			!bindings[0].Secret || bindings[1].FileTarget != "/etc/tls/setup.conf" || bindings[1].Uid != 1001 ||
			bindings[1].Gid != 1002 || bindings[1].Mode != 0o444) {
			t.Fatal("selected Entry metadata lost its exact destination or ownership")
		}
	}
}

// Rationale: all grant ownership, exposure and file/Volume target checks must
// finish before resolving even the first selected secret value.
func TestExplicitScriptEntriesRejectInvalidResourcesBeforeValueReads(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*etcd.ScriptExecutionSources)
	}{
		{"missing Entry", func(s *etcd.ScriptExecutionSources) {
			s.DesiredProjection.Record.Entries = s.DesiredProjection.Record.Entries[:1]
		}},
		{"foreign Entry", func(s *etcd.ScriptExecutionSources) {
			s.DesiredProjection.Record.Entries[1].EnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		}},
		{"unexposed Entry", func(s *etcd.ScriptExecutionSources) {
			s.DesiredProjection.Record.Entries[1].Entry.Exposure = []string{"other"}
		}},
		{"duplicate Entry primary", func(s *etcd.ScriptExecutionSources) {
			s.DesiredProjection.Record.Entries = append(s.DesiredProjection.Record.Entries, s.DesiredProjection.Record.Entries[1])
		}},
		{"missing Volume", func(s *etcd.ScriptExecutionSources) { s.DesiredProjection.Record.Volumes = nil }},
		{"duplicate Volume identity", func(s *etcd.ScriptExecutionSources) {
			s.DesiredProjection.Record.Volumes = append(s.DesiredProjection.Record.Volumes, s.DesiredProjection.Record.Volumes[0])
		}},
		{"foreign projection", func(s *etcd.ScriptExecutionSources) {
			s.DesiredProjection.Record.EnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		}},
		{"foreign Script", func(s *etcd.ScriptExecutionSources) { s.Script.Record.ServiceID = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW" }},
		{"overlapping Volume", func(s *etcd.ScriptExecutionSources) { s.Script.Record.Desired.Execution.Volumes[0].Target = "/etc/tls" }},
		{"runner body", func(s *etcd.ScriptExecutionSources) {
			s.DesiredProjection.Record.Entries[1].Entry.Path = "groundplane-script-body"
		}},
		{"reserved interpreter", func(s *etcd.ScriptExecutionSources) { s.DesiredProjection.Record.Entries[1].Entry.Path = "bin/sh" }},
		{"traversal", func(s *etcd.ScriptExecutionSources) {
			s.DesiredProjection.Record.Entries[1].Entry.Path = "etc/../setup.conf"
		}},
		{"missing ownership", func(s *etcd.ScriptExecutionSources) { s.DesiredProjection.Record.Entries[1].Entry.UID = nil }},
		{"overlapping Entry files", func(s *etcd.ScriptExecutionSources) {
			s.DesiredProjection.Record.Entries[2].Entry = s.DesiredProjection.Record.Entries[1].Entry
			s.DesiredProjection.Record.Entries[2].Entry.ID = "ev_01ARZ3NDEKTSV4RRFFQ69G5FAX"
			s.DesiredProjection.Record.Entries[2].Entry.Path = "etc/tls/setup.conf/child"
			s.Script.Record.Desired.Execution.EntryIDs = append(s.Script.Record.Desired.Execution.EntryIDs, s.DesiredProjection.Record.Entries[2].Entry.ID)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			sources := explicitScriptEntrySources()
			test.edit(&sources)
			values := &explicitScriptEntryValues{}
			service := &ScriptArtifactService{values: values}
			_, err := service.BuildScriptEntryBindings(context.Background(), sources)
			kind, _ := errs.KindOf(err)
			if kind != errs.KindValidationFailed || len(values.ids) != 0 {
				t.Fatalf("invalid resources reached value resolution: error=%v reads=%d", err, len(values.ids))
			}
		})
	}
}

// Rationale: selecting explicit grants must not change established inherited
// behavior: all eligible exposed Entries remain available in inherited mode.
func TestExplicitScriptEntriesPreserveInheritedSelection(t *testing.T) {
	for _, execution := range []*core.ScriptExecution{nil, {Mode: core.ScriptExecutionInherited}} {
		sources := explicitScriptEntrySources()
		sources.Script.Record.Desired.Execution = execution
		values := &explicitScriptEntryValues{}
		bindings, err := (&ScriptArtifactService{values: values}).BuildScriptEntryBindings(
			context.Background(),
			sources,
		)
		if err != nil || len(bindings) != 3 || len(values.ids) != 3 {
			t.Fatalf("inherited selection changed: %v", err)
		}
	}
}

func explicitScriptEntrySources() etcd.ScriptExecutionSources {
	const environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	const serviceID = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	const volumeID = "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	first, second, third := "ev_01ARZ3NDEKTSV4RRFFQ69G5FAV", "ev_01ARZ3NDEKTSV4RRFFQ69G5FAW", "ev_01ARZ3NDEKTSV4RRFFQ69G5FAX"
	uid, gid := uint32(1001), uint32(1002)
	sources := etcd.ScriptExecutionSources{}
	sources.Environment.Record.ID = environmentID
	sources.Service.Record.EnvironmentID, sources.Service.Record.Desired.ID = environmentID, serviceID
	sources.Service.Record.Desired.Name = "worker"
	sources.Script.Record.EnvironmentID, sources.Script.Record.ServiceID = environmentID, serviceID
	sources.Script.Record.Desired.ID = "scr_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	sources.Script.Record.Desired.Execution = &core.ScriptExecution{
		Mode: core.ScriptExecutionExplicit, Image: "example/setup@sha256:" + strings.Repeat("b", 64), User: "0:0",
		Volumes: []core.ScriptVolumeGrant{{VolumeID: volumeID, Target: "/state"}}, EntryIDs: []string{second, first},
	}
	sources.DesiredProjection.Record.EnvironmentID = environmentID
	sources.DesiredProjection.Record.Volumes = []etcd.EnvironmentVolumeIdentity{
		{ID: volumeID, Key: "state", Slug: "state"},
	}
	entries := []core.EnvEntry{
		{
			ID:       first,
			Kind:     core.EntryKindEnv,
			Key:      "SETUP_SECRET",
			Secret:   true,
			Exposure: []string{"worker"},
			Source:   core.EntrySource{Kind: core.SourceSecretRef, SecretRef: "setup-secret"},
		},
		{
			ID:       second,
			Kind:     core.EntryKindFile,
			Path:     "etc/tls/setup.conf",
			UID:      &uid,
			GID:      &gid,
			Exposure: []string{"worker"},
			Source:   core.EntrySource{Kind: core.SourceLiteral, Literal: "config"},
		},
		{
			ID:       third,
			Kind:     core.EntryKindEnv,
			Key:      "UNSELECTED_SECRET",
			Secret:   true,
			Exposure: []string{"all"},
			Source:   core.EntrySource{Kind: core.SourceSecretRef, SecretRef: "unselected-secret"},
		},
	}
	for _, entry := range entries {
		sources.DesiredProjection.Record.Entries = append(sources.DesiredProjection.Record.Entries, etcd.EntryRecord{
			EnvironmentID: environmentID, Entry: entry, CurrentValueGenerationID: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		})
	}
	return sources
}
