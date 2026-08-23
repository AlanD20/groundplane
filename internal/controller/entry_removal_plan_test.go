package controller

import (
	"bytes"
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type entryRemovalPlanReader struct {
	*blueprintPlanReader
	intent etcd.EntryRemovalIntent
}

func (reader *entryRemovalPlanReader) GetEntryRemovalIntent(
	context.Context,
	string,
) (etcd.Versioned[etcd.EntryRemovalIntent], bool, error) {
	return etcd.Versioned[etcd.EntryRemovalIntent]{Record: reader.intent}, true, nil
}

type entryRemovalMaterializationResolverFake struct {
	content []byte
}

func (fake entryRemovalMaterializationResolverFake) ResolveTaskMaterializationSource(
	_ context.Context,
	_ string,
	source etcd.TaskMaterializationSource,
) ([]byte, error) {
	if source.Kind == etcd.TaskMaterializationSourceRemoval {
		return nil, nil
	}
	return append([]byte(nil), fake.content...), nil
}

// Rationale: an applied file Entry removal must reconstruct one authenticated
// unlink procedure with the exact path, storage class, ownership, mode, and
// candidate artifact while never introducing a Compose mutation step.
func TestTaskPlanResolverRebuildsFileEntryRemoval(t *testing.T) {
	t.Parallel()
	reader, intent, task := entryRemovalPlanTestState(t, core.EnvEntry{
		Kind: core.EntryKindFile, Path: "config/app.yaml", UID: uint32Pointer(1000), GID: uint32Pointer(1001),
		Source: core.EntrySource{Kind: core.SourceLiteral}, Exposure: []string{"api"}, Secret: true,
	}, nil)
	resolver, err := NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", reader)
	if err != nil {
		t.Fatalf("NewTaskPlanResolverWithBlueprints() error = %v", err)
	}
	_, _, _, _, catalog := componentPlanProjectionInput(t)
	catalog[0].Environment = routeRemovalPlanCaddyRenderer{}
	resolver.componentCatalog = catalog
	prepared, err := resolver.PrepareEntryRemovalTask(
		context.Background(), task, intent,
		EntryRemovalTaskProcedureIDs{
			ArtifactID: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		},
		entryRemovalMaterializationResolverFake{},
	)
	if err != nil {
		t.Fatalf("PrepareEntryRemovalTask() error = %v", err)
	}
	reader.intent = intent
	first, err := resolver.ResolveExecutionPlan(context.Background(), prepared)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan() error = %v", err)
	}
	second, err := resolver.ResolveExecutionPlan(context.Background(), prepared)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan(replay) error = %v", err)
	}
	materialization := first.Steps[0].GetMaterializeFile()
	if !bytes.Equal(first.PlanHash, second.PlanHash) || hex.EncodeToString(first.PlanHash) != prepared.PlanHash ||
		first.Operation != agentpb.PlanOperation_PLAN_OPERATION_REMOVE || first.TargetId != intent.EntryID ||
		len(first.Artifacts) != 1 || len(first.Steps) != 1 || materialization == nil ||
		materialization.Destination != "config/app.yaml" || materialization.Uid != 1000 ||
		materialization.Gid != 1001 || materialization.Mode != 0o600 ||
		materialization.OutputKind != agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_REMOVE_SECRET_FILE {
		t.Fatalf("resolved Entry removal plans = %#v / %#v", first, second)
	}
}

// Rationale: generated env cleanup must retain the canonical all-services file
// even when empty, remove an empty service-specific file, and rewrite a scope
// from the remaining immutable Entry generations rather than current values.
func TestEntryRemovalMaterializationTemplatesCloseEnvironmentScopes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		removed    core.EnvEntry
		remaining  []core.EnvEntry
		wantKind   etcd.TaskMaterializationOutputKind
		wantValues int
		wantName   string
	}{
		{
			name:     "canonical file remains when empty",
			removed:  core.EnvEntry{Kind: core.EntryKindEnv, Key: "APP_ENV", Exposure: []string{"all"}},
			wantKind: etcd.TaskMaterializationOutputGeneratedEnvironment,
		},
		{
			name:     "service file removed when empty",
			removed:  core.EnvEntry{Kind: core.EntryKindEnv, Key: "TOKEN", Exposure: []string{"api"}},
			wantKind: etcd.TaskMaterializationOutputRemoveGeneratedEnv,
		},
		{
			name:    "service file rewritten from remaining generation",
			removed: core.EnvEntry{Kind: core.EntryKindEnv, Key: "OLD", Exposure: []string{"api"}},
			remaining: []core.EnvEntry{{
				Kind: core.EntryKindEnv, Key: "TOKEN", Exposure: []string{"api"}, Secret: true,
			}},
			wantKind: etcd.TaskMaterializationOutputGeneratedEnvironment, wantValues: 1, wantName: "TOKEN",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader, intent, _ := entryRemovalPlanTestState(t, test.removed, test.remaining)
			_ = reader
			templates, err := entryRemovalMaterializationTemplates(intent)
			if err != nil || len(templates) != 1 || templates[0].OutputKind != test.wantKind {
				t.Fatalf("entryRemovalMaterializationTemplates() = %#v, %v", templates, err)
			}
			values := []etcd.TaskGeneratedEnvironmentEntryReference(nil)
			if templates[0].Source.GeneratedEnvironment != nil {
				values = templates[0].Source.GeneratedEnvironment.Values
			}
			if len(values) != test.wantValues || test.wantValues != 0 && values[0].Name != test.wantName {
				t.Fatalf("generated values = %#v", values)
			}
		})
	}
}

func entryRemovalPlanTestState(
	t *testing.T,
	removed core.EnvEntry,
	remaining []core.EnvEntry,
) (*entryRemovalPlanReader, etcd.EntryRemovalIntent, etcd.TaskRecord) {
	t.Helper()
	baseReader, _, _ := routeRemovalPlanTestState(t)
	at := time.Date(2026, 8, 23, 6, 0, 0, 0, time.UTC)
	projection := baseReader.projection
	projection.Entries = nil
	removed.ID = ids.NewAt(ids.KindEnvEntry, at, 1)
	removed.Source.Kind = core.SourceLiteral
	removedRecord, err := etcd.NewEntryRecord(
		projection.EnvironmentID,
		removed,
		ids.NewAt(ids.KindConfig, at, 2),
	)
	if err != nil {
		t.Fatalf("NewEntryRecord(removed) error = %v", err)
	}
	projection.Entries = append(projection.Entries, removedRecord)
	for index := range remaining {
		remaining[index].ID = ids.NewAt(ids.KindEnvEntry, at, int64(index)+10)
		remaining[index].Source.Kind = core.SourceLiteral
		record, recordErr := etcd.NewEntryRecord(
			projection.EnvironmentID,
			remaining[index],
			ids.NewAt(ids.KindConfig, at, int64(index)+20),
		)
		if recordErr != nil {
			t.Fatalf("NewEntryRecord(remaining) error = %v", recordErr)
		}
		projection.Entries = append(projection.Entries, record)
	}
	sortEntryRecords(projection.Entries)
	taskID := ids.NewAt(ids.KindTask, at, 30)
	intent, err := etcd.NewEntryRemovalIntent(
		taskID,
		projection.EnvironmentID,
		removed.ID,
		41,
		&etcd.Versioned[etcd.EnvironmentComposeProjection]{Record: projection, Revision: 42, ReadRevision: 42},
		at,
	)
	if err != nil {
		t.Fatalf("NewEntryRemovalIntent() error = %v", err)
	}
	baseReader.projection = projection
	reader := &entryRemovalPlanReader{blueprintPlanReader: baseReader.blueprintPlanReader, intent: intent}
	task := etcd.TaskRecord{
		ID: taskID, OperationID: ids.NewAt(ids.KindOperation, at, 31), Executor: etcd.TaskExecutorAgent,
		PlanID: ids.NewAt(ids.KindPlan, at, 32), Type: etcd.TaskRemove, Target: removed.ID,
		TimeoutSeconds: 120, Status: etcd.TaskStatusPending, CreatedAt: at,
	}
	return reader, intent, task
}

func uint32Pointer(value uint32) *uint32 {
	return &value
}

func sortEntryRecords(records []etcd.EntryRecord) {
	for left := range records {
		for right := left + 1; right < len(records); right++ {
			if records[right].Entry.ID < records[left].Entry.ID {
				records[left], records[right] = records[right], records[left]
			}
		}
	}
}
