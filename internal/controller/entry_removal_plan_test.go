package controller

import (
	"bytes"
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	entrycapability "github.com/AlanD20/groundplane/internal/controller/entry"
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
	resolver, err := NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", reader, nil)
	if err != nil {
		t.Fatalf("NewTaskPlanResolverWithBlueprints() error = %v", err)
	}
	_, _, _, _, catalog := componentPlanProjectionInput(t)
	catalog[0].Plan = routeRemovalPlanProviderRenderer{}.Plan
	resolver.componentCatalog = catalog
	prepared, err := resolver.prepareEntryRemovalTask(
		context.Background(), task, intent,
		entryRemovalTaskProcedureIDs{
			ArtifactID: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		},
		entryRemovalMaterializationResolverFake{},
		entryRemovalIdentityFromReader(reader),
	)
	if err != nil {
		t.Fatalf("prepareEntryRemovalTask() error = %v", err)
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
		first.Operation != agentpb.PlanOperation_PLAN_OPERATION_REMOVE || second.Operation != first.Operation ||
		first.TargetId != intent.EntryID || second.TargetId != first.TargetId ||
		len(first.Artifacts) != 1 || len(first.Steps) != 1 || materialization == nil ||
		len(second.Steps) != 1 || second.Steps[0].StepId != first.Steps[0].StepId ||
		first.Steps[0].StepId != prepared.Steps[0].ID ||
		materialization.Destination != "config/app.yaml" || materialization.Uid != 1000 ||
		materialization.Gid != 1001 || materialization.Mode != 0o600 ||
		materialization.OutputKind != agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_REMOVE_SECRET_FILE {
		t.Fatalf("resolved Entry removal plans = %#v / %#v", first, second)
	}
}

// Rationale: immutable Blueprint metadata must be reparsed with the labels
// sealed at publication, so later hierarchy renames cannot strand a queued or
// retried Agent removal after Controller restart.
func TestTaskPlanResolverRebuildsEntryRemovalAfterHierarchyRename(t *testing.T) {
	t.Parallel()
	reader, intent, task := entryRemovalPlanTestState(t, core.EnvEntry{
		Kind: core.EntryKindFile, Path: "config/runtime.env", UID: uint32Pointer(1000), GID: uint32Pointer(1000),
		Source: core.EntrySource{Kind: core.SourceLiteral}, Exposure: []string{"api"},
	}, nil)
	resolver, err := NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", reader, nil)
	if err != nil {
		t.Fatalf("NewTaskPlanResolverWithBlueprints() error = %v", err)
	}
	_, _, _, _, catalog := componentPlanProjectionInput(t)
	catalog[0].Plan = routeRemovalPlanProviderRenderer{}.Plan
	resolver.componentCatalog = catalog
	identity := entryRemovalIdentityFromReader(reader)
	prepared, err := resolver.prepareEntryRemovalTask(
		context.Background(), task, intent,
		entryRemovalTaskProcedureIDs{ArtifactID: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
		entryRemovalMaterializationResolverFake{}, identity,
	)
	if err != nil {
		t.Fatalf("prepareEntryRemovalTask() error = %v", err)
	}
	reader.intent = intent
	reader.tenant.Slug = "renamed-tenant"
	reader.project.Slug = "renamed-project"
	reader.environment.Name = "renamed-environment"
	first, err := resolver.ResolveExecutionPlan(context.Background(), prepared)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan(after rename) error = %v", err)
	}
	second, err := resolver.ResolveExecutionPlan(context.Background(), prepared)
	if err != nil || !bytes.Equal(first.PlanHash, second.PlanHash) ||
		hex.EncodeToString(first.PlanHash) != prepared.PlanHash {
		t.Fatalf("reconstructed plans after rename = %#v/%#v/%v", first, second, err)
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

func TestEntryRemovalMaterializationTemplateUsesDesiredServiceIdentity(t *testing.T) {
	t.Parallel()
	_, intent, _ := entryRemovalPlanTestState(t, core.EnvEntry{
		Kind: core.EntryKindEnv, Key: "TOKEN", Exposure: []string{"api"},
	}, nil)
	template, err := entryRemovalEnvironmentTemplate(intent, "api")
	if err != nil {
		t.Fatalf("entryRemovalEnvironmentTemplate() error = %v", err)
	}
	if template.ServiceID != intent.CandidateProjection.DesiredServices[0].Desired.ID ||
		template.ServiceName != "api" || template.Destination != ServiceEnvFileName(intent.EnvironmentID, "api") {
		t.Fatalf("desired service identity = %#v", template)
	}
}

func TestEntryRemovalMaterializationTemplateResolvesGeneratedComponentService(t *testing.T) {
	t.Parallel()
	_, intent, _ := entryRemovalPlanTestState(t, core.EnvEntry{
		Kind: core.EntryKindEnv, Key: "TOKEN", Exposure: []string{"caddy"},
	}, nil)
	generatedServiceID := intent.CandidateProjection.Components[0].Runtime.GeneratedServices[0]
	template, err := entryRemovalEnvironmentTemplate(intent, "caddy")
	if err != nil {
		t.Fatalf("entryRemovalEnvironmentTemplate() error = %v", err)
	}
	if template.ServiceID != generatedServiceID || template.ServiceName != "caddy" ||
		template.Destination != ServiceEnvFileName(intent.EnvironmentID, "caddy") {
		t.Fatalf("generated component service identity = %#v", template)
	}
}

// Rationale: durable Entry removal records cross a capability boundary, so
// every closed output and storage variant must be translated explicitly and
// future variants must fail closed instead of passing through by string cast.
func TestEntryRemovalMaterializationConvertsClosedKinds(t *testing.T) {
	t.Parallel()
	outputs := []struct {
		input etcd.TaskMaterializationOutputKind
		want  entrycapability.RemovalOutputKind
	}{
		{
			input: etcd.TaskMaterializationOutputGeneratedEnvironment,
			want:  entrycapability.RemovalOutputGeneratedEnvironment,
		},
		{input: etcd.TaskMaterializationOutputPlainFile, want: entrycapability.RemovalOutputPlainFile},
		{input: etcd.TaskMaterializationOutputSecretFile, want: entrycapability.RemovalOutputSecretFile},
		{
			input: etcd.TaskMaterializationOutputRemoveGeneratedEnv,
			want:  entrycapability.RemovalOutputRemoveGeneratedEnv,
		},
		{input: etcd.TaskMaterializationOutputRemovePlainFile, want: entrycapability.RemovalOutputRemovePlainFile},
		{input: etcd.TaskMaterializationOutputRemoveSecretFile, want: entrycapability.RemovalOutputRemoveSecretFile},
	}
	for _, test := range outputs {
		converted, err := entryRemovalMaterialization(etcd.TaskMaterializationRecord{
			OutputKind: test.input,
			Source:     etcd.TaskMaterializationSource{Kind: etcd.TaskMaterializationSourceRemoval},
		})
		if err != nil || converted.OutputKind != test.want {
			t.Fatalf("entryRemovalMaterialization(%q) = %q, %v", test.input, converted.OutputKind, err)
		}
	}
	for _, storage := range []struct {
		input etcd.TaskEntryValueStorage
		want  entrycapability.RemovalValueStorage
	}{
		{input: etcd.TaskEntryValueStoragePlain, want: entrycapability.RemovalValueStoragePlain},
		{input: etcd.TaskEntryValueStorageSecret, want: entrycapability.RemovalValueStorageSecret},
	} {
		converted, err := entryRemovalMaterialization(generatedEntryRemovalMaterialization(storage.input))
		if err != nil || converted.Source.GeneratedEnvironment.Values[0].Storage != storage.want {
			t.Fatalf("entryRemovalMaterialization(storage %q) = %#v, %v", storage.input, converted, err)
		}
	}
	if _, err := entryRemovalMaterialization(etcd.TaskMaterializationRecord{
		OutputKind: "future",
		Source:     etcd.TaskMaterializationSource{Kind: etcd.TaskMaterializationSourceRemoval},
	}); err == nil {
		t.Fatal("entryRemovalMaterialization(invalid output) error = nil")
	}
	if _, err := entryRemovalMaterialization(generatedEntryRemovalMaterialization("future")); err == nil {
		t.Fatal("entryRemovalMaterialization(invalid storage) error = nil")
	}
}

func generatedEntryRemovalMaterialization(storage etcd.TaskEntryValueStorage) etcd.TaskMaterializationRecord {
	return etcd.TaskMaterializationRecord{
		OutputKind: etcd.TaskMaterializationOutputGeneratedEnvironment,
		Source: etcd.TaskMaterializationSource{
			Kind: etcd.TaskMaterializationSourceGeneratedEnvironment,
			GeneratedEnvironment: &etcd.TaskGeneratedEnvironmentValueReference{
				Values: []etcd.TaskGeneratedEnvironmentEntryReference{{
					Value: etcd.TaskEntryValueReference{Storage: storage},
				}},
			},
		},
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
		Owner: etcd.TaskOwner{
			WorkspaceType: etcd.TaskWorkspaceTenant, TenantID: baseReader.tenant.ID,
			ProjectID: baseReader.project.ID, EnvironmentID: baseReader.environment.ID,
		},
	}
	return reader, intent, task
}

func entryRemovalIdentityFromReader(reader *entryRemovalPlanReader) entrycapability.RemovalEnvironmentIdentity {
	return entrycapability.RemovalEnvironmentIdentity{
		TenantID: reader.tenant.ID, TenantSlug: reader.tenant.Slug,
		ProjectID: reader.project.ID, ProjectSlug: reader.project.Slug,
		EnvironmentID: reader.environment.ID, EnvironmentName: reader.environment.Name,
		AuthorizedVolumeDir: reader.environment.VolumeDir,
	}
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
