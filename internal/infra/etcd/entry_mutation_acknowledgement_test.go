package etcd

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
)

// Rationale: Entry materialization must record the pinned generation needed by
// subsequent removal, without promoting unrelated pending workload decisions.
func TestEntryMutationAcknowledgementRecordsEntriesWithoutPromotingWorkloads(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	project, environment := createEnvironmentBlueprintOwners(t, hierarchy)
	task := environmentBlueprintTestTask(t, project.Record, environment.Record, 280)
	task.RenderGeneration = 2
	task.Params[TaskResourceKindParam] = TaskResourceEntry
	baseline := environmentBlueprintTestProjection(environment.Record.ID, task, 1)
	baseline.RevisionID = ids.NewAt(ids.KindTask, task.CreatedAt, 300)
	baselineValue, err := encodeEnvironmentComposeProjection(baseline)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(baselineValue)
	if _, err := store.Transact(ctx, nil, []Mutation{{Type: MutationPut,
		Key: environmentComposeProjectionKey(environment.Record.ID), Value: baselineValue}}); err != nil {
		t.Fatal(err)
	}
	candidate := cloneEnvironmentComposeProjection(baseline)
	candidate.RevisionID, candidate.RenderGeneration = task.ID, 2
	candidate.DesiredServices[0].Desired.Image = "example/api:pending"
	entry, err := NewEntryRecord(environment.Record.ID, core.EnvEntry{
		ID: ids.NewAt(ids.KindEnvEntry, task.CreatedAt, 301), Kind: core.EntryKindEnv, Key: "MODE",
		Source: core.EntrySource{Kind: core.SourceLiteral, Literal: "test"}, Exposure: []string{"all"},
	}, ids.NewAt(ids.KindConfig, task.CreatedAt, 302))
	if err != nil {
		t.Fatal(err)
	}
	candidate.Entries = []EntryRecord{entry}
	stageEnvironmentBlueprintForPublicationTest(t, hierarchy, 0,
		environmentBlueprintTestRevision(environment.Record.ID, task, "services: {}\n"),
		candidate, environmentBlueprintTestMarker(task, environment.Record.ID))
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	read, err := store.Get(ctx, environmentComposeProjectionKey(environment.Record.ID))
	if err != nil {
		t.Fatal(err)
	}
	change, err := tasks.prepareTaskMaterializationProjectionAcknowledgement(
		ctx,
		task,
		TaskStatusCompleted,
		read.ReadRevision,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer clearTaskMaterializationProjectionChange(change)
	if len(change.mutations) != 1 {
		t.Fatalf("Entry completion produced %d applied writes; want one pinned Entry update", len(change.mutations))
	}
	applied, err := decodeEnvironmentComposeProjection(change.mutations[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	if len(applied.Entries) != 1 || applied.Entries[0].CurrentValueGenerationID != entry.CurrentValueGenerationID {
		t.Fatal("Entry completion lost its materialized generation")
	}
	if applied.DesiredServices[0].Desired.Image != baseline.DesiredServices[0].Desired.Image {
		t.Fatal("Entry completion promoted an unrelated pending image")
	}
	if !bytes.Equal(applied.ComposeArtifact, candidate.ComposeArtifact) {
		t.Fatal("Entry completion did not retain the artifact applied by its Compose step")
	}
}

// Rationale: a materialization-only Entry update must not promote a newer
// desired artifact over independently older applied resource authority.
func TestEntryMutationAcknowledgementPreservesAppliedArtifactWithoutComposeApply(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	project, environment := createEnvironmentBlueprintOwners(t, hierarchy)
	task := environmentBlueprintTestTask(t, project.Record, environment.Record, 380)
	task.RenderGeneration = 2
	task.Params[TaskResourceKindParam] = TaskResourceEntry

	baseline := environmentBlueprintTestProjection(environment.Record.ID, task, 1)
	pendingVolume := baseline.Volumes[0]
	baseline.RevisionID = ids.NewAt(ids.KindTask, task.CreatedAt, 400)
	baseline.Volumes = nil
	baseline = withTestEnvironmentComposeArtifact(baseline)
	baselineValue, err := encodeEnvironmentComposeProjection(baseline)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(baselineValue)
	if _, err := store.Transact(ctx, nil, []Mutation{{Type: MutationPut,
		Key: environmentComposeProjectionKey(environment.Record.ID), Value: baselineValue}}); err != nil {
		t.Fatal(err)
	}

	candidate := cloneEnvironmentComposeProjection(baseline)
	candidate.RevisionID, candidate.RenderGeneration = task.ID, 2
	candidate.Volumes = []EnvironmentVolumeIdentity{pendingVolume}
	candidate = withTestEnvironmentComposeArtifact(candidate)
	uid, gid := uint32(82), uint32(82)
	entry, err := NewEntryRecord(environment.Record.ID, core.EnvEntry{
		ID: ids.NewAt(ids.KindEnvEntry, task.CreatedAt, 401), Kind: core.EntryKindFile, Path: "config/mode",
		UID: &uid, GID: &gid, Source: core.EntrySource{Kind: core.SourceLiteral, Literal: "test"}, Exposure: []string{"all"},
	}, ids.NewAt(ids.KindConfig, task.CreatedAt, 402))
	if err != nil {
		t.Fatal(err)
	}
	candidate.Entries = []EntryRecord{entry}
	task.Materializations = []TaskMaterializationRecord{{
		StepID: task.Steps[0].ID, MaterializationID: ids.NewAt(ids.KindConfig, task.CreatedAt, 403),
		EnvironmentID: environment.Record.ID, Destination: "config/mode",
		OutputKind: TaskMaterializationOutputPlainFile, UID: 82, GID: 82, Mode: 0o444,
		Length: 4, SHA256: strings.Repeat("a", 64),
		Source: TaskMaterializationSource{Kind: TaskMaterializationSourceEntryValue,
			EntryValue: &TaskEntryValueReference{EntryID: entry.Entry.ID,
				ValueGenerationID: entry.CurrentValueGenerationID, Storage: TaskEntryValueStoragePlain}},
	}}
	if err := validateTaskMaterializationReferences(task.Materializations, task.Steps,
		environment.Record.ID, true, uint64(task.RenderGeneration)); err != nil {
		t.Fatal(err)
	}
	stageEnvironmentBlueprintForPublicationTest(t, hierarchy, 0,
		environmentBlueprintTestRevision(environment.Record.ID, task, "services: {}\n"),
		candidate, environmentBlueprintTestMarker(task, environment.Record.ID))
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	read, err := store.Get(ctx, environmentComposeProjectionKey(environment.Record.ID))
	if err != nil {
		t.Fatal(err)
	}
	change, err := tasks.prepareTaskMaterializationProjectionAcknowledgement(
		ctx, task, TaskStatusCompleted, read.ReadRevision,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer clearTaskMaterializationProjectionChange(change)
	if len(change.mutations) != 1 {
		t.Fatalf("Entry completion produced %d applied writes; want one pinned Entry update", len(change.mutations))
	}
	applied, err := decodeEnvironmentComposeProjection(change.mutations[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	if len(applied.Entries) != 1 || applied.Entries[0].CurrentValueGenerationID != entry.CurrentValueGenerationID {
		t.Fatal("Entry completion lost its materialized generation")
	}
	if len(applied.Volumes) != 0 || !bytes.Equal(applied.ComposeArtifact, baseline.ComposeArtifact) {
		t.Fatal("materialization-only Entry completion changed applied resource authority")
	}
}
