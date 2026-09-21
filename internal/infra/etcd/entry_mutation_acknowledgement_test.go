package etcd

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testtaskmaterialization "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/core"
	testentries "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// Rationale: Entry materialization must record the pinned generation needed by
// subsequent removal, without promoting unrelated pending workload decisions
// into the aggregate applied artifact owned by Service runtime receipts.
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
	task.Params[testtaskjournal.TaskResourceKindParam] = testtaskjournal.TaskResourceEntry
	baseline := environmentBlueprintTestProjection(environment.Record.ID, task, 1)
	baseline.RevisionID = ids.NewAt(ids.KindTask, task.CreatedAt, 300)
	baselineValue, err := testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(baseline)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(baselineValue)
	if _, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut,
		Key: testenvironmentprojection.EnvironmentComposeProjectionStorageKey(environment.Record.ID), Value: baselineValue}}); err != nil {
		t.Fatal(err)
	}
	candidate := testenvironmentprojection.CloneEnvironmentComposeProjection(baseline)
	candidate.RevisionID, candidate.RenderGeneration = task.ID, 2
	candidate.DesiredServices[0].Desired.Image = "example/api:pending"
	entry, err := testentries.NewRecord(environment.Record.ID, core.EnvEntry{
		ID: ids.NewAt(ids.KindEnvEntry, task.CreatedAt, 301), Kind: core.EntryKindEnv, Key: "MODE",
		Source: core.EntrySource{Kind: core.SourceLiteral, Literal: "test"}, Exposure: []string{"all"},
	}, ids.NewAt(ids.KindConfig, task.CreatedAt, 302))
	if err != nil {
		t.Fatal(err)
	}
	candidate.Entries = []testentries.Record{entry}
	stageEnvironmentBlueprintForPublicationTest(t, hierarchy, 0,
		environmentBlueprintTestRevision(environment.Record.ID, task, "services: {}\n"),
		candidate, environmentBlueprintTestMarker(task, environment.Record.ID))
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	read, err := store.Get(ctx, testenvironmentprojection.EnvironmentComposeProjectionStorageKey(environment.Record.ID))
	if err != nil {
		t.Fatal(err)
	}
	change, err := tasks.prepareTaskMaterializationProjectionAcknowledgement(
		ctx,
		task, testtaskjournal.TaskStatusCompleted, read.ReadRevision,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer clearTaskMaterializationProjectionChange(change)
	if len(change.mutations) != 1 {
		t.Fatalf("Entry completion produced %d applied writes; want one pinned Entry update", len(change.mutations))
	}
	applied, err := testenvironmentprojection.DecodeEnvironmentComposeProjectionStorage(change.mutations[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	if len(applied.Entries) != 1 || applied.Entries[0].CurrentValueGenerationID != entry.CurrentValueGenerationID {
		t.Fatal("Entry completion lost its materialized generation")
	}
	if applied.DesiredServices[0].Desired.Image != baseline.DesiredServices[0].Desired.Image {
		t.Fatal("Entry completion promoted an unrelated pending image")
	}
	if !bytes.Equal(applied.ComposeArtifact, baseline.ComposeArtifact) {
		t.Fatal("Entry completion replaced unrelated aggregate applied runtime")
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
	task.Params[testtaskjournal.TaskResourceKindParam] = testtaskjournal.TaskResourceEntry

	baseline := environmentBlueprintTestProjection(environment.Record.ID, task, 1)
	pendingVolume := baseline.Volumes[0]
	baseline.RevisionID = ids.NewAt(ids.KindTask, task.CreatedAt, 400)
	baseline.Volumes = nil
	baseline = withTestEnvironmentComposeArtifact(baseline)
	baselineValue, err := testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(baseline)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(baselineValue)
	if _, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut,
		Key: testenvironmentprojection.EnvironmentComposeProjectionStorageKey(environment.Record.ID), Value: baselineValue}}); err != nil {
		t.Fatal(err)
	}

	candidate := testenvironmentprojection.CloneEnvironmentComposeProjection(baseline)
	candidate.RevisionID, candidate.RenderGeneration = task.ID, 2
	candidate.Volumes = []testenvironmentprojection.EnvironmentVolumeIdentity{pendingVolume}
	candidate = withTestEnvironmentComposeArtifact(candidate)
	uid, gid := uint32(82), uint32(82)
	entry, err := testentries.NewRecord(environment.Record.ID, core.EnvEntry{
		ID: ids.NewAt(ids.KindEnvEntry, task.CreatedAt, 401), Kind: core.EntryKindFile, Path: "config/mode",
		UID: &uid, GID: &gid, Source: core.EntrySource{Kind: core.SourceLiteral, Literal: "test"}, Exposure: []string{"all"},
	}, ids.NewAt(ids.KindConfig, task.CreatedAt, 402))
	if err != nil {
		t.Fatal(err)
	}
	candidate.Entries = []testentries.Record{entry}
	task.Materializations = []testtaskmaterialization.Record{{
		StepID: task.Steps[0].ID, MaterializationID: ids.NewAt(ids.KindConfig, task.CreatedAt, 403),
		EnvironmentID: environment.Record.ID, Destination: "config/mode",
		OutputKind: testtaskmaterialization.OutputPlainFile, UID: 82, GID: 82, Mode: 0o444,
		Length: 4, SHA256: strings.Repeat("a", 64),
		Source: testtaskmaterialization.Source{Kind: testtaskmaterialization.SourceEntryValue,
			EntryValue: &testtaskmaterialization.EntryValueReference{EntryID: entry.Entry.ID,
				ValueGenerationID: entry.CurrentValueGenerationID, Storage: testtaskmaterialization.EntryValueStoragePlain}},
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
	read, err := store.Get(ctx, testenvironmentprojection.EnvironmentComposeProjectionStorageKey(environment.Record.ID))
	if err != nil {
		t.Fatal(err)
	}
	change, err := tasks.prepareTaskMaterializationProjectionAcknowledgement(
		ctx, task, testtaskjournal.TaskStatusCompleted, read.ReadRevision,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer clearTaskMaterializationProjectionChange(change)
	if len(change.mutations) != 1 {
		t.Fatalf("Entry completion produced %d applied writes; want one pinned Entry update", len(change.mutations))
	}
	applied, err := testenvironmentprojection.DecodeEnvironmentComposeProjectionStorage(change.mutations[0].Value)
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
