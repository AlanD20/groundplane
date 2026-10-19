package app

import (
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// TASK-127: read native prerequisite snapshots at the selected MVCC revision,
// including when the producing Task record is absent. Missing Apply authority must
// not become an empty dependency set; an ordinary direct Attach needs no Apply
// snapshot. Uses actual repositories over MVCC memory; this is not Task-collector
// qualification or an authored Apply journey.
func TestNativeBlueprintPrerequisiteSnapshotSurvivesHistoryAndRefusesMissingAuthority(t *testing.T) {
	ctx := t.Context()
	store := newMemoryHierarchyStore()
	store.revision = 1
	repository, err := etcd.NewEnvironmentBlueprintRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	environment := ids.New(ids.KindEnvironment)
	task := workexecution.Task{ID: ids.New(ids.KindTask), Actor: workexecution.OperatorActor,
		Scope: workexecution.Scope{
			TenantID:      ids.New(ids.KindTenant),
			ProjectID:     ids.New(ids.KindProject),
			EnvironmentID: environment,
		},
		Action: workexecution.ActionApply, TargetID: environment,
		Intent:        workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("a", 64)},
		ExpectedItems: 1, PublicationComplete: true, Progress: workexecution.Progress{Ready: 1},
		CreatedAt: at, UpdatedAt: at, Deadline: at.Add(time.Hour)}
	putTask := func(task workexecution.Task) {
		t.Helper()
		raw, err := workqueue.EncodeTask(task)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.Put(ctx, workqueue.TaskKey(task.ID), raw); err != nil {
			t.Fatal(err)
		}
	}
	putTask(task)
	if _, _, err := repository.GetBlueprintPrerequisiteAuthority(ctx, environment, task.ID, 0); err == nil {
		t.Fatal("missing native Apply prerequisite snapshot was treated as no dependencies")
	}
	authority, err := etcd.NewBlueprintPrerequisiteAuthority(environment, task.ID, core.BlueprintRequirements{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := recordcodec.Encode("blueprint-prerequisite-authority", authority)
	if err != nil {
		t.Fatal(err)
	}
	key := blueprints.EnvironmentBlueprintRevisionPrefixFinal(environment, task.ID) + "prerequisite-authority"
	pinned, err := store.Put(ctx, key, raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, key, []byte("corrupt newer value")); err != nil {
		t.Fatal(err)
	}
	selected, found, err := repository.GetBlueprintPrerequisiteAuthority(ctx, environment, task.ID, pinned)
	if err != nil || !found || selected.Record.RevisionID != task.ID || selected.ReadRevision != pinned ||
		selected.Revision != pinned {
		t.Fatalf("fixed native prerequisite snapshot was replaced by newer state: %v", err)
	}
	if _, _, err := repository.GetBlueprintPrerequisiteAuthority(ctx, environment, task.ID, 0); err == nil {
		t.Fatal("corrupt current native snapshot was accepted")
	}
	if _, err := store.Put(ctx, key, raw); err != nil {
		t.Fatal(err)
	}
	deleted, err := store.Transact(
		ctx,
		nil,
		[]keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: workqueue.TaskKey(task.ID)}},
	)
	if err != nil || !deleted.Succeeded {
		t.Fatalf("expire producing Task fixture: %v", err)
	}
	selected, found, err = repository.GetBlueprintPrerequisiteAuthority(ctx, environment, task.ID, 0)
	if err != nil || !found || selected.Record.RevisionID != task.ID {
		t.Fatalf("retained prerequisite snapshot depended on Task diagnostics: %v", err)
	}
	direct := task
	direct.ID, direct.Action = ids.New(ids.KindTask), workexecution.ActionAttach
	putTask(direct)
	if _, found, err := repository.GetBlueprintPrerequisiteAuthority(ctx, environment, direct.ID, 0); err != nil ||
		found {
		t.Fatalf("direct Attach incorrectly requires an Apply snapshot: found=%t %v", found, err)
	}
	foreign := task
	foreign.ID, foreign.Scope.EnvironmentID = ids.New(ids.KindTask), ids.New(ids.KindEnvironment)
	foreign.TargetID = foreign.Scope.EnvironmentID
	putTask(foreign)
	if _, _, err := repository.GetBlueprintPrerequisiteAuthority(ctx, environment, foreign.ID, 0); err == nil {
		t.Fatal("foreign Environment's producer supplied prerequisite authority")
	}
}
