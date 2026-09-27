package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	resolutionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hostresolution"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// DNS-04: Environment-scoped Component completion must promote the candidate
// into DNS planning and publish its projection, just like a direct Component Task.
func TestBlueprintComponentCompletionReconcilesHostResolution(t *testing.T) {
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	now := taskJournalTime()
	task := validTaskRecord(now)
	task.Type = taskjournal.TaskUpdate
	task.Target = ids.NewAt(ids.KindEnvironment, now, 2101)
	pinComponentTaskDesiredRevision(&task)
	task.Params[componentTaskBlueprintProcedureParam] = componentTaskBlueprintProcedureNone
	delete(task.Params, taskjournal.TaskResourceKindParam)
	createLifecycleTask(t, repository, task)
	records := componentTaskLifecycleRecords(t, task.Target, now, true)
	seedComponentTaskLifecycle(t, store, task, records)
	read, err := store.Get(ctx, resolutionrecord.StorageKey)
	if err != nil {
		t.Fatal(err)
	}
	_, _, components, err := repository.hostResolutionTerminalOverlay(
		ctx,
		task,
		taskjournal.TaskStatusCompleted,
		read.ReadRevision,
	)
	if err != nil {
		t.Fatal(err)
	}
	component, found := components[records.candidate.Desired.ID]
	if !found || !component.Runtime.Healthy || component.Runtime.PinnedIPv4 != records.candidate.Runtime.PinnedIPv4 {
		t.Fatal("Blueprint completion did not promote the exact router candidate into DNS input")
	}
	change, err := repository.prepareHostResolutionReconciliation(
		ctx,
		task,
		taskjournal.TaskStatusCompleted,
		read.ReadRevision,
		nil,
		platformComponentTaskChange{},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer clearHostResolutionReconciliationChange(change)
	for _, mutation := range change.mutations {
		if mutation.Key == resolutionrecord.StorageKey {
			projection, err := resolutionrecord.DecodeHostResolutionProjectionRecord(mutation.Value)
			if err != nil || projection.InputRevision != read.ReadRevision {
				t.Fatalf("DNS projection not bound to terminal snapshot: %v", err)
			}
			return
		}
	}
	t.Fatal("Blueprint Component completion did not publish DNS projection")
}
