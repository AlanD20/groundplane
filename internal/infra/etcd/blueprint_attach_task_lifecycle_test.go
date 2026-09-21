package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testattachments "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	testenvironmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

func TestBlueprintAttachTaskAdvancesEveryCandidateWithOneLifecycle(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 29, 13, 0, 0, 0, time.UTC)
	task := TaskRecord{
		ID: ids.NewAt(ids.KindTask, now, 1), Target: ids.NewAt(ids.KindEnvironment, now, 2), CreatedAt: now,
	}
	backingProjectID := ids.NewAt(ids.KindProject, now, 3)
	backingEnvironmentID := ids.NewAt(ids.KindEnvironment, now, 4)
	backingServiceID := ids.NewAt(ids.KindService, now, 5)
	backingNetworkID := ids.NewAt(ids.KindNetwork, now, 6)
	ownerID := ids.NewAt(ids.KindAttach, now, 7)
	dependentID := ids.NewAt(ids.KindAttach, now, 8)
	owner, err := testattachments.NewPendingAttachRecord(
		ownerID, task.Target, "api-db", backingProjectID, backingEnvironmentID,
		backingServiceID, backingNetworkID, ids.NewAt(ids.KindService, now, 9),
		ownerID, nil, nil, task.ID, now,
	)
	if err != nil {
		t.Fatalf("NewPendingAttachRecord(owner) error = %v", err)
	}
	dependent, err := testattachments.NewPendingAttachRecord(
		dependentID, task.Target, "worker-db", backingProjectID, backingEnvironmentID,
		backingServiceID, backingNetworkID, ids.NewAt(ids.KindService, now, 10),
		ownerID, nil, nil, task.ID, now,
	)
	if err != nil {
		t.Fatalf("NewPendingAttachRecord(dependent) error = %v", err)
	}
	intent := testattachments.BlueprintAttachTaskIntent{
		TaskID: task.ID, EnvironmentID: task.Target, Status: testtaskjournal.TaskStatusPending,
		OwnsEnvironmentFence: true, Candidates: []testattachments.Record{owner, dependent}, CreatedAt: now,
	}
	intentValue, err := testattachments.EncodeBlueprintAttachTaskIntent(intent)
	if err != nil {
		t.Fatalf("encodeBlueprintAttachTaskIntent() error = %v", err)
	}
	store := newMemoryTaskStore()
	seedTaskRepositoryValue(t, store, testattachments.BlueprintAttachTaskIntentKey(task.ID), intentValue)
	for _, record := range []testattachments.Record{owner, dependent} {
		value, encodeErr := testattachments.EncodeAttachRecord(record)
		if encodeErr != nil {
			t.Fatalf("encodeAttachRecord(%s) error = %v", record.ID, encodeErr)
		}
		seedTaskRepositoryValue(t, store, testattachments.AttachKey(record.ID), value)
	}
	seedTaskRepositoryValue(
		t,
		store,
		testenvironmentchanges.ComponentTaskActiveEnvironmentKey(task.Target),
		[]byte(task.ID),
	)
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	claim, err := repository.prepareBlueprintAttachTaskClaim(ctx, task, store.revision)
	if err != nil {
		t.Fatalf("prepareBlueprintAttachTaskClaim() error = %v", err)
	}
	applyBlueprintAttachTaskChange(t, store, claim)
	assertBlueprintAttachStatuses(t, store, []string{ownerID, dependentID}, core.AttachProvisioning)

	terminalAt := now.Add(time.Minute)
	terminal, err := repository.prepareBlueprintAttachTaskAcknowledgement(
		ctx, task, testtaskjournal.TaskStatusCompleted, terminalAt, store.revision,
	)
	if err != nil {
		t.Fatalf("prepareBlueprintAttachTaskAcknowledgement() error = %v", err)
	}
	applyBlueprintAttachTaskChange(t, store, terminal)
	assertBlueprintAttachStatuses(t, store, []string{ownerID, dependentID}, core.AttachReady)
	storedIntent, err := store.Get(ctx, testattachments.BlueprintAttachTaskIntentKey(task.ID))
	if err != nil || storedIntent.Entry == nil {
		t.Fatalf("Get(intent) = %#v, %v", storedIntent, err)
	}
	completed, err := testattachments.DecodeBlueprintAttachTaskIntent(storedIntent.Entry.Value)
	if err != nil || completed.Status != testtaskjournal.TaskStatusCompleted || completed.TerminalAt == nil ||
		!completed.TerminalAt.Equal(terminalAt) {
		t.Fatalf("terminal intent = %#v, %v", completed, err)
	}
	active, err := store.Get(ctx, testenvironmentchanges.ComponentTaskActiveEnvironmentKey(task.Target))
	if err != nil || active.Entry != nil {
		t.Fatalf("active Environment fence = %#v, %v, want absent", active, err)
	}
}

func applyBlueprintAttachTaskChange(t *testing.T, store *memoryTaskStore, change blueprintAttachTaskChange) {
	t.Helper()
	defer clearBlueprintAttachTaskChange(change)
	result, err := store.Transact(context.Background(), change.conditions, change.mutations)
	if err != nil || !result.Succeeded {
		t.Fatalf("Transact(Blueprint Attach change) = %#v, %v", result, err)
	}
}

func assertBlueprintAttachStatuses(
	t *testing.T,
	store *memoryTaskStore,
	attachIDs []string,
	want core.AttachStatus,
) {
	t.Helper()
	for _, attachID := range attachIDs {
		stored, err := store.Get(context.Background(), testattachments.AttachKey(attachID))
		if err != nil || stored.Entry == nil {
			t.Fatalf("Get(Attach %s) = %#v, %v", attachID, stored, err)
		}
		record, err := testattachments.DecodeAttachRecord(stored.Entry.Value)
		if err != nil || record.Status != want {
			t.Fatalf("Attach %s = %#v, %v, want status %s", attachID, record, err, want)
		}
	}
}
