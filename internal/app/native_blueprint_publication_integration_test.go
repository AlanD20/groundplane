package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/blueprintcoordinator"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/api"
)

// TASK-122: actual captured Blueprint graph, atomic acceptance and bounded
// publication over MVCC memory. Lose a committed batch's response, then resume;
// Abort during publication must also fence members not published at that time.
// Operation planning/effects and desired-state admission are not simulated as
// successful here: this is the graph's publication/dispatch boundary only.
func TestNativeBlueprintPublicationResumesWithoutDuplicateWork(t *testing.T) {
	for _, abort := range []bool{false, true} {
		name := "resume"
		if abort {
			name = "abort_during_publication"
		}
		t.Run(name, func(t *testing.T) {
			f := newNativeBlueprintPublication(t, false)
			f.store.loseBatchResponse = true
			if err := f.publications.Advance(t.Context(), f.task.ID, time.Now().UTC()); !errors.Is(
				err,
				errLostBlueprintBatchResponse,
			) {
				t.Fatalf("committed response loss not reached: %v", err)
			}
			first, _, err := f.queue.ReadTask(t.Context(), f.task.ID, 0)
			if err != nil || first.Task.Progress.Total() != 8 || first.Task.PublicationComplete {
				t.Fatalf("first batch lost/doubled or opened dispatch early: %#v %v", first.Task.Progress, err)
			}
			page, err := f.queue.ListWork(t.Context(), f.task.ID, "", 100, 0)
			if err != nil || len(page.Items) != 8 {
				t.Fatalf("first membership: %v", err)
			}
			before := make(map[string]int64, len(page.Items))
			for _, item := range page.Items {
				before[item.Item.ID] = item.Revision
				f.checkCandidate(t, item.Item, false, false)
			}
			if abort {
				stop, err := workqueue.PrepareStop(first, workexecution.Cancelled, time.Now().UTC())
				if err != nil {
					t.Fatal(err)
				}
				f.commit(t, stop)
			}
			// Reconstruct both owner and publisher as a process restart would;
			// only persisted membership chooses the next bounded batch.
			f.publisher(t)
			for range 4 {
				if err := f.publications.Advance(t.Context(), f.task.ID, time.Now().UTC()); err != nil {
					t.Fatal(err)
				}
			}
			finished, _, err := f.queue.ReadTask(t.Context(), f.task.ID, 0)
			if err != nil || !finished.Task.PublicationComplete || finished.Task.Progress.Total() != 17 ||
				finished.Task.FinishedAt != nil {
				t.Fatalf("publication claimed execution or lost membership: %#v %v", finished.Task, err)
			}
			page, err = f.queue.ListWork(t.Context(), f.task.ID, "", 100, 0)
			if err != nil || len(page.Items) != 17 || page.More {
				t.Fatalf("final membership: %v", err)
			}
			for _, item := range page.Items {
				if rev := before[item.Item.ID]; rev != 0 && item.Revision != rev {
					t.Fatal("resume rewrote an already published item")
				}
				if item.Item.AttemptNumber != 0 || item.Item.ActiveAttemptID != "" || item.Item.Result != nil {
					t.Fatal("publication invented an attempt/result")
				}
				f.checkCandidate(t, item.Item, true, abort)
			}
			all, err := f.queue.ListTasks(t.Context(), "all", "", 100, 0)
			if err != nil || len(all.Tasks) != 1 || all.More || all.Tasks[0].Task.ID != f.task.ID {
				t.Fatalf("publication created hidden child Tasks: %#v %v", all, err)
			}
		})
	}
}

// An immutable captured graph is still validated when read. Corruption cannot
// turn a partially published Apply into executable work or change its members.
func TestNativeBlueprintPublicationRefusesChangedCapture(t *testing.T) {
	f := newNativeBlueprintPublication(t, false)
	if err := f.publications.Advance(t.Context(), f.task.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	before, _, err := f.queue.ReadTask(t.Context(), f.task.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	key := blueprintunits.CapturedGraphKey(f.task.Scope.EnvironmentID, f.task.ID)
	if _, err := f.store.Put(t.Context(), key, []byte("corrupt captured graph")); err != nil {
		t.Fatal(err)
	}
	if err := f.publications.Advance(t.Context(), f.task.ID, time.Now().UTC()); err == nil {
		t.Fatal("corrupt graph opened publication")
	}
	after, _, err := f.queue.ReadTask(t.Context(), f.task.ID, 0)
	if err != nil || after.Revision != before.Revision || after.Task.PublicationComplete {
		t.Fatalf("corrupt graph changed Task: %v", err)
	}
}

var errLostBlueprintBatchResponse = errors.New("lost committed Blueprint batch response")

// Obsolete input must settle without running its planner or inventing an
// attempt. A newer head racing that settlement must invalidate the prepared
// decision, preserving the original work until it is read again.
func TestNativeBlueprintInputSupersessionFencesPlanningAndConcurrentHead(t *testing.T) {
	for _, race := range []bool{false, true} {
		t.Run(map[bool]string{false: "settle", true: "concurrent_head"}[race], func(t *testing.T) {
			f := newNativeBlueprintPublication(t, true)
			for range 4 {
				if err := f.publications.Advance(t.Context(), f.task.ID, time.Now().UTC()); err != nil {
					t.Fatal(err)
				}
			}
			page, err := f.queue.ListWork(t.Context(), f.task.ID, "", 1, 0)
			if err != nil || len(page.Items) != 1 {
				t.Fatalf("published pending input: %v", err)
			}
			item := page.Items[0]
			headKey := blueprints.EnvironmentBlueprintHeadKey(f.task.Scope.EnvironmentID)
			putHead := func(taskID string) {
				t.Helper()
				value, err := idempotency.EncodeTaskReference(taskID)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.store.Put(t.Context(), headKey, value); err != nil {
					t.Fatal(err)
				}
			}
			putHead(f.task.ID)
			units, err := blueprintunits.NewRepository(f.store)
			if err != nil {
				t.Fatal(err)
			}
			planner := &nativeCountingInputPlanner{}
			registered, err := blueprintcoordinator.NewInputPreparations(
				f.queue,
				units,
				[]workdispatch.InputRegistration{
					{Action: workexecution.ActionApply, Operation: workexecution.Release, Owner: planner},
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			owner := registered[0].Owner
			if err := owner.Prepare(t.Context(), f.task.ID, item.Item.ID); err != nil || planner.calls != 1 {
				t.Fatalf("current captured input was not delegated to its typed planner: %v", err)
			}
			putHead(ids.New(ids.KindTask))
			if race {
				f.store.beforeCommit = func(mutations []keyvalue.Mutation) {
					for _, mutation := range mutations {
						if mutation.Key == workqueue.ItemKey(item.Item.ID) {
							f.store.beforeCommit = nil
							putHead(ids.New(ids.KindTask))
							break
						}
					}
				}
			}
			err = owner.Prepare(t.Context(), f.task.ID, item.Item.ID)
			after, readErr := f.queue.ReadWork(t.Context(), f.task.ID, item.Item.ID, 0)
			if readErr != nil || planner.calls != 1 || after.Item.Item.Input != nil ||
				after.Item.Item.AttemptNumber != 0 {
				t.Fatalf("supersession ran a stale planner or fabricated execution: %v", readErr)
			}
			if race {
				if err == nil || after.Item.Revision != item.Revision ||
					after.Item.Item.Phase != workexecution.Waiting {
					t.Fatalf("changed source committed a stale supersession decision: %v", err)
				}
			} else if err != nil || after.Item.Item.Phase != workexecution.Settled || after.Item.Item.Outcome != workexecution.Superseded ||
				after.Task.Task.Progress.Superseded != 1 {
				t.Fatalf("obsolete waiting work was not settled with its Task outcome: %v", err)
			}
		})
	}
}

type nativeCountingInputPlanner struct{ calls int }

func (planner *nativeCountingInputPlanner) Prepare(context.Context, string, string) error {
	planner.calls++
	return nil
}

type nativeBlueprintPublicationStore struct {
	*memoryHierarchyStore
	loseBatchResponse bool
	beforeCommit      func([]keyvalue.Mutation)
}

func (store *nativeBlueprintPublicationStore) Transact(ctx context.Context, conditions []keyvalue.Condition,
	mutations []keyvalue.Mutation,
) (keyvalue.TransactionResult, error) {
	if store.beforeCommit != nil {
		store.beforeCommit(mutations)
	}
	result, err := store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
	if err != nil || !result.Succeeded || !store.loseBatchResponse {
		return result, err
	}
	for _, mutation := range mutations {
		if mutation.Type == keyvalue.MutationPut && strings.HasPrefix(mutation.Key, workqueue.ItemKey("")) {
			store.loseBatchResponse = false
			return keyvalue.TransactionResult{}, errLostBlueprintBatchResponse
		}
	}
	return result, nil
}

type nativeBlueprintPublicationFixture struct {
	store        *nativeBlueprintPublicationStore
	queue        *workqueue.Repository
	task         workexecution.Task
	identity     workexecution.Identity
	publications *workdispatch.Publications
}

func newNativeBlueprintPublication(t *testing.T, waitingInput bool) *nativeBlueprintPublicationFixture {
	t.Helper()
	f := &nativeBlueprintPublicationFixture{
		store: &nativeBlueprintPublicationStore{memoryHierarchyStore: newMemoryHierarchyStore()},
	}
	f.store.revision = 1
	var err error
	f.queue, err = workqueue.NewRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	scope := workexecution.Scope{
		TenantID:      ids.New(ids.KindTenant),
		ProjectID:     ids.New(ids.KindProject),
		EnvironmentID: ids.New(ids.KindEnvironment),
	}
	f.task = workexecution.Task{
		ID:            ids.New(ids.KindTask),
		Actor:         workexecution.OperatorActor,
		Action:        workexecution.ActionApply,
		Scope:         scope,
		TargetID:      scope.EnvironmentID,
		TargetName:    "publication",
		Intent:        workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("a", 64)},
		ExpectedItems: 17,
		CreatedAt:     at,
		UpdatedAt:     at,
		Deadline:      at.Add(time.Hour),
	}
	prepared := make([]blueprintcoordinator.PreparedUnit, 0, 17)
	for index := 0; index < 17; index++ {
		target := blueprintunits.ResourceKey{Kind: ids.KindRoute, ID: ids.New(ids.KindRoute)}
		if waitingInput {
			target = blueprintunits.ResourceKey{Kind: ids.KindService, ID: ids.New(ids.KindService)}
		}
		unit := blueprintunits.Unit{
			Target:      target,
			Fingerprint: strings.Repeat("b", 64),
			Writes:      []blueprintunits.ResourceKey{target},
		}
		if waitingInput {
			prepared = append(prepared, blueprintcoordinator.PreparedUnit{Unit: unit, Operation: workexecution.Release,
				Placement: workexecution.Placement{Executor: workexecution.Agent, HostID: ids.New(ids.KindHost)}})
			continue
		}
		input, err := blueprintunits.DefinitionWorkInput(f.task, unit)
		if err != nil {
			t.Fatal(err)
		}
		prepared = append(
			prepared,
			blueprintcoordinator.PreparedUnit{Unit: unit, Operation: workexecution.ResourceDefinition,
				Placement: workexecution.Placement{Executor: workexecution.Controller}, Input: &input},
		)
	}
	graph, err := blueprintcoordinator.BuildWorkGraph(f.task, prepared)
	if err != nil {
		t.Fatal(err)
	}
	_, encrypted := manualJourneyEncryptedValue(t, "captured-blueprint-intent")
	defer clear(encrypted)
	digest := sha256.Sum256(encrypted)
	body, err := json.Marshal(api.TaskAccepted{TaskID: f.task.ID})
	if err != nil {
		t.Fatal(err)
	}
	marker := idempotency.IdempotencyMarker{
		Kind:  idempotency.IdempotencyMarkerTask,
		State: idempotency.IdempotencyMarkerPending,
		Locator: idempotency.IdempotencyLocator{
			ScopeKind: idempotency.IdempotencyScopeEnvironment,
			ScopeID:   scope.EnvironmentID,
			Method:    "POST",
			Route:     "/environments/{id}/blueprint",
			Key:       "native-blueprint-publication",
		},
		Intent: idempotency.ProtectedIntentRecord{
			EnvelopeVersion:  1,
			Cipher:           "age-x25519",
			DigestAlgorithm:  "sha256",
			CiphertextDigest: hex.EncodeToString(digest[:]),
			Ciphertext:       encrypted,
		},
		Response: idempotency.IdempotencyResponse{
			Status:      202,
			ContentKind: "application/json",
			Body:        body,
		},
		TaskID:    f.task.ID,
		CreatedAt: at,
		UpdatedAt: at,
	}
	task, err := workqueue.PrepareTask(f.task, marker)
	if err != nil {
		t.Fatal(err)
	}
	captured, err := blueprintunits.PrepareCapturedGraph(f.task, graph)
	if err != nil {
		t.Fatal(err)
	}
	conditions, mutations, err := idempotency.PreparePublication(marker)
	if err != nil {
		t.Fatal(err)
	}
	f.commit(t, task, captured, workqueue.Change{Conditions: conditions, Mutations: mutations})
	registered, record, err := workqueue.PrepareRegistration(workqueue.ExecutorSnapshot{}, workqueue.ExecutorRecord{
		Identity: workexecution.Identity{
			Executor: workexecution.Controller,
		}, Capacity: 2, Operations: []workexecution.Operation{workexecution.ResourceDefinition}})
	if err != nil {
		t.Fatal(err)
	}
	f.commit(t, registered)
	f.identity = record.Identity
	executor, err := f.queue.ReadExecutor(t.Context(), workexecution.Placement{Executor: workexecution.Controller})
	if err != nil {
		t.Fatal(err)
	}
	available, err := workqueue.PrepareAvailability(executor, f.identity, true, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	f.commit(t, available)
	f.publisher(t)
	return f
}

func (f *nativeBlueprintPublicationFixture) publisher(t *testing.T) {
	t.Helper()
	records, err := blueprintunits.NewRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := blueprintcoordinator.NewPublication(records)
	if err != nil {
		t.Fatal(err)
	}
	f.publications, err = workdispatch.NewPublications(
		f.queue,
		[]workdispatch.PublicationRegistration{{Action: workexecution.ActionApply, Owner: owner}},
	)
	if err != nil {
		t.Fatal(err)
	}
}

func (f *nativeBlueprintPublicationFixture) commit(t *testing.T, changes ...workqueue.Change) {
	t.Helper()
	result, err := f.queue.Commit(t.Context(), changes...)
	if err != nil || !result.Succeeded {
		t.Fatalf("Blueprint transaction: %t %v", result.Succeeded, err)
	}
}

func (f *nativeBlueprintPublicationFixture) checkCandidate(
	t *testing.T,
	item workexecution.Item,
	published, stopped bool,
) {
	t.Helper()
	queued, err := f.store.Get(t.Context(), workqueue.QueueKey(item))
	if err != nil || queued.Entry == nil {
		t.Fatalf("queue membership: %v", err)
	}
	candidate, err := f.queue.ReadCandidate(
		t.Context(),
		workqueue.QueueEntry{TaskID: item.TaskID, WorkItemID: item.ID, Revision: queued.Entry.ModRevision},
		f.identity,
		time.Now().UTC(),
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Stopped != stopped || !published && candidate.Wait == nil ||
		published && !stopped && candidate.Wait != nil {
		t.Fatalf("publication/stop barrier: published=%t stopped=%t candidate=%#v", published, stopped, candidate)
	}
}
