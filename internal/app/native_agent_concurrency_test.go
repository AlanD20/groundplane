package app

import (
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// Exercise actual MVCC admission and assignment, including two candidates
// captured before either commits. This does not simulate host execution.
func TestNativeAgentBoundsTasksAndParallelOperations(t *testing.T) {
	ctx, at := t.Context(), time.Now().UTC()
	store := newMemoryHierarchyStore()
	queue, err := workqueue.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	commit := func(change workqueue.Change) bool {
		t.Helper()
		result, err := queue.Commit(ctx, change)
		if err != nil {
			t.Fatal(err)
		}
		return result.Succeeded
	}
	registration, executor, err := workqueue.PrepareRegistration(workqueue.ExecutorSnapshot{}, workqueue.ExecutorRecord{
		Identity:          workexecution.Identity{Executor: workexecution.Agent, HostID: ids.New(ids.KindHost), AgentID: ids.New(ids.KindAgent)},
		ProcessGeneration: [16]byte{1}, Capacity: 4, MaxConcurrentTasks: 2, MaxParallelOperationsPerTask: 2,
		Operations: []workexecution.Operation{workexecution.Release},
	})
	if err != nil || !commit(registration) {
		t.Fatalf("register: %v", err)
	}
	snapshot, err := queue.ReadExecutor(ctx, workexecution.Placement{Executor: workexecution.Agent, HostID: executor.Identity.HostID})
	if err != nil {
		t.Fatal(err)
	}
	ready, err := workqueue.PrepareAvailability(snapshot, executor.Identity, true, 4, nil)
	if err != nil || !commit(ready) {
		t.Fatalf("ready: %v", err)
	}
	var tasks [3][]workexecution.Item
	for taskIndex := range tasks {
		task := workexecution.Task{ID: ids.New(ids.KindTask), Actor: workexecution.OperatorActor,
			Action: workexecution.ActionDeploy, TargetID: ids.New(ids.KindService),
			Intent:    workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("a", 64)},
			CreatedAt: at, UpdatedAt: at, Deadline: at.Add(time.Minute), ExpectedItems: 3,
			PublicationComplete: true, Progress: workexecution.Progress{Ready: 3}}
		raw, err := workqueue.EncodeTask(task)
		if err != nil {
			t.Fatal(err)
		}
		change := workqueue.Change{Conditions: []keyvalue.Condition{{Key: workqueue.TaskKey(task.ID)}},
			Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: workqueue.TaskKey(task.ID), Value: raw}}}
		for range 3 {
			item := workexecution.Item{ID: ids.New(ids.KindWorkItem), TaskID: task.ID,
				TargetID: ids.New(ids.KindService), Operation: workexecution.Release,
				Placement: workexecution.Placement{Executor: workexecution.Agent, HostID: executor.Identity.HostID},
				Input:     &task.Intent, Phase: workexecution.Ready, Cleanup: workexecution.CleanupNone,
				CreatedAt: at, UpdatedAt: at, Deadline: task.Deadline}
			item.Claims = []workexecution.Claim{{HostID: executor.Identity.HostID, ScopeID: item.TargetID}}
			encoded, err := workqueue.EncodeItem(item)
			if err != nil {
				t.Fatal(err)
			}
			owner, err := recordcodec.Encode("execution_owner", workqueue.ClaimOwner{TaskID: task.ID, WorkItemID: item.ID})
			if err != nil {
				t.Fatal(err)
			}
			change.Mutations = append(change.Mutations,
				keyvalue.Mutation{Type: keyvalue.MutationPut, Key: workqueue.ItemKey(item.ID), Value: encoded},
				keyvalue.Mutation{Type: keyvalue.MutationPut, Key: workqueue.TaskItemKey(task.ID, item.ID), Value: owner},
				keyvalue.Mutation{Type: keyvalue.MutationPut, Key: workqueue.QueueKey(item), Value: owner})
			tasks[taskIndex] = append(tasks[taskIndex], item)
		}
		if !commit(change) {
			t.Fatal("publish fixture")
		}
	}
	read := func(item workexecution.Item) workqueue.Candidate {
		t.Helper()
		value, err := store.Get(ctx, workqueue.QueueKey(item))
		if err != nil || value.Entry == nil {
			t.Fatalf("queued work: %v", err)
		}
		candidate, err := queue.ReadCandidate(ctx, workqueue.QueueEntry{TaskID: item.TaskID, WorkItemID: item.ID,
			Revision: value.Entry.ModRevision}, executor.Identity, at.Add(time.Second), nil)
		if err != nil {
			t.Fatal(err)
		}
		return candidate
	}
	prepare := func(candidate workqueue.Candidate) workqueue.Change {
		t.Helper()
		if candidate.Wait != nil {
			t.Fatalf("unexpected wait: %v", candidate.Wait)
		}
		request := candidate.Assignment
		request.AttemptID, request.At, request.Deadline = ids.New(ids.KindAttempt), at.Add(time.Second), at.Add(time.Minute)
		change, _, err := workqueue.PrepareAssignment(request)
		if err != nil {
			t.Fatal(err)
		}
		return change
	}
	first, stale := read(tasks[0][0]), read(tasks[1][0])
	// Use distinct free slots: the admission fence, rather than a slot collision,
	// must reject the second stale count snapshot.
	stale.Assignment.Slot = 1
	if !commit(prepare(first)) || commit(prepare(stale)) {
		t.Fatal("stale concurrent admission was accepted")
	}
	if !commit(prepare(read(tasks[1][0]))) || !commit(prepare(read(tasks[0][1]))) {
		t.Fatal("independent Tasks did not use available capacity")
	}
	for _, item := range []workexecution.Item{tasks[0][2], tasks[2][0]} {
		candidate := read(item)
		if candidate.Wait == nil || candidate.Wait.Reason != workexecution.WaitCapacity {
			t.Fatal("Task or per-Task limit exceeded")
		}
	}
	if !commit(prepare(read(tasks[1][1]))) {
		t.Fatal("one full Task blocked another Task's remaining operation")
	}
}
