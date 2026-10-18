package app

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	agentruntime "github.com/AlanD20/groundplane/internal/controller/localagent/runtime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: updating an Agent must not cancel host work to manufacture idleness.
// The maintenance owner reads native effect membership, not channel subscribers.
func TestAgentMaintenanceIdleCheckPreservesAssignedWork(t *testing.T) {
	store := newMemoryHierarchyStore()
	store.revision = 1
	agents, err := agentregistration.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	record := agentConfigurationTestRecord()
	if _, err := agents.CreateSingleton(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	queue, err := workqueue.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	maintenance, err := agentruntime.NewWork(queue, agents)
	if err != nil {
		t.Fatal(err)
	}
	if err := maintenance.RequireIdle(t.Context(), record.ID, record.Generation, 1); err != nil {
		t.Fatal(err)
	}
	taskID, _ := seedAgentMaintenanceWork(t, store, queue, agents, record.ID, record.Generation)
	if err := maintenance.RequireIdle(t.Context(), record.ID, record.Generation, 1); !errors.Is(
		err,
		errs.New(errs.KindResourceInUse, ""),
	) {
		t.Fatalf("busy host idle check = %v", err)
	}
	task, _, err := queue.ReadTask(t.Context(), taskID, 0)
	if err != nil || task.Task.Stop != nil {
		t.Fatalf("idle check cancelled work: %#v, %v", task, err)
	}
}

// Rationale: losing the removal caller immediately after durable cancellation
// must leave the exact assignment/effect claim intact, not report settled work.
func TestAgentMaintenanceCancellationRetainsUnsettledEffects(t *testing.T) {
	store := newMemoryHierarchyStore()
	store.revision = 1
	agents, err := agentregistration.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	record := agentConfigurationTestRecord()
	if _, err := agents.CreateSingleton(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	backend := &cancelAfterAgentStopStore{memoryHierarchyStore: store, cancel: cancel}
	queue, err := workqueue.NewRepository(backend)
	if err != nil {
		t.Fatal(err)
	}
	taskID, itemID := seedAgentMaintenanceWork(t, store, queue, agents, record.ID, record.Generation)
	backend.taskKey = workqueue.TaskKey(taskID)
	maintenance, err := agentruntime.NewWork(queue, agents)
	if err != nil {
		t.Fatal(err)
	}
	if err := maintenance.AbortActive(ctx, record.ID, record.Generation, 1, "agent_removed"); !errors.Is(
		err,
		context.Canceled,
	) {
		t.Fatalf("unfinished removal = %v", err)
	}
	view, err := queue.ReadWork(t.Context(), taskID, itemID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if view.Task.Task.Stop == nil || view.Task.Task.Stop.Outcome != workexecution.Cancelled ||
		view.Item.Item.Phase != workexecution.Assigned || view.Item.Item.ActiveAttemptID == "" {
		t.Fatalf("cancellation discarded assignment or stop authority: %#v", view)
	}
	binding, err := agents.MaintenanceBinding(t.Context(), record.ID, record.Generation)
	if err != nil {
		t.Fatal(err)
	}
	idle, err := queue.HostIdle(t.Context(), binding.HostID, binding.ReadRevision)
	if err != nil || idle {
		t.Fatalf("unsettled host falsely idle: %v, %v", idle, err)
	}
	page, err := queue.HostActivity(t.Context(), binding.HostID, "", 1, binding.ReadRevision)
	if err != nil || len(page.Work) != 1 || page.Work[0].Item.Item.ID != itemID {
		t.Fatalf("lost retained host effect: %#v, %v", page, err)
	}
}

type cancelAfterAgentStopStore struct {
	*memoryHierarchyStore
	cancel  context.CancelFunc
	taskKey string
}

func (store *cancelAfterAgentStopStore) Transact(ctx context.Context, conditions []keyvalue.Condition,
	mutations []keyvalue.Mutation,
) (keyvalue.TransactionResult, error) {
	result, err := store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
	if err == nil && result.Succeeded {
		for _, mutation := range mutations {
			if mutation.Key != store.taskKey {
				continue
			}
			task, decodeErr := workqueue.DecodeTask(mutation.Value)
			if decodeErr == nil && task.Stop != nil {
				store.cancel()
			}
		}
	}
	return result, err
}

// This fixture starts at native assignment, not operation publication or host
// execution. The real maintenance/queue owners must preserve that granted work.
func seedAgentMaintenanceWork(t *testing.T, store *memoryHierarchyStore, queue *workqueue.Repository,
	agents *agentregistration.Repository, agentID string, generation uint64,
) (string, string) {
	t.Helper()
	ctx := t.Context()
	binding, err := agents.MaintenanceBinding(ctx, agentID, generation)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	executor := workqueue.ExecutorRecord{Identity: workexecution.Identity{Executor: workexecution.Agent,
		HostID: binding.HostID, AgentID: agentID, Generation: 1}, ProcessGeneration: [16]byte{1},
		Ready: true, Capacity: 1, Operations: []workexecution.Operation{workexecution.Release}}
	item := workexecution.Item{ID: ids.New(ids.KindWorkItem), TaskID: ids.New(ids.KindTask),
		TargetID: ids.New(ids.KindService), Operation: workexecution.Release,
		Placement: workexecution.Placement{Executor: workexecution.Agent, HostID: binding.HostID},
		Input:     &workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("a", 64)},
		Phase:     workexecution.Ready, Cleanup: workexecution.CleanupNone,
		CreatedAt: at, UpdatedAt: at, Deadline: at.Add(time.Minute)}
	item.Claims = []workexecution.Claim{{HostID: binding.HostID, ScopeID: item.TargetID}}
	task := workexecution.Task{ID: item.TaskID, Actor: workexecution.OperatorActor, Action: workexecution.ActionRun,
		TargetID: item.TargetID, Intent: *item.Input, CreatedAt: at, UpdatedAt: at, Deadline: item.Deadline,
		ExpectedItems: 1, PublicationComplete: true, Progress: workexecution.Progress{Ready: 1}}
	taskRaw, err := workqueue.EncodeTask(task)
	if err != nil {
		t.Fatal(err)
	}
	itemRaw, err := workqueue.EncodeItem(item)
	if err != nil {
		t.Fatal(err)
	}
	executorRaw, err := workqueue.EncodeExecutor(executor)
	if err != nil {
		t.Fatal(err)
	}
	ownerRaw, err := recordcodec.Encode("execution_owner", workqueue.ClaimOwner{TaskID: task.ID, WorkItemID: item.ID})
	if err != nil {
		t.Fatal(err)
	}
	seed, err := store.Transact(ctx, nil, []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: workqueue.TaskKey(task.ID), Value: taskRaw},
		{Type: keyvalue.MutationPut, Key: workqueue.ItemKey(item.ID), Value: itemRaw},
		{Type: keyvalue.MutationPut, Key: workqueue.TaskItemKey(task.ID, item.ID), Value: ownerRaw},
		{Type: keyvalue.MutationPut, Key: workqueue.ExecutorKey(executor.Identity), Value: executorRaw},
		{Type: keyvalue.MutationPut, Key: workqueue.QueueKey(item), Value: ownerRaw},
	})
	if err != nil {
		t.Fatal(err)
	}
	change, _, err := workqueue.PrepareAssignment(workqueue.Assignment{
		Task:     workqueue.TaskSnapshot{Task: task, Revision: seed.Revision},
		Item:     workqueue.ItemSnapshot{Item: item, Revision: seed.Revision},
		Executor: workqueue.ExecutorSnapshot{Record: executor, Revision: seed.Revision},
		Claims:   []workqueue.ClaimSnapshot{{Claim: item.Claims[0]}}, QueueRevision: seed.Revision,
		AttemptID: ids.New(
			ids.KindAttempt,
		), Direction: workexecution.Forward, At: at.Add(time.Second), Deadline: item.Deadline,
	})
	if err != nil {
		t.Fatal(err)
	}
	committed, err := queue.Commit(ctx, change)
	if err != nil || !committed.Succeeded {
		t.Fatalf("assign native host work: %v, %v", committed, err)
	}
	return task.ID, item.ID
}
