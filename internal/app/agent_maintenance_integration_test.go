package app

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	buildversion "github.com/AlanD20/groundplane/internal/common/version"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/agentchannel"
	agentruntime "github.com/AlanD20/groundplane/internal/controller/localagent/runtime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestPreparedAgentMaintenanceUsesExecutorGenerationNotCredentialGeneration(t *testing.T) {
	store := newMemoryHierarchyStore()
	store.revision = 1
	agents, err := agentregistration.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	record := agentConfigurationTestRecord()
	record.Generation = 2
	created, err := agents.CreateSingleton(t.Context(), record)
	if err != nil {
		t.Fatal(err)
	}
	ready, err := agents.MarkReady(t.Context(), record.ID, record.Generation, created.Revision,
		record.CreatedAt.Add(time.Second), buildversion.BuildIdentity{Version: "test"})
	if err != nil {
		t.Fatal(err)
	}
	binding, err := agents.MaintenanceBinding(t.Context(), record.ID, record.Generation)
	if err != nil {
		t.Fatal(err)
	}
	executor := workqueue.ExecutorRecord{Identity: workexecution.Identity{Executor: workexecution.Agent,
		HostID: binding.HostID, AgentID: record.ID, Generation: 6}, ProcessGeneration: [16]byte{6},
		Ready: true, Capacity: 1, MaxConcurrentTasks: 1, MaxParallelOperationsPerTask: 1,
		Operations: []workexecution.Operation{workexecution.EntryConfiguration}}
	raw, err := workqueue.EncodeExecutor(executor)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Transact(t.Context(), nil, []keyvalue.Mutation{{
		Type: keyvalue.MutationPut, Key: workqueue.ExecutorKey(executor.Identity), Value: raw,
	}}); err != nil {
		t.Fatal(err)
	}
	queue, err := workqueue.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	sessions := agentchannel.NewRegistry()
	maintenance, err := agentruntime.NewWork(queue, agents, sessions)
	if err != nil {
		t.Fatal(err)
	}
	resume, err := sessions.PauseAssignments(t.Context(), record.ID, record.Generation)
	if err != nil {
		t.Fatal(err)
	}
	taskID := ids.NewAt(ids.KindTask, record.CreatedAt, 100)
	if err := maintenance.RequirePreparedReplacementReady(
		t.Context(), record.ID, record.Generation, 1, taskID,
	); err != nil {
		t.Fatalf("credential 2 / executor 6 qualification: %v", err)
	}
	seedRetainedEntryAttempt(t, store, binding.HostID, record.ID, 6)
	if err := maintenance.RequireUpgradeReady(t.Context(), record.ID, record.Generation, 1); !errors.Is(
		err,
		errs.New(errs.KindRequestUnavailable, ""),
	) {
		t.Fatalf("executor 6 retained work did not reach the missing-session nonce proof: %v", err)
	}
	resume()

	token := sha256.Sum256([]byte("candidate-token"))
	candidate, err := agents.ReplaceGeneration(t.Context(), ready,
		"ghcr.io/groundplane/agent@sha256:"+strings.Repeat("b", 64), []byte("candidate-encrypted-token"),
		base64.RawURLEncoding.EncodeToString(token[:]), nil, record.CreatedAt.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	all, err := sessions.PauseAssignments(t.Context(), record.ID, ^uint64(0))
	if err != nil {
		t.Fatal(err)
	}
	defer all()
	if err := maintenance.RequireReplacementRollbackReady(t.Context(), taskID, record.ID,
		record.Generation, candidate.Record.Generation, 1); err != nil {
		t.Fatalf("offline rollback rejected saved executor 6 boundary: %v", err)
	}
	seedRetainedEntryAttempt(t, store, binding.HostID, record.ID, 7)
	if err := maintenance.RequireReplacementRollbackReady(t.Context(), taskID, record.ID,
		record.Generation, candidate.Record.Generation, 1); !errors.Is(
		err,
		errs.New(errs.KindResourceInUse, ""),
	) {
		t.Fatalf("candidate executor work crossed saved generation 6 boundary: %v", err)
	}
}

func seedRetainedEntryAttempt(
	t *testing.T,
	store *memoryHierarchyStore,
	hostID, agentID string,
	generation uint64,
) {
	t.Helper()
	at := time.Date(2026, 11, 22, 13, 0, 0, 0, time.UTC)
	seed := int64(generation * 10)
	input := workexecution.Input{ID: ids.NewAt(ids.KindPlan, at, seed+1), SHA256: strings.Repeat("c", 64)}
	evidence := workexecution.Input{ID: ids.NewAt(ids.KindPlan, at, seed+2), SHA256: strings.Repeat("d", 64)}
	taskID := ids.NewAt(ids.KindTask, at, seed+3)
	itemID := ids.NewAt(ids.KindWorkItem, at, seed+4)
	attemptID := ids.NewAt(ids.KindAttempt, at, seed+5)
	started, finished := at.Add(time.Second), at.Add(2*time.Second)
	task := workexecution.Task{ID: taskID, Actor: workexecution.OperatorActor, Action: workexecution.ActionUpdate,
		TargetID: ids.NewAt(ids.KindEnvEntry, at, seed+6), TargetName: "retained Entry", Intent: input,
		CreatedAt: at, UpdatedAt: finished, Deadline: at.Add(time.Hour), ExpectedItems: 1,
		PublicationComplete: true, Progress: workexecution.Progress{Attention: 1}}
	item := workexecution.Item{ID: itemID, TaskID: taskID, Operation: workexecution.EntryConfiguration,
		TargetID: task.TargetID, Placement: workexecution.Placement{Executor: workexecution.Agent, HostID: hostID},
		Claims: []workexecution.Claim{{HostID: hostID, ScopeID: task.TargetID}}, Input: &input,
		Phase: workexecution.Attention, AttemptNumber: 1, Outcome: workexecution.Failed,
		Result: &evidence, Cleanup: workexecution.CleanupRequired,
		CreatedAt: at, UpdatedAt: finished, Deadline: task.Deadline}
	attempt := workexecution.Attempt{ID: attemptID, WorkItemID: itemID, TaskID: taskID, Number: 1,
		Executor: workexecution.Identity{Executor: workexecution.Agent, HostID: hostID,
			AgentID: agentID, Generation: generation},
		Direction: workexecution.Forward, Input: input, AssignedAt: at, Deadline: task.Deadline,
		StartedAt: &started, FinishedAt: &finished, Outcome: workexecution.Failed, Result: &evidence}
	taskRaw, err := workqueue.EncodeTask(task)
	if err != nil {
		t.Fatal(err)
	}
	itemRaw, err := workqueue.EncodeItem(item)
	if err != nil {
		t.Fatal(err)
	}
	attemptRaw, err := workqueue.EncodeAttempt(attempt)
	if err != nil {
		t.Fatal(err)
	}
	owner := workqueue.ClaimOwner{TaskID: taskID, WorkItemID: itemID}
	ownerRaw, err := recordcodec.Encode("execution_owner", owner)
	if err != nil {
		t.Fatal(err)
	}
	hostRaw, err := recordcodec.Encode("execution_host_effect", owner)
	if err != nil {
		t.Fatal(err)
	}
	indexRaw, err := recordcodec.Encode("execution_slot", workqueue.Slot{
		TaskID: taskID, WorkItemID: itemID, AttemptID: attemptID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Transact(t.Context(), nil, []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: workqueue.TaskKey(taskID), Value: taskRaw},
		{Type: keyvalue.MutationPut, Key: workqueue.ItemKey(itemID), Value: itemRaw},
		{Type: keyvalue.MutationPut, Key: workqueue.TaskItemKey(taskID, itemID), Value: ownerRaw},
		{Type: keyvalue.MutationPut, Key: workqueue.AttemptKey(attemptID), Value: attemptRaw},
		{Type: keyvalue.MutationPut, Key: workqueue.AttemptIndexKey(itemID, 1), Value: indexRaw},
		{Type: keyvalue.MutationPut, Key: workqueue.HostEffectKey(hostID, itemID), Value: hostRaw},
	}); err != nil {
		t.Fatal(err)
	}
}

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
	maintenance, err := agentruntime.NewWork(queue, agents, agentchannel.NewRegistry())
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
	maintenance, err := agentruntime.NewWork(queue, agents, agentchannel.NewRegistry())
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
		Ready: true, Capacity: 1, MaxConcurrentTasks: 1, MaxParallelOperationsPerTask: 1, Operations: []workexecution.Operation{workexecution.Release}}
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
