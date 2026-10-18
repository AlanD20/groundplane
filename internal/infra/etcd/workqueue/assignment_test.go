package workqueue

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// TASK-23: losing Backup staging after scheduling must invalidate its stale
// assignment transaction without disabling unrelated work or consuming a slot.
func TestUnavailableOperationFencesStaleAssignmentAndLeavesCapacity(t *testing.T) {
	ctx := context.Background()
	store := &assignmentStore{revision: 1, values: make(map[string]keyvalue.KeyValue)}
	backup := assignmentFixture(t, store, workexecution.Backup, nil)
	repository, err := NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	stale, attempt, err := PrepareAssignment(backup)
	if err != nil {
		t.Fatal(err)
	}
	availability, err := PrepareAvailability(
		backup.Executor,
		backup.Executor.Record.Identity,
		true,
		3,
		[]workexecution.Operation{workexecution.Backup},
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := repository.Commit(ctx, availability)
	if err != nil || !result.Succeeded {
		t.Fatalf("disable Backup staging: %v/%v", result, err)
	}
	result, err = repository.Commit(ctx, stale)
	if err != nil || result.Succeeded {
		t.Fatalf("stale assignment was not refused atomically: %v/%v", result, err)
	}
	if _, exists := store.values[AttemptKey(attempt.ID)]; exists {
		t.Fatal("failed admission left an executable attempt")
	}
	current, err := DecodeExecutor(store.values[ExecutorKey(backup.Executor.Record.Identity)].Value)
	if err != nil {
		t.Fatal(err)
	}
	backup.Executor = ExecutorSnapshot{Record: current, Revision: 2}
	if _, _, err := PrepareAssignment(backup); err == nil {
		t.Fatal("fresh Backup assignment ignored unavailable staging")
	}
	independent := assignmentFixture(t, store, workexecution.Release, &backup.Executor)
	change, _, err := PrepareAssignment(independent)
	if err != nil {
		t.Fatal(err)
	}
	result, err = repository.Commit(ctx, change)
	if err != nil || !result.Succeeded {
		t.Fatalf("unrelated Release lost capacity: %v/%v", result, err)
	}
	assigned, err := DecodeItem(store.values[ItemKey(independent.Item.Item.ID)].Value)
	if err != nil || assigned.Phase != workexecution.Assigned {
		t.Fatalf("unrelated work was not assigned: %v/%v", assigned.Phase, err)
	}
	untouched, err := DecodeTask(store.values[TaskKey(backup.Task.Task.ID)].Value)
	if err != nil || untouched.Progress.Ready != 1 || untouched.Progress.Assigned != 0 {
		t.Fatalf("refused Backup admission changed Task progress: %v/%v", untouched.Progress, err)
	}
}

// TASK-24: workers with different free slots must still serialize a shared
// resource claim; the losing transaction cannot publish even part of an attempt.
func TestConcurrentResourceClaimsAdmitOneAssignment(t *testing.T) {
	store := &assignmentStore{revision: 1, values: make(map[string]keyvalue.KeyValue)}
	first := assignmentFixture(t, store, workexecution.Release, nil)
	second := assignmentFixture(t, store, workexecution.Release, &first.Executor)
	second.Item.Item.Claims = append([]workexecution.Claim(nil), first.Item.Item.Claims...)
	second.Claims = []ClaimSnapshot{{Claim: second.Item.Item.Claims[0]}}
	second.Slot = 1
	raw, err := EncodeItem(second.Item.Item)
	if err != nil {
		t.Fatal(err)
	}
	store.values[ItemKey(second.Item.Item.ID)] = keyvalue.KeyValue{
		Key:         ItemKey(second.Item.Item.ID),
		Value:       raw,
		ModRevision: 1,
	}
	repository, err := NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	changes := make([]Change, 2)
	for index, request := range []Assignment{first, second} {
		changes[index], _, err = PrepareAssignment(request)
		if err != nil {
			t.Fatal(err)
		}
	}
	results := make(chan keyvalue.TransactionResult, 2)
	failures := make(chan error, 2)
	var workers sync.WaitGroup
	for _, change := range changes {
		workers.Go(func() {
			result, err := repository.Commit(context.Background(), change)
			results <- result
			failures <- err
		})
	}
	workers.Wait()
	close(results)
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	admitted := 0
	for result := range results {
		if result.Succeeded {
			admitted++
		}
	}
	if admitted != 1 {
		t.Fatalf("admitted %d conflicting assignments", admitted)
	}
	for _, request := range []Assignment{first, second} {
		item, err := DecodeItem(store.values[ItemKey(request.Item.Item.ID)].Value)
		if err != nil {
			t.Fatal(err)
		}
		_, attemptExists := store.values[AttemptKey(request.AttemptID)]
		_, slotExists := store.values[SlotKey(request.Executor.Record.Identity, request.Slot)]
		_, queueExists := store.values[QueueKey(request.Item.Item)]
		if item.Phase == workexecution.Assigned {
			if !attemptExists || !slotExists || queueExists {
				t.Fatal("winning assignment was only partially published")
			}
		} else if item.Phase != workexecution.Ready || attemptExists || slotExists || !queueExists {
			t.Fatal("losing assignment changed its work, attempt, slot or queue entry")
		}
	}
}

func assignmentFixture(
	t *testing.T,
	store *assignmentStore,
	operation workexecution.Operation,
	executor *ExecutorSnapshot,
) Assignment {
	t.Helper()
	at := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	if executor == nil {
		executor = &ExecutorSnapshot{Revision: 1, Record: ExecutorRecord{
			Identity: workexecution.Identity{
				Executor:   workexecution.Agent,
				HostID:     ids.New(ids.KindHost),
				AgentID:    ids.New(ids.KindAgent),
				Generation: 1,
			},
			ProcessGeneration: [16]byte{1}, Ready: true, Capacity: 3,
			Operations: []workexecution.Operation{workexecution.Backup, workexecution.Release},
		}}
		raw, err := EncodeExecutor(executor.Record)
		if err != nil {
			t.Fatal(err)
		}
		store.values[ExecutorKey(executor.Record.Identity)] = keyvalue.KeyValue{Value: raw, ModRevision: 1}
	}
	item := workexecution.Item{
		ID: ids.New(
			ids.KindWorkItem,
		), TaskID: ids.New(ids.KindTask), TargetID: ids.New(ids.KindService), Operation: operation,
		Placement: workexecution.Placement{Executor: workexecution.Agent, HostID: executor.Record.Identity.HostID},
		Input:     &workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("a", 64)},
		Phase:     workexecution.Ready, Cleanup: workexecution.CleanupNone, CreatedAt: at, UpdatedAt: at, Deadline: at.Add(time.Minute),
	}
	item.Claims = []workexecution.Claim{{HostID: item.Placement.HostID, ScopeID: item.TargetID}}
	task := workexecution.Task{
		ID: item.TaskID, Actor: workexecution.OperatorActor, Action: workexecution.ActionRun, TargetID: item.TargetID,
		Intent: *item.Input, CreatedAt: at, UpdatedAt: at, Deadline: item.Deadline,
		ExpectedItems: 1, PublicationComplete: true, Progress: workexecution.Progress{Ready: 1},
	}
	itemBytes, err := EncodeItem(item)
	if err != nil {
		t.Fatal(err)
	}
	taskBytes, err := EncodeTask(task)
	if err != nil {
		t.Fatal(err)
	}
	for key, raw := range map[string][]byte{TaskKey(task.ID): taskBytes, ItemKey(item.ID): itemBytes, QueueKey(item): []byte("queued")} {
		store.values[key] = keyvalue.KeyValue{Key: key, Value: raw, ModRevision: 1}
	}
	return Assignment{
		Task: TaskSnapshot{Task: task, Revision: 1}, Item: ItemSnapshot{Item: item, Revision: 1}, Executor: *executor,
		Claims: []ClaimSnapshot{{Claim: item.Claims[0]}}, QueueRevision: 1, Direction: workexecution.Forward,
		AttemptID: ids.New(ids.KindAttempt), At: at.Add(time.Second), Deadline: item.Deadline,
	}
}

// This store models atomic compare-and-swap only. These tests do not claim
// actual etcd durability, MVCC reads or network-failure qualification.
type assignmentStore struct {
	mu       sync.Mutex
	revision int64
	values   map[string]keyvalue.KeyValue
}

func (*assignmentStore) GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error) {
	return nil, errors.New("unexpected read in assignment transaction test")
}

func (*assignmentStore) Range(context.Context, keyvalue.RangeRequest) (*keyvalue.RangeResult, error) {
	return nil, errors.New("unexpected range in assignment transaction test")
}

func (*assignmentStore) MeasureTransaction(
	_ context.Context,
	conditions []keyvalue.Condition,
	mutations []keyvalue.Mutation,
) (keyvalue.TransactionBudget, error) {
	return keyvalue.TransactionBudget{Operations: len(conditions) + len(mutations)}, nil
}

func (store *assignmentStore) Transact(
	_ context.Context,
	conditions []keyvalue.Condition,
	mutations []keyvalue.Mutation,
) (keyvalue.TransactionResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, condition := range conditions {
		if condition.Prefix {
			for key := range store.values {
				if strings.HasPrefix(key, condition.Key) {
					return keyvalue.TransactionResult{Revision: store.revision}, nil
				}
			}
		} else if store.values[condition.Key].ModRevision != condition.ModRevision {
			return keyvalue.TransactionResult{Revision: store.revision}, nil
		}
	}
	store.revision++
	for _, mutation := range mutations {
		if mutation.Type == keyvalue.MutationDelete {
			delete(store.values, mutation.Key)
		} else {
			store.values[mutation.Key] = keyvalue.KeyValue{Key: mutation.Key, Value: append([]byte(nil), mutation.Value...), ModRevision: store.revision}
		}
	}
	return keyvalue.TransactionResult{Succeeded: true, Revision: store.revision}, nil
}
