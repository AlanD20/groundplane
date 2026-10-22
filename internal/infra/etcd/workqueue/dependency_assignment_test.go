package workqueue

import (
	"slices"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// TASK-99: prerequisite success must be consumed atomically with assignment,
// not inferred from an old Agent step list. Refused consumers occupy no slot,
// and unrelated work remains assignable. This is an atomic-CAS model, not etcd
// durability, physical Zone/Volume preparation or complete Blueprint execution.
func TestAssignmentRequiresExactSuccessfulDependency(t *testing.T) {
	for _, scenario := range []string{"pending", "failed", "cancelled", "superseded", "wrong Task", "missing", "future dependency", "stale Task snapshot", "success", "changed after admission"} {
		t.Run(scenario, func(t *testing.T) {
			store := &assignmentStore{revision: 1, values: make(map[string]keyvalue.KeyValue)}
			request := assignmentFixture(t, store, workexecution.Release, nil)
			dependency := workexecution.Clone(request.Item.Item)
			dependency.ID, dependency.TargetID = ids.New(ids.KindWorkItem), ids.New(ids.KindNetwork)
			dependency.Operation, dependency.Dependencies = workexecution.Network, nil
			dependency.Claims = []workexecution.Claim{
				{HostID: dependency.Placement.HostID, ScopeID: dependency.TargetID},
			}
			request.Item.Item.Dependencies = []string{dependency.ID}
			request.Task.Task.ExpectedItems, request.Task.Task.Progress.Ready = 2, 2
			if scenario != "pending" {
				outcome := workexecution.Succeeded
				switch scenario {
				case "failed":
					outcome = workexecution.Failed
				case "cancelled":
					outcome = workexecution.Cancelled
				case "superseded":
					outcome = workexecution.Superseded
				}
				settled, err := workexecution.SettleUnstarted(dependency, workexecution.Completion{
					Outcome: outcome, Evidence: *dependency.Input, EffectsSettled: true}, request.At)
				if err != nil {
					t.Fatal(err)
				}
				request.Task.Task, err = workexecution.CountTransition(
					request.Task.Task,
					dependency,
					settled,
					request.At,
				)
				if err != nil {
					t.Fatal(err)
				}
				dependency = settled
			}
			if scenario == "wrong Task" {
				dependency.TaskID = ids.New(ids.KindTask)
			}
			request.Dependencies = []ItemSnapshot{{Item: dependency, Revision: 1}}
			if scenario == "future dependency" {
				request.Dependencies[0].Revision = 2
			}
			if scenario == "stale Task snapshot" {
				request.ReadRevision = 0
			}
			if scenario == "missing" {
				request.Dependencies = nil
			}
			itemRaw, err := EncodeItem(request.Item.Item)
			if err != nil {
				t.Fatal(err)
			}
			taskRaw, err := EncodeTask(request.Task.Task)
			if err != nil {
				t.Fatal(err)
			}
			dependencyRaw, err := EncodeItem(dependency)
			if err != nil {
				t.Fatal(err)
			}
			for key, raw := range map[string][]byte{TaskKey(request.Task.Task.ID): taskRaw,
				ItemKey(request.Item.Item.ID): itemRaw, ItemKey(dependency.ID): dependencyRaw} {
				store.values[key] = keyvalue.KeyValue{Key: key, Value: raw, ModRevision: 1}
			}
			repository, err := NewRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			change, attempt, err := PrepareAssignment(request)
			if scenario != "success" && scenario != "changed after admission" {
				if err == nil {
					t.Fatal("an unsatisfied or foreign prerequisite granted consumer execution")
				}
				independent := assignmentFixture(t, store, workexecution.Release, &request.Executor)
				change, _, err = PrepareAssignment(independent)
				if err != nil {
					t.Fatal(err)
				}
				committed, err := repository.Commit(t.Context(), change)
				if err != nil || !committed.Succeeded {
					t.Fatalf("blocked prerequisite consumed unrelated capacity: %v", err)
				}
				if _, found := store.values[AttemptKey(request.AttemptID)]; found {
					t.Fatal("refused consumer retained an execution attempt")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "changed after admission" {
				// Dependency transitions update their Task in the same transaction.
				// Even an identical replacement of that snapshot fences admission.
				competing, err := store.Transact(t.Context(), nil, []keyvalue.Mutation{{
					Type: keyvalue.MutationPut, Key: ItemKey(dependency.ID), Value: dependencyRaw}, {
					Type: keyvalue.MutationPut, Key: TaskKey(request.Task.Task.ID), Value: taskRaw}})
				if err != nil || !competing.Succeeded {
					t.Fatalf("competing dependency write: %v", err)
				}
			}
			committed, err := repository.Commit(t.Context(), change)
			want := scenario == "success"
			if err != nil || committed.Succeeded != want {
				t.Fatalf("consumer assignment ignored captured dependency: %v/%v", committed, err)
			}
			_, hasAttempt := store.values[AttemptKey(attempt.ID)]
			_, hasSlot := store.values[SlotKey(request.Executor.Record.Identity, request.Slot)]
			_, queued := store.values[QueueKey(request.Item.Item)]
			item, err := DecodeItem(store.values[ItemKey(request.Item.Item.ID)].Value)
			if err != nil || hasAttempt != want || hasSlot != want || queued == want ||
				(item.Phase == workexecution.Assigned) != want {
				t.Fatal("dependency admission left a partial assignment or consumed refused capacity")
			}
		})
	}
}

// TASK-198: a shared router can depend on many configuration/binding results
// while atomically claiming every affected Service. Fan-in must not consume the
// transaction budget needed for those claims and operation-owned source guards.
func TestAssignmentWithLargeFanInKeepsClaimsAtomic(t *testing.T) {
	store := &assignmentStore{revision: 1, values: make(map[string]keyvalue.KeyValue)}
	request := assignmentFixture(t, store, workexecution.Release, nil)
	request.Task.Task.ExpectedItems, request.Task.Task.Progress.Ready = 47, 47
	for range 46 {
		dependency := workexecution.Clone(request.Item.Item)
		dependency.ID = ids.New(ids.KindWorkItem)
		settled, err := workexecution.SettleUnstarted(dependency, workexecution.Completion{
			Outcome: workexecution.Succeeded, Evidence: *dependency.Input, EffectsSettled: true}, request.At)
		if err != nil {
			t.Fatal(err)
		}
		request.Task.Task, err = workexecution.CountTransition(request.Task.Task, dependency, settled, request.At)
		if err != nil {
			t.Fatal(err)
		}
		request.Dependencies = append(request.Dependencies, ItemSnapshot{Item: settled, Revision: 1})
	}
	slices.SortFunc(request.Dependencies, func(a, b ItemSnapshot) int {
		return strings.Compare(a.Item.ID, b.Item.ID)
	})
	for _, dependency := range request.Dependencies {
		request.Item.Item.Dependencies = append(request.Item.Item.Dependencies, dependency.Item.ID)
	}
	for len(request.Item.Item.Claims) < 24 {
		request.Item.Item.Claims = append(
			request.Item.Item.Claims,
			workexecution.Claim{ScopeID: ids.New(ids.KindService)},
		)
	}
	request.Claims = nil
	for _, claim := range request.Item.Item.Claims {
		request.Claims = append(request.Claims, ClaimSnapshot{Claim: claim})
	}
	for _, snapshot := range append(slices.Clone(request.Dependencies), request.Item) {
		raw, err := EncodeItem(snapshot.Item)
		if err != nil {
			t.Fatal(err)
		}
		store.values[ItemKey(snapshot.Item.ID)] = keyvalue.KeyValue{Value: raw, ModRevision: 1}
	}
	raw, err := EncodeTask(request.Task.Task)
	if err != nil {
		t.Fatal(err)
	}
	store.values[TaskKey(request.Task.Task.ID)] = keyvalue.KeyValue{Value: raw, ModRevision: 1}
	change, attempt, err := PrepareAssignment(request)
	if err != nil {
		t.Fatal(err)
	}
	sources := Change{}
	for range 20 {
		sources.Conditions = append(sources.Conditions, keyvalue.Condition{Key: "/source/" + ids.New(ids.KindService)})
	}
	repository, err := NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	result, err := repository.Commit(t.Context(), change, sources)
	if err != nil || !result.Succeeded {
		t.Fatalf("large fan-in prevented assignment: %v/%v", result, err)
	}
	if _, found := store.values[AttemptKey(attempt.ID)]; !found {
		t.Fatal("assignment did not publish its attempt")
	}
	for _, claim := range request.Claims {
		if _, found := store.values[ClaimKey(claim.Claim)]; !found {
			t.Fatal("assignment omitted a required resource claim")
		}
	}
}
