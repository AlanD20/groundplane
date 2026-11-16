package app

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/acceptedactions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/imageoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/api"
)

// This exercises actual typed acceptance, MVCC predecessor facts and native
// image preparation. Registry effects are simulated; no Docker claim is made.
func TestWholeTaskDependenciesCaptureOnlyAfterEveryParentSettles(t *testing.T) {
	f := newNativeImageFixture(t)
	f.registry.selected.Requested = "nginx:first"
	_, first := f.accept(t, "dependency-first")
	f.registry.selected = imageTestPlan("c", "d")
	f.registry.selected.Requested = "nginx:second"
	_, second := f.accept(t, "dependency-second")
	f.registry.selected = imageTestPlan("e", "f")
	dependent := acceptDependentImage(t, f, "dependency-consumer", []string{first.TaskID, second.TaskID})
	resolutions := f.registry.resolutions
	assertDependencyUncaptured(t, f, dependent, resolutions)
	f.registry.returnedID = first.ConfigDigest
	if _, err := f.execute(t, first.TaskID); err != nil {
		t.Fatal(err)
	}
	assertDependencyUncaptured(t, f, dependent, resolutions)
	f.deferFinish = true
	f.registry.returnedID = second.ConfigDigest
	if _, err := f.execute(t, second.TaskID); err != nil {
		t.Fatal(err)
	}
	// Successful work is insufficient until the whole parent completion settles.
	assertDependencyUncaptured(t, f, dependent, resolutions)
	completions, err := workdispatch.NewCompletions(
		f.queue,
		[]workdispatch.CompletionRegistration{
			{
				Action:    workexecution.ActionFetch,
				Operation: workexecution.Image,
				Owner:     workdispatch.CheckImageCompletion{},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := completions.Finish(t.Context(), second.TaskID, time.Now().UTC(), time.Hour); err != nil {
		t.Fatal(err)
	}
	latest := imageTestPlan("1", "2")
	f.registry.selected = latest
	task := f.prepare(t, dependent)
	if task.ID != dependent || task.PendingPreparation() || task.ExpectedItems != 1 ||
		f.registry.resolutions != resolutions+1 {
		t.Fatalf("dependent was not prepared once under its original Task: %#v", task)
	}
	operations, err := imageoperations.NewRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	captured, err := operations.Load(t.Context(), dependent)
	if err != nil || captured.Record.Input.Fetch == nil || *captured.Record.Input.Fetch != latest {
		t.Fatalf("dependent selected pre-prerequisite image content: %#v %v", captured, err)
	}
	// Promotion retires request ciphertext while preserving immutable kind routing.
	data, err := f.store.Range(
		t.Context(),
		keyvalue.RangeRequest{Prefix: acceptedactions.Prefix(dependent), Limit: 100},
	)
	if err != nil || len(data.Values) != 0 {
		t.Fatalf("promoted request ciphertext survived: %v", err)
	}
	metadata, err := f.store.Get(t.Context(), acceptedactions.KindKey(dependent))
	if err != nil || metadata.Entry == nil {
		t.Fatalf("compact accepted kind missing: %v", err)
	}
	if _, err := f.preparations.CollectAcceptedPreparation(t.Context(), dependent, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	page, err := f.queue.AcceptedPreparationCleanup(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range page.Tasks {
		if entry.Task.ID == dependent {
			t.Fatal("completed draft cleanup stayed indexed")
		}
	}
}

func TestWholeTaskDependencyCompletionReceiptAndMVCCRace(t *testing.T) {
	f := newNativeImageFixture(t)
	_, parent := f.accept(t, "dependency-receipt-parent")
	f.registry.selected = imageTestPlan("c", "d")
	child := acceptDependentImage(t, f, "dependency-receipt-child", []string{parent.TaskID})
	f.registry.returnedID = parent.ConfigDigest
	if _, err := f.execute(t, parent.TaskID); err != nil {
		t.Fatal(err)
	}
	childTask, _, err := f.queue.ReadTask(t.Context(), child, 0)
	if err != nil {
		t.Fatal(err)
	}
	decision, change, err := f.queue.PrepareTaskDependencyProgress(t.Context(), childTask, time.Now().UTC())
	if err != nil || !decision.Ready {
		t.Fatalf("completed prerequisite not observed: %#v %v", decision, err)
	}
	parentTask, _, err := f.queue.ReadTask(t.Context(), parent.TaskID, 0)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := workqueue.EncodeTask(parentTask.Task)
	if err != nil {
		t.Fatal(err)
	}
	// A concurrent parent transition must fence an already-read dependency fact.
	if _, err := f.store.Transact(t.Context(), nil, []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: workqueue.TaskKey(parent.TaskID), Value: raw}}); err != nil {
		t.Fatal(err)
	}
	result, err := f.queue.Commit(t.Context(), change)
	if err != nil || result.Succeeded {
		t.Fatalf("stale dependency decision committed: %#v %v", result, err)
	}
	// Model final history retirement without retaining the parent graph: the
	// durable reverse edge must preserve its tiny completion fact until consumed.
	if _, err := f.store.Transact(t.Context(), nil, []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: workqueue.TaskKey(parent.TaskID)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.queue.CollectTaskCompletionReceipts(t.Context(), ""); err != nil {
		t.Fatal(err)
	}
	task := f.prepare(t, child)
	if task.ID != child || task.Intent == (workexecution.Input{}) {
		t.Fatalf("parent history collection stranded dependent: %#v", task)
	}
	if _, err := f.queue.CollectTaskCompletionReceipts(t.Context(), ""); err != nil {
		t.Fatal(err)
	}
}

func TestTerminalPureTaskCollectionWaitsForTypedDraftCleanup(t *testing.T) {
	f := newNativeImageFixture(t)
	_, parent := f.accept(t, "dependency-cleanup-parent")
	child := acceptDependentImage(t, f, "dependency-cleanup-child", []string{parent.TaskID})
	current, _, err := f.queue.ReadTask(t.Context(), child, 0)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if _, err := f.preparations.Advance(t.Context(), child, current.Task.Deadline.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	current, _, err = f.queue.ReadTask(t.Context(), child, 0)
	if err != nil {
		t.Fatal(err)
	}
	begin, err := workqueue.PrepareCollection(current, current.Task.RetainUntil.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	f.commit(t, begin)
	current, _, err = f.queue.ReadTask(t.Context(), child, 0)
	if err != nil {
		t.Fatal(err)
	}
	collect, err := workqueue.PrepareTaskCollection(current)
	if err != nil {
		t.Fatal(err)
	}
	result, err := f.queue.Commit(t.Context(), collect)
	if err != nil || result.Succeeded {
		t.Fatalf("Task history vanished before draft owner cleanup: %#v %v", result, err)
	}
	if _, err := f.preparations.CollectAcceptedPreparation(t.Context(), child, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	current, _, err = f.queue.ReadTask(t.Context(), child, 0)
	if err != nil {
		t.Fatal(err)
	}
	collect, err = workqueue.PrepareTaskCollection(current)
	if err != nil {
		t.Fatal(err)
	}
	f.commit(t, collect)
	metadata, err := f.store.Get(t.Context(), acceptedactions.KindKey(child))
	if err != nil || metadata.Entry != nil {
		t.Fatalf("compact kind survived final Task collection: %v", err)
	}
}

func TestWholeTaskDependencyStopAndDeadlineRejectWithoutExecution(t *testing.T) {
	for _, scenario := range []string{"failed", "cancelled", "superseded", "deadline"} {
		t.Run(scenario, func(t *testing.T) {
			f := newNativeImageFixture(t)
			_, parent := f.accept(t, "dependency-parent")
			id := acceptDependentImage(t, f, "dependency-stopped-child", []string{parent.TaskID})
			before := f.registry.resolutions
			at := time.Now().UTC()
			if scenario == "deadline" {
				task, _, err := f.queue.ReadTask(t.Context(), id, 0)
				if err != nil {
					t.Fatal(err)
				}
				at = task.Task.Deadline.Add(time.Second)
			} else if scenario == "failed" {
				f.deferFinish = true
				f.registry.returnedID = "wrong-image"
				if _, err := f.execute(t, parent.TaskID); err == nil {
					t.Fatal("parent did not fail")
				}
				at = time.Now().UTC()
			} else {
				task, _, err := f.queue.ReadTask(t.Context(), parent.TaskID, 0)
				if err != nil {
					t.Fatal(err)
				}
				outcome := workexecution.Cancelled
				if scenario == "superseded" {
					outcome = workexecution.Superseded
				}
				stop, err := workqueue.PrepareStop(task, outcome, at)
				if err != nil {
					t.Fatal(err)
				}
				f.commit(t, stop)
			}
			for range 3 {
				if _, err := f.preparations.Advance(t.Context(), id, at); err != nil {
					t.Fatal(err)
				}
			}
			task, _, err := f.queue.ReadTask(t.Context(), id, 0)
			if err != nil || task.Task.FinishedAt == nil || task.Task.Outcome != workexecution.Failed ||
				task.Task.ExpectedItems != 0 ||
				f.registry.resolutions != before {
				t.Fatalf("waiting Task did not terminate without execution: %#v %v", task, err)
			}
			failure, err := f.queue.ReadAcceptedPreparationFailure(t.Context(), id, 0)
			if err != nil || failure == nil || failure.Detail == "" {
				t.Fatalf("terminal preparation cause unavailable: %#v %v", failure, err)
			}
		})
	}
}

func acceptDependentImage(t *testing.T, f *nativeImageFixture, key string, after []string) string {
	t.Helper()
	response, err := f.service.FetchImage(t.Context(), f.registry.selected.Requested, key, after)
	if err != nil {
		t.Fatal(err)
	}
	var accepted api.TaskAccepted
	if err := json.Unmarshal(response.Body, &accepted); err != nil {
		t.Fatal(err)
	}
	return accepted.TaskID
}

func assertDependencyUncaptured(t *testing.T, f *nativeImageFixture, id string, resolutions int) {
	t.Helper()
	for range 2 {
		if _, err := f.preparations.Advance(t.Context(), id, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
	}
	task, _, err := f.queue.ReadTask(t.Context(), id, 0)
	if err != nil || !task.Task.PendingPreparation() || task.Task.Intent != (workexecution.Input{}) ||
		task.Task.ExpectedItems != 0 ||
		f.registry.resolutions != resolutions {
		t.Fatalf("dependent captured execution before all parents finished: %#v %v", task, err)
	}
	page, err := f.queue.ListWork(t.Context(), id, "", 10, 0)
	if err != nil || len(page.Items) != 0 {
		t.Fatalf("waiting dependent published host work: %#v %v", page, err)
	}
}
