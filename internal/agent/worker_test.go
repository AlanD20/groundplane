package agent

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	workerTestTaskID  = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	workerOtherTaskID = "task_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	workerTestStepID  = "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

// Rationale: replaying one live assignment must neither execute twice nor
// replace the cancellation authority reserved by the first delivery.
func TestWorkerPoolDeduplicatesMatchingLiveAssignmentAndRejectsHashMismatch(t *testing.T) {
	t.Parallel()
	pool := NewWorkerPool(2, nil, testLogger())
	started := make(chan struct{}, 2)
	pool.executeStep = func(ctx context.Context, _ adapters.Step) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		pool.Run(ctx)
		close(done)
	}()

	assignment := workerAssignment(workerTestTaskID, "plan-a")
	if err := pool.Submit(ctx, assignment); err != nil {
		t.Fatalf("Submit(first) error = %v", err)
	}
	if err := pool.Submit(ctx, assignment); err != nil {
		t.Fatalf("Submit(replay) error = %v", err)
	}
	mismatch := workerAssignment(workerTestTaskID, "plan-b")
	if err := pool.Submit(ctx, mismatch); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("Submit(mismatch) error = %v, want internal", err)
	}
	if capacity := pool.Capacity(); capacity != 1 {
		t.Fatalf("Capacity() = %d, want 1 reservation remaining", capacity)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first assignment did not start")
	}
	select {
	case <-started:
		t.Fatal("matching replay executed concurrently")
	default:
	}
	if err := pool.Abort(ctx, workerTestTaskID); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
	result := nextWorkerResult(t, pool)
	if result.TaskID != workerTestTaskID || result.PlanHash != assignment.PlanHash || result.Terminal != TaskTerminalAborted {
		t.Fatalf("result = %#v", result)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run() did not join workers")
	}
}

// Rationale: TaskAbort can overtake worker dequeue, so cancellation ownership
// must exist while queued and must suppress every step side effect.
func TestWorkerPoolRetainsQueuedAbortAndReservationCapacity(t *testing.T) {
	t.Parallel()
	pool := NewWorkerPool(1, nil, testLogger())
	executed := make(chan struct{}, 1)
	pool.executeStep = func(context.Context, adapters.Step) error {
		executed <- struct{}{}
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	assignment := workerAssignment(workerTestTaskID, "plan-a")
	if err := pool.Submit(ctx, assignment); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if capacity := pool.Capacity(); capacity != 0 {
		t.Fatalf("Capacity() = %d, want queued reservation to consume the slot", capacity)
	}
	if err := pool.Abort(ctx, workerTestTaskID); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
	done := make(chan struct{})
	go func() {
		pool.Run(ctx)
		close(done)
	}()
	result := nextWorkerResult(t, pool)
	if result.Terminal != TaskTerminalAborted {
		t.Fatalf("terminal = %v, want aborted", result.Terminal)
	}
	select {
	case <-executed:
		t.Fatal("queued aborted assignment executed a step")
	default:
	}
	cancel()
	<-done
}

// Rationale: the gRPC receive goroutine is the only task/control reader; a
// full pool must reject promptly instead of blocking TaskAbort or Shutdown.
func TestWorkerPoolSubmitReturnsConflictWithoutBlockingWhenFull(t *testing.T) {
	t.Parallel()
	pool := NewWorkerPool(1, nil, testLogger())
	ctx := context.Background()
	if err := pool.Submit(ctx, workerAssignment(workerTestTaskID, "plan-a")); err != nil {
		t.Fatalf("Submit(first) error = %v", err)
	}
	returned := make(chan error, 1)
	go func() { returned <- pool.Submit(ctx, workerAssignment(workerOtherTaskID, "plan-b")) }()
	select {
	case err := <-returned:
		if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
			t.Fatalf("Submit(full) error = %v, want state conflict", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Submit(full) blocked the control loop")
	}
}

// Rationale: Run returning is the shutdown join boundary; cancellation must
// reach active task contexts and wait for their executor cleanup.
func TestWorkerPoolCancellationJoinsActiveWorkers(t *testing.T) {
	t.Parallel()
	pool := NewWorkerPool(1, nil, testLogger())
	started := make(chan struct{})
	exited := make(chan struct{})
	pool.executeStep = func(ctx context.Context, _ adapters.Step) error {
		close(started)
		<-ctx.Done()
		close(exited)
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := pool.Submit(ctx, workerAssignment(workerTestTaskID, "plan-a")); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	done := make(chan struct{})
	go func() {
		pool.Run(ctx)
		close(done)
	}()
	<-started
	cancel()
	select {
	case <-done:
		select {
		case <-exited:
		default:
			t.Fatal("Run() returned before executor cleanup")
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not join canceled worker")
	}
}

func workerAssignment(taskID, plan string) Assignment {
	return Assignment{
		TaskID:   taskID,
		PlanHash: sha256.Sum256([]byte(plan)),
		Steps: []TaskStep{{
			StepID: workerTestStepID,
			Step:   adapters.Step{Op: adapters.StepAck},
		}},
		Timeout: time.Minute,
	}
}

func nextWorkerResult(t *testing.T, pool *WorkerPool) TaskResult {
	t.Helper()
	for {
		select {
		case output := <-pool.Outputs():
			if output.Result != nil {
				return *output.Result
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for worker result")
		}
	}
}
