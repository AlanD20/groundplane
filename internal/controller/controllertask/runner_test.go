package controllertask

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

func TestRunnerResumesClaimBeforeClaimingAndAcknowledgesCompletion(t *testing.T) {
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	claim := controllerClaim(now, now.Add(time.Minute))
	store := &fakeStore{claims: []etcd.TaskAssignment{claim}}
	handler := &fakeHandler{}
	runner := testRunner(t, store, handler, now)

	progressed, err := runner.runOne(context.Background())
	if err != nil || !progressed {
		t.Fatalf("runOne() = %v, %v", progressed, err)
	}
	if store.claimCalls != 0 || handler.calls != 1 || handler.task.ID != claim.Task.Record.ID {
		t.Fatalf("resume calls = store %d, handler %d/%s", store.claimCalls, handler.calls, handler.task.ID)
	}
	if store.ackStatus != etcd.TaskStatusCompleted || store.ackTaskID != claim.Task.Record.ID {
		t.Fatalf("ack = %s/%s", store.ackTaskID, store.ackStatus)
	}
}

func TestRunnerClaimsNewWorkAndTimesOutExpiredClaimWithoutExecuting(t *testing.T) {
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	claim := controllerClaim(now.Add(-time.Minute), now)
	store := &fakeStore{next: claim, found: true}
	handler := &fakeHandler{}
	runner := testRunner(t, store, handler, now)

	progressed, err := runner.runOne(context.Background())
	if err != nil || !progressed {
		t.Fatalf("runOne() = %v, %v", progressed, err)
	}
	if store.claimCalls != 1 || handler.calls != 0 || store.ackStatus != etcd.TaskStatusTimedOut {
		t.Fatalf("timeout calls = claim %d, handler %d, status %s", store.claimCalls, handler.calls, store.ackStatus)
	}
}

func TestRunnerLeavesClaimRunningWhenControllerStops(t *testing.T) {
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	claim := controllerClaim(now, now.Add(time.Minute))
	store := &fakeStore{claims: []etcd.TaskAssignment{claim}}
	handler := &fakeHandler{waitForCancellation: true}
	runner := testRunner(t, store, handler, now)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	progressed, err := runner.runOne(ctx)
	if !progressed || err == nil || store.ackCalls != 0 {
		t.Fatalf("runOne(cancelled) = %v, %v, ack calls %d", progressed, err, store.ackCalls)
	}
}

func TestRunnerWakeClaimsWorkBeforeRecoveryInterval(t *testing.T) {
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	store := newWakeStore(controllerClaim(now, now.Add(time.Minute)))
	runner, err := New(
		store,
		&fakeHandler{},
		time.Hour,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatal(err)
	}
	runner.now = func() time.Time { return now }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go runner.Run(ctx)
	select {
	case <-store.emptyScan:
	case <-time.After(time.Second):
		t.Fatal("Controller Task runner did not perform its startup scan")
	}
	store.enqueue()
	runner.Wake()
	select {
	case <-store.acknowledged:
	case <-time.After(time.Second):
		t.Fatal("Controller Task runner did not claim work after Wake")
	}
}

func testRunner(t *testing.T, store Store, handler Handler, now time.Time) *Runner {
	t.Helper()
	runner, err := New(
		store,
		handler,
		time.Second,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	runner.now = func() time.Time { return now }
	return runner
}

func controllerClaim(assignedAt time.Time, deadline time.Time) etcd.TaskAssignment {
	taskID := ids.NewAt(ids.KindTask, assignedAt, 1)
	return etcd.TaskAssignment{
		Assignment: etcd.Versioned[etcd.TaskAssignmentRecord]{Record: etcd.TaskAssignmentRecord{
			TaskID: taskID, Executor: etcd.TaskExecutorController,
			ClaimedTaskRevision: 2, AssignedAt: assignedAt, Deadline: deadline,
		}},
		Task: etcd.Versioned[etcd.TaskRecord]{Record: etcd.TaskRecord{
			ID: taskID, Executor: etcd.TaskExecutorController,
		}},
	}
}

type fakeStore struct {
	claims     []etcd.TaskAssignment
	next       etcd.TaskAssignment
	found      bool
	claimCalls int
	ackCalls   int
	ackTaskID  string
	ackStatus  etcd.TaskStatus
}

func (store *fakeStore) ListControllerTaskClaims(context.Context) ([]etcd.TaskAssignment, error) {
	return append([]etcd.TaskAssignment(nil), store.claims...), nil
}

func (store *fakeStore) ClaimNextControllerTask(
	context.Context,
	time.Time,
) (etcd.TaskAssignment, bool, error) {
	store.claimCalls++
	return store.next, store.found, nil
}

func (store *fakeStore) AcknowledgeControllerTask(
	_ context.Context,
	taskID string,
	status etcd.TaskStatus,
	_ time.Time,
) (etcd.Versioned[etcd.TaskRecord], error) {
	store.ackCalls++
	store.ackTaskID = taskID
	store.ackStatus = status
	return etcd.Versioned[etcd.TaskRecord]{Record: etcd.TaskRecord{ID: taskID, Status: status}}, nil
}

type fakeHandler struct {
	calls               int
	task                etcd.TaskRecord
	waitForCancellation bool
}

type wakeStore struct {
	mu           sync.Mutex
	claim        etcd.TaskAssignment
	queued       bool
	emptyScan    chan struct{}
	acknowledged chan struct{}
	emptyOnce    sync.Once
	ackOnce      sync.Once
}

func newWakeStore(claim etcd.TaskAssignment) *wakeStore {
	return &wakeStore{
		claim: claim, emptyScan: make(chan struct{}), acknowledged: make(chan struct{}),
	}
}

func (store *wakeStore) enqueue() {
	store.mu.Lock()
	store.queued = true
	store.mu.Unlock()
}

func (*wakeStore) ListControllerTaskClaims(context.Context) ([]etcd.TaskAssignment, error) {
	return nil, nil
}

func (store *wakeStore) ClaimNextControllerTask(
	context.Context,
	time.Time,
) (etcd.TaskAssignment, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if !store.queued {
		store.emptyOnce.Do(func() { close(store.emptyScan) })
		return etcd.TaskAssignment{}, false, nil
	}
	store.queued = false
	return store.claim, true, nil
}

func (store *wakeStore) AcknowledgeControllerTask(
	context.Context,
	string,
	etcd.TaskStatus,
	time.Time,
) (etcd.Versioned[etcd.TaskRecord], error) {
	store.ackOnce.Do(func() { close(store.acknowledged) })
	return etcd.Versioned[etcd.TaskRecord]{}, nil
}

func (handler *fakeHandler) Execute(ctx context.Context, task etcd.TaskRecord) error {
	handler.calls++
	handler.task = task
	if handler.waitForCancellation {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}
