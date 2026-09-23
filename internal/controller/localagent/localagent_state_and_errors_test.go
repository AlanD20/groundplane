package localagent

import (
	context "context"
	base64 "encoding/base64"
	errors "errors"
	strings "strings"
	sync "sync"
	testing "testing"
	time "time"

	errs "github.com/AlanD20/groundplane/pkg/errs"
)

func cloneStored(stored StoredRecord) StoredRecord {
	stored.Record = cloneRecord(stored.Record)
	return stored
}

type fakeRuntime struct {
	trace            *traceLog
	generateCalls    int
	materializeCalls int
	removeCalls      int
	materializeError error
	removeError      error
}

func (runtime *fakeRuntime) GenerateCredential(ctx context.Context, _ string) (Credential, error) {
	if err := ctx.Err(); err != nil {
		return Credential{}, err
	}
	runtime.trace.add("generate")
	runtime.generateCalls++
	credential := testCredential()
	digest, _ := base64.RawURLEncoding.DecodeString(credential.Digest)
	digest[len(digest)-1] += byte(runtime.generateCalls)
	credential.Digest = base64.RawURLEncoding.EncodeToString(digest)
	return credential, nil
}

func (runtime *fakeRuntime) Materialize(ctx context.Context, _ RuntimeMaterial) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	runtime.trace.add("materialize")
	runtime.materializeCalls++
	if runtime.materializeError != nil {
		err := runtime.materializeError
		runtime.materializeError = nil
		return err
	}
	return nil
}

func (runtime *fakeRuntime) Remove(ctx context.Context, _ string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	runtime.trace.add("runtime_remove")
	runtime.removeCalls++
	return runtime.removeError
}

type fakeContainer struct {
	trace         *traceLog
	convergeCalls int
	removeCalls   int
	convergeError error
	removeError   error
}

func (container *fakeContainer) Converge(ctx context.Context, _ ContainerDesired) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	container.trace.add("converge")
	container.convergeCalls++
	return container.convergeError
}

func (container *fakeContainer) Remove(ctx context.Context, _ string, _ uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	container.trace.add("container_remove")
	container.removeCalls++
	return container.removeError
}

type fakeSessions struct {
	trace         *traceLog
	ready         <-chan struct{}
	readySequence []<-chan struct{}
	readyContext  context.Context
	snapshot      SessionSnapshot
	hasSnapshot   bool
	stopError     error
	revokeError   error
	offlineError  error
	fenceError    error
	fencedThrough []uint64
}

func (sessions *fakeSessions) Ready(
	ctx context.Context,
	_ string,
	_ uint64,
) (<-chan struct{}, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sessions.readyContext = ctx
	if len(sessions.readySequence) != 0 {
		ready := sessions.readySequence[0]
		sessions.readySequence = sessions.readySequence[1:]
		return ready, nil
	}
	return sessions.ready, nil
}

func (sessions *fakeSessions) Snapshot(string) (SessionSnapshot, bool) {
	return sessions.snapshot, sessions.hasSnapshot
}

func (sessions *fakeSessions) StopAssignments(ctx context.Context, _ string, _ uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	sessions.trace.add("stop_assignments")
	return sessions.stopError
}

func (sessions *fakeSessions) FenceThrough(ctx context.Context, _ string, generation uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	sessions.trace.add("fence_through")
	sessions.fencedThrough = append(sessions.fencedThrough, generation)
	return sessions.fenceError
}

func (sessions *fakeSessions) Revoke(ctx context.Context, _ string, _ uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	sessions.trace.add("revoke")
	return sessions.revokeError
}

func (sessions *fakeSessions) WaitOffline(ctx context.Context, _ string, _ uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	sessions.trace.add("wait_offline")
	return sessions.offlineError
}

type fakeTasks struct {
	trace      *traceLog
	abortError error
	idleError  error
}

func (tasks *fakeTasks) RequireIdle(ctx context.Context, _ string, _ uint64, _ int32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tasks.trace.add("require_idle")
	return tasks.idleError
}

func (tasks *fakeTasks) AbortActive(
	ctx context.Context,
	_ string,
	_ uint64,
	_ int32,
	reason string,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if reason != RemovedTaskReason {
		return errors.New("unexpected abort reason")
	}
	tasks.trace.add("abort_active")
	return tasks.abortError
}

type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	created chan *fakeTimer
}

func newFakeClock(now time.Time) *fakeClock {
	return &fakeClock{now: now, created: make(chan *fakeTimer, 8)}
}

func (clock *fakeClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *fakeClock) NewTimer(duration time.Duration) Timer {
	timer := &fakeTimer{duration: duration, channel: make(chan time.Time, 1)}
	clock.created <- timer
	return timer
}

func (clock *fakeClock) setNow(now time.Time) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.now = now
}

func (clock *fakeClock) awaitTimer(t *testing.T) *fakeTimer {
	t.Helper()
	select {
	case timer := <-clock.created:
		return timer
	case <-time.After(time.Second):
		t.Fatal("readiness timer was not created")
		return nil
	}
}

type fakeTimer struct {
	duration time.Duration
	channel  chan time.Time
}

func (timer *fakeTimer) C() <-chan time.Time {
	return timer.channel
}

func (timer *fakeTimer) Stop() {}

func (timer *fakeTimer) fire(at time.Time) {
	timer.channel <- at
}

type traceLog struct {
	mu      sync.Mutex
	actions []string
}

func (trace *traceLog) add(action string) {
	trace.mu.Lock()
	defer trace.mu.Unlock()
	trace.actions = append(trace.actions, action)
}

func (trace *traceLog) values() []string {
	trace.mu.Lock()
	defer trace.mu.Unlock()
	return append([]string(nil), trace.actions...)
}

func (trace *traceLog) contains(action string) bool {
	for _, candidate := range trace.values() {
		if candidate == action {
			return true
		}
	}
	return false
}

func containsOrdered(values []string, expected ...string) bool {
	index := 0
	for _, value := range values {
		if index < len(expected) && value == expected[index] {
			index++
		}
	}
	return index == len(expected)
}

func closedSignal() <-chan struct{} {
	ready := make(chan struct{})
	close(ready)
	return ready
}

// Rationale: only cancellation owned by the caller context may cross a port
// unchanged; dependency cancellation and deadline failures have unknown origin
// and must become sanitized internal errors.
func TestSafePortErrorUsesCallerContextOnly(t *testing.T) {
	t.Parallel()

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := safePortError(cancelled, errors.New("dependency secret"), "safe"); !errors.Is(err, context.Canceled) {
		t.Fatalf("caller cancellation = %v", err)
	}

	deadline, deadlineCancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer deadlineCancel()
	if err := safePortError(
		deadline,
		errors.New("dependency secret"),
		"safe",
	); !errors.Is(
		err,
		context.DeadlineExceeded,
	) {
		t.Fatalf("caller deadline = %v", err)
	}

	for _, dependencyErr := range []error{context.Canceled, context.DeadlineExceeded} {
		err := safePortError(context.Background(), dependencyErr, "safe port failure")
		if !errors.Is(err, errs.New(errs.KindInternal, "")) {
			t.Fatalf("dependency error %v became %v", dependencyErr, err)
		}
		problem := err.(*errs.Error).ToProblem()
		if problem.Detail != "Internal Server Error" || strings.Contains(problem.Detail, "dependency") {
			t.Fatalf("dependency problem = %#v", problem)
		}
	}
}

// Rationale: a domain Kind may cross a port, but its private cause and message
// must be replaced so secret-bearing backend text cannot reach an API problem.
func TestSafePortErrorPreservesKindWithoutPrivateCause(t *testing.T) {
	t.Parallel()

	const secret = "token=super-secret"
	cause := errors.New(secret)
	backendErr := errs.Wrap(errs.KindStorageUnavailable, cause)
	err := safePortError(context.Background(), backendErr, "durable store unavailable")
	if !errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) {
		t.Fatalf("safe port error = %v", err)
	}
	if errors.Is(err, cause) {
		t.Fatal("safe port error retained private backend cause")
	}
	problem := err.(*errs.Error).ToProblem()
	if problem.Detail != "durable store unavailable" || strings.Contains(problem.Detail, secret) {
		t.Fatalf("safe port problem = %#v", problem)
	}
}
