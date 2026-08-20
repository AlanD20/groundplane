package localagent

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	testAgentID      = "agt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testOtherAgentID = "agt_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	testImage        = "ghcr.io/aland20/groundplane-agent@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
)

var testNow = time.Date(2026, time.August, 20, 12, 0, 0, 0, time.UTC)

func TestEnrollCommitsCredentialBeforeRuntimeAndEnforcesSingletonCAS(t *testing.T) {
	// Rationale: entropy failure or singleton contention must never expose an
	// uncommitted runtime token, container, or second local Agent.
	t.Parallel()

	trace := &traceLog{}
	repository := &fakeRepository{trace: trace}
	first := newTestManager(t, repository, trace)
	first.sessions.ready = closedSignal()
	if _, err := first.manager.Enroll(context.Background(), testRequest(testAgentID)); err != nil {
		t.Fatalf("first Enroll() error = %v", err)
	}
	second := newTestManager(t, repository, trace)
	second.sessions.ready = closedSignal()
	if _, err := second.manager.Enroll(
		context.Background(),
		testRequest(testOtherAgentID),
	); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("second Enroll() error = %v, want %s", err, errs.CodeStateConflict)
	}

	if repository.record.Record.ID != testAgentID {
		t.Fatalf("durable Agent id = %q, want %q", repository.record.Record.ID, testAgentID)
	}
	if got := trace.values(); !containsOrdered(got, "generate", "create", "materialize", "converge") {
		t.Fatalf("enrollment order = %v, want durable create before materialization", got)
	}
	if second.runtime.materializeCalls != 0 || second.container.convergeCalls != 0 {
		t.Fatalf(
			"losing enrollment materialize/converge = %d/%d, want 0/0",
			second.runtime.materializeCalls,
			second.container.convergeCalls,
		)
	}
}

func TestReconcileResumesProvisioningAfterCrashWithoutRotatingIdentity(t *testing.T) {
	// Rationale: a Controller crash after the durable transaction must reuse the
	// same Agent id, generation, and encrypted token instead of creating a peer.
	t.Parallel()

	trace := &traceLog{}
	repository := &fakeRepository{trace: trace}
	first := newTestManager(t, repository, trace)
	first.runtime.materializeError = errors.New("runtime unavailable")
	if _, err := first.manager.Enroll(context.Background(), testRequest(testAgentID)); err == nil {
		t.Fatal("Enroll() error = nil, want materialization failure")
	}
	before := cloneRecord(repository.record.Record)
	if before.Phase != PhaseProvisioning {
		t.Fatalf("phase after failure = %q, want %q", before.Phase, PhaseProvisioning)
	}

	resumed := newTestManager(t, repository, trace)
	resumed.sessions.ready = closedSignal()
	if err := resumed.manager.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	after := repository.record.Record
	if after.Phase != PhaseReady || after.ID != before.ID || after.Generation != before.Generation {
		t.Fatalf("resumed record = %+v, want same identity in ready phase", after)
	}
	if string(after.Credential.EncryptedToken) != string(before.Credential.EncryptedToken) ||
		after.Credential.Digest != before.Credential.Digest {
		t.Fatal("Reconcile() rotated the durable credential")
	}
	if resumed.runtime.generateCalls != 0 {
		t.Fatalf("resumed GenerateCredential calls = %d, want 0", resumed.runtime.generateCalls)
	}
}

func TestEnrollUsesExactReadinessDeadlineAndLeavesRetryablePhase(t *testing.T) {
	// Rationale: enrollment has one accepted 120-second authenticated Ready
	// deadline, while timeout must preserve provisioning state for reconciliation.
	t.Parallel()

	trace := &traceLog{}
	repository := &fakeRepository{trace: trace}
	harness := newTestManager(t, repository, trace)
	harness.sessions.ready = make(chan struct{})

	result := make(chan error, 1)
	go func() {
		_, err := harness.manager.Enroll(context.Background(), testRequest(testAgentID))
		result <- err
	}()
	timer := harness.clock.awaitTimer(t)
	if timer.duration != ReadyTimeout {
		t.Fatalf("readiness timer = %s, want %s", timer.duration, ReadyTimeout)
	}
	timer.fire(testNow.Add(ReadyTimeout))
	if err := <-result; !errors.Is(err, errs.New(errs.KindTaskTimedOut, "")) {
		t.Fatalf("Enroll() error = %v, want %s", err, errs.CodeTaskTimedOut)
	}
	if repository.record.Record.Phase != PhaseProvisioning {
		t.Fatalf("phase after timeout = %q, want %q", repository.record.Record.Phase, PhaseProvisioning)
	}
}

func TestReconcileAndRemoveAreIdempotent(t *testing.T) {
	// Rationale: boot loops and retried deletion tasks must safely repeat after
	// ambiguous process or Docker failures without changing the Agent generation.
	t.Parallel()

	trace := &traceLog{}
	repository := seededRepository(trace, PhaseReady)
	harness := newTestManager(t, repository, trace)
	if err := harness.manager.Reconcile(context.Background()); err != nil {
		t.Fatalf("first Reconcile() error = %v", err)
	}
	if err := harness.manager.Reconcile(context.Background()); err != nil {
		t.Fatalf("second Reconcile() error = %v", err)
	}
	if repository.record.Record.Generation != initialGeneration {
		t.Fatalf("generation = %d, want %d", repository.record.Record.Generation, initialGeneration)
	}
	if harness.runtime.materializeCalls != 2 || harness.container.convergeCalls != 2 {
		t.Fatalf(
			"reconcile calls = materialize %d, converge %d; want 2, 2",
			harness.runtime.materializeCalls,
			harness.container.convergeCalls,
		)
	}
	if err := harness.manager.Remove(context.Background(), testAgentID); err != nil {
		t.Fatalf("first Remove() error = %v", err)
	}
	removeCalls := harness.container.removeCalls
	if err := harness.manager.Remove(context.Background(), testAgentID); err != nil {
		t.Fatalf("second Remove() error = %v", err)
	}
	if harness.container.removeCalls != removeCalls {
		t.Fatalf("second Remove() container calls = %d, want %d", harness.container.removeCalls, removeCalls)
	}
}

func TestRemovePersistsRevocationBeforeRuntimeDeletion(t *testing.T) {
	// Rationale: no container or credential file may be removed while a token can
	// still authenticate, and the durable record must remain until cleanup ends.
	t.Parallel()

	trace := &traceLog{}
	repository := seededRepository(trace, PhaseReady)
	harness := newTestManager(t, repository, trace)
	if err := harness.manager.Remove(context.Background(), testAgentID); err != nil {
		t.Fatalf("Remove() error = %v", err)
	}
	want := []string{
		"stop_assignments",
		"abort_active",
		"begin_delete",
		"revoke",
		"wait_offline",
		"container_remove",
		"runtime_remove",
		"repo_delete",
	}
	if got := trace.values(); !containsOrdered(got, want...) {
		t.Fatalf("removal order = %v, want subsequence %v", got, want)
	}
}

func TestRemoveFailureNeverAdvancesPastTheFailedBoundary(t *testing.T) {
	// Rationale: each deletion boundary must leave enough durable state for a
	// later boot reconciliation and must never delete the record prematurely.
	t.Parallel()

	tests := []struct {
		name       string
		configure  func(*testHarness, error)
		forbidden  string
		wantPhase  Phase
		wantRecord bool
	}{
		{
			name: "abort active",
			configure: func(harness *testHarness, failure error) {
				harness.tasks.abortError = failure
			},
			forbidden:  "begin_delete",
			wantPhase:  PhaseReady,
			wantRecord: true,
		},
		{
			name: "session revoke",
			configure: func(harness *testHarness, failure error) {
				harness.sessions.revokeError = failure
			},
			forbidden:  "container_remove",
			wantPhase:  PhaseDeleting,
			wantRecord: true,
		},
		{
			name: "container removal",
			configure: func(harness *testHarness, failure error) {
				harness.container.removeError = failure
			},
			forbidden:  "runtime_remove",
			wantPhase:  PhaseDeleting,
			wantRecord: true,
		},
		{
			name: "runtime removal",
			configure: func(harness *testHarness, failure error) {
				harness.runtime.removeError = failure
			},
			forbidden:  "repo_delete",
			wantPhase:  PhaseDeleting,
			wantRecord: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			trace := &traceLog{}
			repository := seededRepository(trace, PhaseReady)
			harness := newTestManager(t, repository, trace)
			failure := errors.New("injected failure")
			test.configure(harness, failure)
			if err := harness.manager.Remove(
				context.Background(),
				testAgentID,
			); !errors.Is(
				err,
				errs.New(errs.KindInternal, ""),
			) {
				t.Fatalf("Remove() error = %v, want %s", err, errs.CodeInternal)
			}
			if trace.contains(test.forbidden) {
				t.Fatalf("trace %v contains forbidden later action %q", trace.values(), test.forbidden)
			}
			if repository.exists != test.wantRecord || repository.record.Record.Phase != test.wantPhase {
				t.Fatalf(
					"record exists/phase = %v/%q, want %v/%q",
					repository.exists,
					repository.record.Record.Phase,
					test.wantRecord,
					test.wantPhase,
				)
			}
		})
	}
}

func TestReconcileResumesDeletingAfterDockerOutage(t *testing.T) {
	// Rationale: Docker unavailability must leave a revoked deleting record that
	// a later Controller boot can finish without restoring channel authority.
	t.Parallel()

	trace := &traceLog{}
	repository := seededRepository(trace, PhaseDeleting)
	first := newTestManager(t, repository, trace)
	first.container.removeError = errors.New("Docker unavailable")
	if err := first.manager.Reconcile(context.Background()); err == nil {
		t.Fatal("Reconcile() error = nil, want Docker outage")
	}
	if !repository.exists || repository.record.Record.Phase != PhaseDeleting ||
		repository.record.Record.Credential.Digest != "" {
		t.Fatalf("record after outage = %+v, want revoked deleting record", repository.record)
	}

	resumed := newTestManager(t, repository, trace)
	if err := resumed.manager.Reconcile(context.Background()); err != nil {
		t.Fatalf("resumed Reconcile() error = %v", err)
	}
	if repository.exists {
		t.Fatal("record still exists after resumed deletion")
	}
}

func TestHealthRequiresCurrentNonStaleReadyGeneration(t *testing.T) {
	// Rationale: keepalive, an old container generation, and stale Ready reports
	// must never project the Agent as online or healthy.
	t.Parallel()

	trace := &traceLog{}
	repository := seededRepository(trace, PhaseReady)
	harness := newTestManager(t, repository, trace)
	harness.clock.setNow(testNow)
	harness.sessions.snapshot = SessionSnapshot{
		Generation: initialGeneration - 1,
		Online:     true,
		LastReady:  testNow,
		Capacity:   2,
	}
	harness.sessions.hasSnapshot = true
	health, err := harness.manager.Health(context.Background(), testAgentID)
	if err != nil {
		t.Fatalf("Health() error = %v", err)
	}
	if health.Online || health.Healthy {
		t.Fatal("old generation projected online")
	}

	harness.sessions.snapshot.Generation = initialGeneration
	harness.sessions.snapshot.LastReady = testNow.Add(-30 * time.Second)
	health, err = harness.manager.Health(context.Background(), testAgentID)
	if err != nil {
		t.Fatalf("Health() at exact stale boundary error = %v", err)
	}
	if !health.Online || !health.Healthy {
		t.Fatal("Ready at the exact 30-second boundary projected stale")
	}
	harness.sessions.snapshot.LastReady = testNow.Add(-30*time.Second - time.Nanosecond)
	health, err = harness.manager.Health(context.Background(), testAgentID)
	if err != nil {
		t.Fatalf("Health() after stale boundary error = %v", err)
	}
	if health.Online || health.Healthy {
		t.Fatal("stale Ready projected online")
	}
}

func TestCancellationStopsProvisioningBeforeReadyTransition(t *testing.T) {
	// Rationale: task cancellation must propagate through readiness waiting and
	// leave a durable provisioning record rather than reporting false success.
	t.Parallel()

	trace := &traceLog{}
	repository := &fakeRepository{trace: trace}
	harness := newTestManager(t, repository, trace)
	harness.sessions.ready = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := harness.manager.Enroll(ctx, testRequest(testAgentID))
		result <- err
	}()
	harness.clock.awaitTimer(t)
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Enroll() error = %v, want context.Canceled", err)
	}
	if repository.record.Record.Phase != PhaseProvisioning || trace.contains("mark_ready") {
		t.Fatalf("canceled enrollment record/trace = %+v/%v", repository.record, trace.values())
	}
}

func TestHealthRejectsNilContext(t *testing.T) {
	// Rationale: public lifecycle methods must fail canonically rather than panic
	// when application wiring violates the required context contract.
	t.Parallel()

	trace := &traceLog{}
	harness := newTestManager(t, seededRepository(trace, PhaseReady), trace)
	if _, err := harness.manager.Health(nil, testAgentID); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("Health(nil) error = %v, want %s", err, errs.CodeInternal)
	}
}

func TestEnrollRejectsMutableOrMalformedImageIdentity(t *testing.T) {
	// Rationale: enrollment must never defer mutable-tag or malformed-digest
	// rejection to Docker after creating durable Agent state.
	t.Parallel()

	tests := []struct {
		name  string
		image string
	}{
		{name: "empty"},
		{name: "tag", image: "ghcr.io/aland20/groundplane-agent:latest"},
		{
			name:  "tag and digest",
			image: "ghcr.io/aland20/groundplane-agent:v1@sha256:" + strings.Repeat("a", 64),
		},
		{
			name:  "short digest",
			image: "ghcr.io/aland20/groundplane-agent@sha256:" + strings.Repeat("a", 63),
		},
		{
			name:  "non hex digest",
			image: "ghcr.io/aland20/groundplane-agent@sha256:" + strings.Repeat("z", 64),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			trace := &traceLog{}
			repository := &fakeRepository{trace: trace}
			harness := newTestManager(t, repository, trace)
			request := testRequest(testAgentID)
			request.Image = test.image
			if _, err := harness.manager.Enroll(
				context.Background(),
				request,
			); !errors.Is(
				err,
				errs.New(errs.KindValidationFailed, ""),
			) {
				t.Fatalf("Enroll() error = %v, want %s", err, errs.CodeValidationFailed)
			}
			if harness.runtime.generateCalls != 0 || repository.exists {
				t.Fatal("invalid image reached credential generation or durable creation")
			}
		})
	}
}

func TestSystemClockAndDurableCreationTimesAreUTC(t *testing.T) {
	// Rationale: durable timestamps must remain unambiguous across Controller
	// hosts and JSON round trips, while corrupt non-UTC records fail closed.
	t.Parallel()

	if got := (SystemClock{}).Now(); got.IsZero() || got.Location() != time.UTC {
		t.Fatalf("SystemClock.Now() = %v in %v, want nonzero UTC", got, got.Location())
	}
	trace := &traceLog{}
	repository := seededRepository(trace, PhaseReady)
	repository.record.Record.CreatedAt = testNow.In(time.FixedZone("non-UTC", 3600))
	harness := newTestManager(t, repository, trace)
	if err := harness.manager.Reconcile(context.Background()); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("Reconcile() error = %v, want %s", err, errs.CodeInternal)
	}
	if harness.runtime.materializeCalls != 0 {
		t.Fatal("non-UTC durable record reached materialization")
	}
}

func TestConfigLabelsRequireNULFreeUTF8AtInputAndPersistence(t *testing.T) {
	// Rationale: labels cross YAML, JSON, protobuf, and Docker boundaries, so
	// invalid bytes must fail before durable or runtime side effects.
	t.Parallel()

	invalidLabels := []map[string]string{
		{"bad\x00key": "value"},
		{"key": "bad\x00value"},
		{string([]byte{0xff}): "value"},
		{"key": string([]byte{0xff})},
	}
	for index, labels := range invalidLabels {
		trace := &traceLog{}
		repository := &fakeRepository{trace: trace}
		harness := newTestManager(t, repository, trace)
		request := testRequest(testAgentID)
		request.Config.Labels = labels
		if _, err := harness.manager.Enroll(
			context.Background(),
			request,
		); !errors.Is(
			err,
			errs.New(errs.KindValidationFailed, ""),
		) {
			t.Fatalf("case %d Enroll() error = %v, want %s", index, err, errs.CodeValidationFailed)
		}
		if harness.runtime.generateCalls != 0 || repository.exists {
			t.Fatalf("case %d invalid labels reached side effects", index)
		}
	}

	trace := &traceLog{}
	repository := seededRepository(trace, PhaseReady)
	repository.record.Record.Config.Labels = map[string]string{"bad\x00key": "value"}
	harness := newTestManager(t, repository, trace)
	if err := harness.manager.Reconcile(context.Background()); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("stored-label Reconcile() error = %v, want %s", err, errs.CodeInternal)
	}
}

func TestPublicProjectionAndErrorsNeverExposeCredentialMaterial(t *testing.T) {
	// Rationale: encrypted tokens and digest indexes are storage-only values and
	// must not escape through lifecycle return values or adapter error causes.
	t.Parallel()

	trace := &traceLog{}
	repository := seededRepository(trace, PhaseReady)
	harness := newTestManager(t, repository, trace)
	health, err := harness.manager.Health(context.Background(), testAgentID)
	if err != nil {
		t.Fatalf("Health() error = %v", err)
	}
	encoded, err := json.Marshal(health)
	if err != nil {
		t.Fatalf("marshal Health: %v", err)
	}
	credential := repository.record.Record.Credential
	for _, secret := range []string{string(credential.EncryptedToken), credential.Digest} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("Health JSON contains credential material %q", secret)
		}
	}

	harness.runtime.materializeError = errors.New(
		string(credential.EncryptedToken) + ":" + credential.Digest,
	)
	err = harness.manager.Reconcile(context.Background())
	if err == nil {
		t.Fatal("Reconcile() error = nil, want injected runtime error")
	}
	for _, secret := range []string{string(credential.EncryptedToken), credential.Digest} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("public error contains credential material %q", secret)
		}
	}
}

type testHarness struct {
	manager   *Manager
	runtime   *fakeRuntime
	container *fakeContainer
	sessions  *fakeSessions
	tasks     *fakeTasks
	clock     *fakeClock
}

func newTestManager(t *testing.T, repository *fakeRepository, trace *traceLog) *testHarness {
	t.Helper()
	runtime := &fakeRuntime{trace: trace}
	container := &fakeContainer{trace: trace}
	sessions := &fakeSessions{trace: trace, ready: closedSignal()}
	tasks := &fakeTasks{trace: trace}
	clock := newFakeClock(testNow)
	manager, err := New(Dependencies{
		Repository: repository,
		Runtime:    runtime,
		Container:  container,
		Sessions:   sessions,
		Tasks:      tasks,
		Clock:      clock,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return &testHarness{
		manager:   manager,
		runtime:   runtime,
		container: container,
		sessions:  sessions,
		tasks:     tasks,
		clock:     clock,
	}
}

func testRequest(agentID string) EnrollRequest {
	return EnrollRequest{
		AgentID: agentID,
		Image:   testImage,
		Config: Config{
			PullIntervalSeconds: 10,
			MaxConcurrentTasks:  2,
			Labels:              map[string]string{"host": "local"},
		},
	}
}

func testCredential() Credential {
	digest := make([]byte, 32)
	for index := range digest {
		digest[index] = byte(index + 1)
	}
	return Credential{
		EncryptedToken: []byte("age-encrypted-token"),
		Digest:         base64.RawURLEncoding.EncodeToString(digest),
	}
}

func seededRepository(trace *traceLog, phase Phase) *fakeRepository {
	credential := testCredential()
	if phase == PhaseDeleting {
		credential.Digest = ""
	}
	return &fakeRepository{
		trace:  trace,
		exists: true,
		record: StoredRecord{
			Record: Record{
				ID:         testAgentID,
				Image:      testImage,
				Generation: initialGeneration,
				Phase:      phase,
				Config:     testRequest(testAgentID).Config,
				Credential: credential,
				CreatedAt:  testNow,
			},
			Revision: 1,
		},
	}
}

type fakeRepository struct {
	mu     sync.Mutex
	trace  *traceLog
	exists bool
	record StoredRecord
}

func (repository *fakeRepository) CreateSingleton(
	ctx context.Context,
	record Record,
) (StoredRecord, error) {
	if err := ctx.Err(); err != nil {
		return StoredRecord{}, err
	}
	repository.trace.add("create")
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if repository.exists {
		return StoredRecord{}, errs.New(errs.KindStateConflict, "local Agent already exists")
	}
	repository.exists = true
	repository.record = StoredRecord{Record: cloneRecord(record), Revision: 1}
	return cloneStored(repository.record), nil
}

func (repository *fakeRepository) GetSingleton(ctx context.Context) (StoredRecord, error) {
	if err := ctx.Err(); err != nil {
		return StoredRecord{}, err
	}
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if !repository.exists {
		return StoredRecord{}, errs.New(errs.KindAgentNotFound, "local Agent was not found")
	}
	return cloneStored(repository.record), nil
}

func (repository *fakeRepository) MarkReady(
	ctx context.Context,
	id string,
	generation uint64,
	revision int64,
) (StoredRecord, error) {
	if err := ctx.Err(); err != nil {
		return StoredRecord{}, err
	}
	repository.trace.add("mark_ready")
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if !repository.matches(id, generation, revision) || repository.record.Record.Phase != PhaseProvisioning {
		return StoredRecord{}, errs.New(errs.KindStateConflict, "local Agent changed")
	}
	repository.record.Record.Phase = PhaseReady
	repository.record.Revision++
	return cloneStored(repository.record), nil
}

func (repository *fakeRepository) BeginDelete(
	ctx context.Context,
	id string,
	generation uint64,
	revision int64,
) (StoredRecord, error) {
	if err := ctx.Err(); err != nil {
		return StoredRecord{}, err
	}
	repository.trace.add("begin_delete")
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if !repository.matches(id, generation, revision) {
		return StoredRecord{}, errs.New(errs.KindStateConflict, "local Agent changed")
	}
	repository.record.Record.Phase = PhaseDeleting
	repository.record.Record.Credential.Digest = ""
	repository.record.Revision++
	return cloneStored(repository.record), nil
}

func (repository *fakeRepository) Delete(
	ctx context.Context,
	id string,
	generation uint64,
	revision int64,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	repository.trace.add("repo_delete")
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if !repository.exists {
		return errs.New(errs.KindAgentNotFound, "local Agent was not found")
	}
	if !repository.matches(id, generation, revision) || repository.record.Record.Phase != PhaseDeleting {
		return errs.New(errs.KindStateConflict, "local Agent changed")
	}
	repository.exists = false
	return nil
}

func (repository *fakeRepository) matches(id string, generation uint64, revision int64) bool {
	return repository.exists && repository.record.Record.ID == id &&
		repository.record.Record.Generation == generation && repository.record.Revision == revision
}

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
	return testCredential(), nil
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
	trace        *traceLog
	ready        <-chan struct{}
	snapshot     SessionSnapshot
	hasSnapshot  bool
	stopError    error
	revokeError  error
	offlineError error
}

func (sessions *fakeSessions) Ready(
	ctx context.Context,
	_ string,
	_ uint64,
) (<-chan struct{}, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
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
}

func (tasks *fakeTasks) AbortActive(ctx context.Context, _ string, reason string) error {
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
