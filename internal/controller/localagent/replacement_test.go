package localagent

import (
	context "context"
	errors "errors"
	errs "github.com/AlanD20/groundplane/pkg/errs"
	testing "testing"
)

func TestReconcileFencesPriorGenerationAfterCancellationAtReplacementCommit(t *testing.T) {

	t.Parallel()

	trace := &traceLog{}
	repository := seededRepository(trace, PhaseReady)
	first := newTestManager(t, repository, trace)
	ctx, cancel := context.WithCancel(context.Background())
	repository.replacementCommitted = func(generation uint64) {
		if generation == initialGeneration+1 {
			cancel()
		}
	}
	request := UpdateRequest{
		AgentID: testAgentID, PreviousImage: testImage, DesiredImage: replacementTestImage,
		StartingGeneration: initialGeneration,
	}
	if err := first.manager.Update(ctx, request); !errors.Is(err, context.Canceled) {
		t.Fatalf("Update() error = %v, want context.Canceled", err)
	}
	interrupted := cloneRecord(repository.record.Record)
	if interrupted.Phase != PhaseUpdating || interrupted.Generation != initialGeneration+1 {
		t.Fatalf("interrupted replacement = %#v", interrupted)
	}

	resumed := newTestManager(t, repository, trace)
	if err := resumed.manager.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if len(resumed.sessions.fencedThrough) != 1 ||
		resumed.sessions.fencedThrough[0] != initialGeneration {
		t.Fatalf("recovery fences = %v, want [%d]", resumed.sessions.fencedThrough, initialGeneration)
	}
	if got := trace.values(); !containsOrdered(got, "begin_replacement", "fence_through", "materialize") {
		t.Fatalf("recovery order = %v, want prior-generation fence before materialization", got)
	}
	if repository.record.Record.Generation != interrupted.Generation ||
		repository.record.Record.Credential.Digest != interrupted.Credential.Digest ||
		resumed.runtime.generateCalls != 0 {
		t.Fatal("recovery rotated the current replacement credential")
	}
}

func TestReconcileFencesReplacementGenerationAfterCancellationAtRollbackCommit(t *testing.T) {

	t.Parallel()

	trace := &traceLog{}
	repository := seededRepository(trace, PhaseReady)
	first := newTestManager(t, repository, trace)
	first.sessions.readySequence = []<-chan struct{}{make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	repository.replacementCommitted = func(generation uint64) {
		if generation == initialGeneration+2 {
			cancel()
		}
	}
	request := UpdateRequest{
		AgentID: testAgentID, PreviousImage: testImage, DesiredImage: replacementTestImage,
		StartingGeneration: initialGeneration,
	}
	result := make(chan error, 1)
	go func() { result <- first.manager.Update(ctx, request) }()
	first.clock.awaitTimer(t).fire(testNow.Add(ReadyTimeout))
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Update() error = %v, want context.Canceled", err)
	}
	interrupted := cloneRecord(repository.record.Record)
	if interrupted.Phase != PhaseUpdating || interrupted.Generation != initialGeneration+2 ||
		interrupted.Image != testImage {
		t.Fatalf("interrupted rollback = %#v", interrupted)
	}

	resumed := newTestManager(t, repository, trace)
	if err := resumed.manager.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if len(resumed.sessions.fencedThrough) != 1 ||
		resumed.sessions.fencedThrough[0] != initialGeneration+1 {
		t.Fatalf("recovery fences = %v, want [%d]", resumed.sessions.fencedThrough, initialGeneration+1)
	}
	if got := trace.values(); !containsOrdered(
		got,
		"begin_replacement",
		"begin_replacement",
		"fence_through",
		"materialize",
	) {
		t.Fatalf("rollback recovery order = %v, want prior-generation fence before materialization", got)
	}
	if repository.record.Record.Generation != interrupted.Generation ||
		repository.record.Record.Credential.Digest != interrupted.Credential.Digest ||
		resumed.runtime.generateCalls != 0 {
		t.Fatal("rollback recovery rotated the current credential")
	}
}

func TestReconcileCompletesInterruptedUpdateAfterAuthenticatedReady(t *testing.T) {

	t.Parallel()

	trace := &traceLog{}
	repository := seededRepository(trace, PhaseReady)
	wantConfig := cloneConfig(repository.record.Record.Config)
	wantReadyAt := repository.record.Record.ReadyAt
	first := newTestManager(t, repository, trace)
	first.sessions.ready = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	request := UpdateRequest{
		AgentID: testAgentID, PreviousImage: testImage, DesiredImage: replacementTestImage,
		StartingGeneration: initialGeneration,
	}
	go func() { result <- first.manager.Update(ctx, request) }()
	first.clock.awaitTimer(t)
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Update() error = %v, want context.Canceled", err)
	}
	interrupted := cloneRecord(repository.record.Record)
	if interrupted.Phase != PhaseUpdating || interrupted.Generation != initialGeneration+1 ||
		interrupted.Image != replacementTestImage {
		t.Fatalf("interrupted replacement = %#v", interrupted)
	}

	resumed := newTestManager(t, repository, trace)
	if err := resumed.manager.Reconcile(context.Background()); err != nil {
		t.Fatalf("resumed Reconcile() error = %v", err)
	}
	if err := resumed.manager.Reconcile(context.Background()); err != nil {
		t.Fatalf("repeated Reconcile() error = %v", err)
	}
	got := repository.record.Record
	if got.ID != interrupted.ID || got.EnrollmentTaskID != interrupted.EnrollmentTaskID ||
		got.Image != interrupted.Image || got.Generation != interrupted.Generation ||
		got.Phase != PhaseReady || !got.ReadyAt.Equal(wantReadyAt) ||
		!equalConfig(got.Config, wantConfig) ||
		got.Credential.Digest != interrupted.Credential.Digest || string(got.Credential.EncryptedToken) != string(interrupted.Credential.EncryptedToken) {
		t.Fatalf("reconciled replacement = %#v, interrupted = %#v", got, interrupted)
	}
	if resumed.runtime.generateCalls != 0 {
		t.Fatalf("reconciliation generated %d credentials, want 0", resumed.runtime.generateCalls)
	}
	if err := resumed.manager.Update(context.Background(), request); err != nil {
		t.Fatalf("completed update replay error = %v", err)
	}
	if repository.record.Record.Generation != interrupted.Generation {
		t.Fatalf(
			"completed replay generation = %d, want %d",
			repository.record.Record.Generation,
			interrupted.Generation,
		)
	}
}

func TestReconcileCompletesInterruptedRollbackAfterAuthenticatedReady(t *testing.T) {

	t.Parallel()

	trace := &traceLog{}
	repository := seededRepository(trace, PhaseReady)
	wantConfig := cloneConfig(repository.record.Record.Config)
	wantReadyAt := repository.record.Record.ReadyAt
	first := newTestManager(t, repository, trace)
	first.sessions.readySequence = []<-chan struct{}{make(chan struct{}), make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	request := UpdateRequest{
		AgentID: testAgentID, PreviousImage: testImage, DesiredImage: replacementTestImage,
		StartingGeneration: initialGeneration,
	}
	go func() { result <- first.manager.Update(ctx, request) }()
	first.clock.awaitTimer(t).fire(testNow.Add(ReadyTimeout))
	first.clock.awaitTimer(t)
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Update() error = %v, want context.Canceled", err)
	}
	interrupted := cloneRecord(repository.record.Record)
	if interrupted.Phase != PhaseUpdating || interrupted.Generation != initialGeneration+2 ||
		interrupted.Image != testImage {
		t.Fatalf("interrupted rollback = %#v", interrupted)
	}

	resumed := newTestManager(t, repository, trace)
	if err := resumed.manager.Reconcile(context.Background()); err != nil {
		t.Fatalf("resumed Reconcile() error = %v", err)
	}
	if err := resumed.manager.Reconcile(context.Background()); err != nil {
		t.Fatalf("repeated Reconcile() error = %v", err)
	}
	got := repository.record.Record
	if got.ID != interrupted.ID || got.EnrollmentTaskID != interrupted.EnrollmentTaskID ||
		got.Image != interrupted.Image || got.Generation != interrupted.Generation ||
		got.Phase != PhaseReady || !got.ReadyAt.Equal(wantReadyAt) ||
		!equalConfig(got.Config, wantConfig) ||
		got.Credential.Digest != interrupted.Credential.Digest || string(got.Credential.EncryptedToken) != string(interrupted.Credential.EncryptedToken) {
		t.Fatalf("reconciled rollback = %#v, interrupted = %#v", got, interrupted)
	}
	if resumed.runtime.generateCalls != 0 {
		t.Fatalf("rollback reconciliation generated %d credentials, want 0", resumed.runtime.generateCalls)
	}
	if err := resumed.manager.Update(context.Background(), UpdateRequest{
		AgentID: testAgentID, PreviousImage: testImage, DesiredImage: replacementTestImage,
		StartingGeneration: interrupted.Generation,
	}); err != nil {
		t.Fatalf("fresh update after reconciled rollback error = %v", err)
	}
	if repository.record.Record.Phase != PhaseReady ||
		repository.record.Record.Generation != interrupted.Generation+1 ||
		repository.record.Record.Image != replacementTestImage {
		t.Fatalf("fresh replacement after rollback = %#v", repository.record.Record)
	}
}

func TestUpdateRejectsBusyAgentBeforeCredentialOrDurableMutation(t *testing.T) {

	t.Parallel()

	trace := &traceLog{}
	repository := seededRepository(trace, PhaseReady)
	harness := newTestManager(t, repository, trace)
	harness.tasks.idleError = errs.New(errs.KindResourceInUse, "active Task assignment")

	err := harness.manager.Update(context.Background(), UpdateRequest{
		AgentID: testAgentID, PreviousImage: testImage, DesiredImage: replacementTestImage,
		StartingGeneration: initialGeneration,
	})
	if !errors.Is(err, errs.New(errs.KindResourceInUse, "")) {
		t.Fatalf("Update() error = %v, want resource.in_use", err)
	}
	if repository.record.Record.Image != testImage ||
		repository.record.Record.Generation != initialGeneration ||
		repository.record.Record.Phase != PhaseReady {
		t.Fatalf("record mutated for busy Agent = %#v", repository.record.Record)
	}
	if harness.runtime.generateCalls != 0 || harness.runtime.materializeCalls != 0 ||
		harness.container.convergeCalls != 0 {
		t.Fatalf(
			"busy update side effects = generate %d, materialize %d, converge %d",
			harness.runtime.generateCalls,
			harness.runtime.materializeCalls,
			harness.container.convergeCalls,
		)
	}
}

func TestUpdateRotatesIdleAgentAndPreservesStableMetadata(t *testing.T) {

	t.Parallel()

	trace := &traceLog{}
	repository := seededRepository(trace, PhaseReady)
	wantConfig := cloneConfig(repository.record.Record.Config)
	wantReadyAt := repository.record.Record.ReadyAt
	harness := newTestManager(t, repository, trace)

	err := harness.manager.Update(context.Background(), UpdateRequest{
		AgentID: testAgentID, PreviousImage: testImage, DesiredImage: replacementTestImage,
		StartingGeneration: initialGeneration,
	})
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	got := repository.record.Record
	if got.ID != testAgentID || got.EnrollmentTaskID != testRequest(testAgentID).EnrollmentTaskID ||
		got.Image != replacementTestImage || got.Generation != initialGeneration+1 ||
		got.Phase != PhaseReady || !got.ReadyAt.Equal(wantReadyAt) || !equalConfig(got.Config, wantConfig) {
		t.Fatalf("updated record = %#v", got)
	}
	if got.Credential.Digest == testCredential().Digest {
		t.Fatal("successful update retained the previous token digest")
	}
	wantOrder := []string{
		"stop_assignments", "require_idle", "generate", "begin_replacement",
		"revoke", "wait_offline", "materialize", "converge", "mark_replacement_ready",
	}
	if gotTrace := trace.values(); !containsOrdered(gotTrace, wantOrder...) {
		t.Fatalf("update order = %v, want subsequence %v", gotTrace, wantOrder)
	}
}

func TestUpdateRollsBackPreviousDigestAfterReplacementReadinessTimeout(t *testing.T) {

	t.Parallel()

	trace := &traceLog{}
	repository := seededRepository(trace, PhaseReady)
	wantReadyAt := repository.record.Record.ReadyAt
	harness := newTestManager(t, repository, trace)
	harness.sessions.readySequence = []<-chan struct{}{make(chan struct{}), closedSignal()}

	result := make(chan error, 1)
	go func() {
		result <- harness.manager.Update(context.Background(), UpdateRequest{
			AgentID: testAgentID, PreviousImage: testImage, DesiredImage: replacementTestImage,
			StartingGeneration: initialGeneration,
		})
	}()
	harness.clock.awaitTimer(t).fire(testNow.Add(ReadyTimeout))
	err := <-result
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("Update() error = %v, want failed update after rollback", err)
	}
	got := repository.record.Record
	if got.Image != testImage || got.Generation != initialGeneration+2 ||
		got.Phase != PhaseReady || !got.ReadyAt.Equal(wantReadyAt) {
		t.Fatalf("rolled-back record = %#v", got)
	}
	if harness.runtime.generateCalls != 2 || harness.container.convergeCalls != 2 {
		t.Fatalf(
			"rollback rotations = credentials %d, convergences %d",
			harness.runtime.generateCalls,
			harness.container.convergeCalls,
		)
	}
}

func TestUpdateReplayRecognizesCompletedRollback(t *testing.T) {

	t.Parallel()

	trace := &traceLog{}
	repository := seededRepository(trace, PhaseReady)
	repository.record.Record.Generation = initialGeneration + 2
	harness := newTestManager(t, repository, trace)

	err := harness.manager.Update(context.Background(), UpdateRequest{
		AgentID: testAgentID, PreviousImage: testImage, DesiredImage: replacementTestImage,
		StartingGeneration: initialGeneration,
	})
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("Update(replayed rollback) error = %v, want failed update", err)
	}
	if harness.runtime.generateCalls != 0 || harness.container.convergeCalls != 0 {
		t.Fatal("completed rollback replay mutated runtime")
	}
}
