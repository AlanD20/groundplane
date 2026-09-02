package agentchannel

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	testAgentID      = "agt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testOtherAgentID = "agt_01ARZ3NDEKTSV4RRFFQ69G5FAW"
)

// Rationale: repeated publication hints must wake every current session while
// retaining at most one pending signal per Agent.
func TestRegistryWakeTaskDispatchBroadcastsAndCoalesces(t *testing.T) {
	registry := NewRegistry()
	first, err := registry.Open(context.Background(), testAgentID, 1)
	if err != nil {
		t.Fatalf("open first session: %v", err)
	}
	defer first.Close()
	second, err := registry.Open(context.Background(), testOtherAgentID, 1)
	if err != nil {
		t.Fatalf("open second session: %v", err)
	}
	defer second.Close()

	registry.WakeTaskDispatch()
	registry.WakeTaskDispatch()

	select {
	case <-first.taskDispatchWake():
	default:
		t.Fatal("first session did not receive task dispatch wake")
	}
	select {
	case <-second.taskDispatchWake():
	default:
		t.Fatal("second session did not receive task dispatch wake")
	}
	select {
	case <-first.taskDispatchWake():
		t.Fatal("first session received duplicate non-coalesced wake")
	default:
	}
	select {
	case <-second.taskDispatchWake():
		t.Fatal("second session received duplicate non-coalesced wake")
	default:
	}
}

type assignmentSendOutcome struct {
	sent bool
	err  error
}

type sessionOpenOutcome struct {
	session *Session
	err     error
}

// Rationale: every lifecycle fence must close assignment admission atomically
// without holding the global Registry lock while an admitted send ignores
// session cancellation.
func TestRegistryLifecycleFencesRemainResponsiveDuringUncooperativeSend(t *testing.T) {
	t.Run("stop assignments honors caller context", func(t *testing.T) {
		registry := NewRegistry()
		session, err := registry.Open(context.Background(), testAgentID, 1)
		if err != nil {
			t.Fatalf("open session: %v", err)
		}
		defer session.Close()
		release, sendResult := startUncooperativeAssignmentSend(t, session)
		released := false
		defer func() {
			if !released {
				close(release)
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		stopResult := make(chan error, 1)
		go func() { stopResult <- registry.StopAssignments(ctx, testAgentID, 1) }()
		waitForAssignmentFence(t, session)
		assertUnrelatedRegistryOpen(t, registry)
		select {
		case err := <-stopResult:
			t.Fatalf("StopAssignments() returned before send drain: %v", err)
		default:
		}
		cancel()
		if err := waitForLifecycleResult(t, stopResult); !errors.Is(err, context.Canceled) {
			t.Fatalf("StopAssignments() error = %v, want context.Canceled", err)
		}
		close(release)
		released = true
		assertAdmittedSendCompleted(t, sendResult)
		assertAssignmentSendRejected(t, session)
	})

	t.Run("revoke fences before bounded drain", func(t *testing.T) {
		registry := NewRegistry()
		session, err := registry.Open(context.Background(), testAgentID, 1)
		if err != nil {
			t.Fatalf("open session: %v", err)
		}
		defer session.Close()
		release, sendResult := startUncooperativeAssignmentSend(t, session)
		released := false
		defer func() {
			if !released {
				close(release)
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		revokeResult := make(chan error, 1)
		go func() { revokeResult <- registry.Revoke(ctx, testAgentID, 1) }()
		waitForAssignmentFence(t, session)
		assertUnrelatedRegistryOpen(t, registry)
		select {
		case <-session.Done():
		default:
			t.Fatal("Revoke() did not cancel the fenced session")
		}
		cancel()
		if err := waitForLifecycleResult(t, revokeResult); !errors.Is(err, context.Canceled) {
			t.Fatalf("Revoke() error = %v, want context.Canceled", err)
		}
		close(release)
		released = true
		assertAdmittedSendCompleted(t, sendResult)
		assertAssignmentSendRejected(t, session)
	})

	t.Run("replacement preserves the later generation", func(t *testing.T) {
		registry := NewRegistry()
		first, err := registry.Open(context.Background(), testAgentID, 1)
		if err != nil {
			t.Fatalf("open first session: %v", err)
		}
		defer first.Close()
		release, sendResult := startUncooperativeAssignmentSend(t, first)
		released := false
		defer func() {
			if !released {
				close(release)
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		openResult := make(chan sessionOpenOutcome, 1)
		go func() {
			session, openErr := registry.Open(ctx, testAgentID, 2)
			openResult <- sessionOpenOutcome{session: session, err: openErr}
		}()
		waitForAssignmentFence(t, first)
		assertUnrelatedRegistryOpen(t, registry)
		cancel()
		select {
		case outcome := <-openResult:
			if outcome.session != nil || !errors.Is(outcome.err, context.Canceled) {
				t.Fatalf("canceled replacement = %+v, want nil/context.Canceled", outcome)
			}
		case <-time.After(time.Second):
			t.Fatal("replacement did not honor caller cancellation")
		}
		close(release)
		released = true
		assertAdmittedSendCompleted(t, sendResult)

		replacement, err := registry.Open(context.Background(), testAgentID, 2)
		if err != nil {
			t.Fatalf("retry replacement: %v", err)
		}
		defer replacement.Close()
		first.Close()
		snapshot, ok := registry.Snapshot(testAgentID)
		if !ok || !snapshot.Online || snapshot.Generation != 2 {
			t.Fatalf("replacement snapshot = %+v, found = %v", snapshot, ok)
		}
	})
}

func startUncooperativeAssignmentSend(
	t *testing.T,
	session *Session,
) (chan struct{}, <-chan assignmentSendOutcome) {
	t.Helper()
	entered := make(chan struct{})
	release := make(chan struct{})
	result := make(chan assignmentSendOutcome, 1)
	go func() {
		sent, err := session.sendAssignment(func() error {
			close(entered)
			<-release
			return nil
		})
		result <- assignmentSendOutcome{sent: sent, err: err}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("assignment send did not enter serialized boundary")
	}
	return release, result
}

func waitForAssignmentFence(t *testing.T, session *Session) {
	t.Helper()
	select {
	case <-session.state.sendFence:
	case <-time.After(time.Second):
		t.Fatal("lifecycle operation did not fence assignment admission")
	}
}

func assertUnrelatedRegistryOpen(t *testing.T, registry *Registry) {
	t.Helper()
	result := make(chan error, 1)
	go func() {
		session, err := registry.Open(context.Background(), testOtherAgentID, 1)
		if err == nil {
			session.Close()
		}
		result <- err
	}()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("unrelated Open() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("unrelated Registry operation blocked behind assignment send")
	}
}

func waitForLifecycleResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(time.Second):
		t.Fatal("lifecycle operation did not honor caller context")
		return nil
	}
}

func assertAdmittedSendCompleted(t *testing.T, result <-chan assignmentSendOutcome) {
	t.Helper()
	select {
	case outcome := <-result:
		if !outcome.sent || outcome.err != nil {
			t.Fatalf("admitted send outcome = %+v, want sent without error", outcome)
		}
	case <-time.After(time.Second):
		t.Fatal("released assignment send did not complete")
	}
}

func assertAssignmentSendRejected(t *testing.T, session *Session) {
	t.Helper()
	called := false
	sent, err := session.sendAssignment(func() error {
		called = true
		return nil
	})
	if err != nil || sent || called {
		t.Fatalf("fenced send = (%v, %v, %v), want (false, nil, false)", sent, err, called)
	}
}

// Rationale: a replaced connection must lose authority without being able to
// mark its replacement offline or mutate its readiness state.
func TestRegistryFencesReplacementSessions(t *testing.T) {
	registry := NewRegistry()
	first, err := registry.Open(context.Background(), testAgentID, 1)
	if err != nil {
		t.Fatalf("open first session: %v", err)
	}
	second, err := registry.Open(context.Background(), testAgentID, 2)
	if err != nil {
		t.Fatalf("open replacement session: %v", err)
	}

	select {
	case <-first.Done():
	default:
		t.Fatal("replaced session was not canceled")
	}
	assertStateConflict(t, first.RecordReady(testTime(), 3, "v0.4.2"))
	first.Close()
	snapshot, ok := registry.Snapshot(testAgentID)
	if !ok || !snapshot.Online || snapshot.Generation != 2 {
		t.Fatalf("replacement snapshot = %+v, found = %v", snapshot, ok)
	}
	_, err = registry.Open(context.Background(), testAgentID, 1)
	assertStateConflict(t, err)
	second.Close()
}

// Rationale: enrollment must not miss the first Ready when the Agent reports it
// before the Controller installs its readiness subscription.
func TestRegistryReadyClosesImmediatelyAfterMatchingReady(t *testing.T) {
	registry := NewRegistry()
	session, err := registry.Open(context.Background(), testAgentID, 7)
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	defer session.Close()
	if err := session.RecordReady(testTime(), 3, "v0.4.2"); err != nil {
		t.Fatalf("record Ready: %v", err)
	}
	ready, err := registry.Ready(context.Background(), testAgentID, 7)
	if err != nil {
		t.Fatalf("subscribe Ready: %v", err)
	}
	select {
	case <-ready:
	default:
		t.Fatal("matching Ready subscription remained open")
	}
}

// Rationale: a Ready report belongs to one live authenticated stream; after
// disconnect, provisioning must wait for a fresh same-generation stream and
// report instead of reusing historical in-memory evidence.
func TestRegistryReadyWaitsForFreshReportAfterMatchingSessionCloses(t *testing.T) {
	registry := NewRegistry()
	first, err := registry.Open(context.Background(), testAgentID, 7)
	if err != nil {
		t.Fatalf("open first session: %v", err)
	}
	if err := first.RecordReady(testTime(), 3, "v0.4.2"); err != nil {
		t.Fatalf("record first Ready: %v", err)
	}
	first.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready, err := registry.Ready(ctx, testAgentID, 7)
	if err != nil {
		t.Fatalf("subscribe after disconnect: %v", err)
	}
	select {
	case <-ready:
		t.Fatal("historical Ready satisfied a subscription after disconnect")
	default:
	}

	replacement, err := registry.Open(context.Background(), testAgentID, 7)
	if err != nil {
		t.Fatalf("open replacement session: %v", err)
	}
	defer replacement.Close()
	if err := replacement.RecordReady(testTime().Add(time.Second), 2, "v0.4.2"); err != nil {
		t.Fatalf("record replacement Ready: %v", err)
	}
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("fresh matching Ready did not satisfy subscription")
	}
}

// Rationale: enrollment may subscribe before the Agent opens its stream, so
// subscription registration and the later matching Ready must be atomic.
func TestRegistryReadyClosesWhenMatchingReadyArrivesAfterSubscribe(t *testing.T) {
	registry := NewRegistry()
	ready, err := registry.Ready(context.Background(), testAgentID, 7)
	if err != nil {
		t.Fatalf("subscribe Ready: %v", err)
	}
	select {
	case <-ready:
		t.Fatal("Ready subscription closed before a matching report")
	default:
	}

	session, err := registry.Open(context.Background(), testAgentID, 7)
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	defer session.Close()
	if err := session.RecordReady(testTime(), 2, "v0.4.2"); err != nil {
		t.Fatalf("record Ready: %v", err)
	}
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("Ready subscription did not close")
	}
}

// Rationale: canceling enrollment must release its pending readiness
// subscription without changing a later subscriber for the same generation.
func TestRegistryReadyCancellationReleasesSubscription(t *testing.T) {
	registry := NewRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	ready, err := registry.Ready(ctx, testAgentID, 7)
	if err != nil {
		t.Fatalf("subscribe Ready: %v", err)
	}
	cancel()
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("canceled Ready subscription did not close")
	}

	later, err := registry.Ready(context.Background(), testAgentID, 7)
	if err != nil {
		t.Fatalf("subscribe later Ready: %v", err)
	}
	select {
	case <-later:
		t.Fatal("canceled subscription closed a later subscription")
	default:
	}
	session, err := registry.Open(context.Background(), testAgentID, 7)
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	defer session.Close()
	if err := session.RecordReady(testTime(), 1, "v0.4.2"); err != nil {
		t.Fatalf("record Ready: %v", err)
	}
	select {
	case <-later:
	case <-time.After(time.Second):
		t.Fatal("later Ready subscription did not close")
	}
}

// Rationale: a Ready report from a replacement generation must never satisfy
// enrollment or health waiting for an older durable generation.
func TestRegistryReadyIgnoresDifferentGeneration(t *testing.T) {
	registry := NewRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready, err := registry.Ready(ctx, testAgentID, 7)
	if err != nil {
		t.Fatalf("subscribe Ready: %v", err)
	}
	session, err := registry.Open(context.Background(), testAgentID, 8)
	if err != nil {
		t.Fatalf("open replacement: %v", err)
	}
	defer session.Close()
	if err := session.RecordReady(testTime(), 2, "v0.4.2"); err != nil {
		t.Fatalf("record replacement Ready: %v", err)
	}
	select {
	case <-ready:
		t.Fatal("replacement generation satisfied stale Ready subscription")
	default:
	}
}

// Rationale: a recovery fence must revoke every live session at or below its
// bound and must not report completion until the canceled stream closes.
func TestRegistryFenceThroughCancelsAndWaitsForPriorGeneration(t *testing.T) {
	registry := NewRegistry()
	session, err := registry.Open(context.Background(), testAgentID, 7)
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	result := make(chan error, 1)
	go func() {
		result <- fenceRegistryThrough(context.Background(), registry, testAgentID, 7)
	}()
	select {
	case <-session.Done():
	case <-time.After(time.Second):
		t.Fatal("prior-generation fence did not cancel the session")
	}
	select {
	case err := <-result:
		t.Fatalf("prior-generation fence returned before Close: %v", err)
	default:
	}
	session.Close()
	if err := <-result; err != nil {
		t.Fatalf("FenceThrough() error = %v", err)
	}
	_, err = registry.Open(context.Background(), testAgentID, 7)
	assertStateConflict(t, err)
}

// Rationale: recovery may repeat after the normal path already revoked and
// disconnected the prior generation, so the bounded fence must remain a no-op.
func TestRegistryFenceThroughIsIdempotentAfterNormalRevocation(t *testing.T) {
	registry := NewRegistry()
	session, err := registry.Open(context.Background(), testAgentID, 7)
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	if err := registry.Revoke(context.Background(), testAgentID, 7); err != nil {
		t.Fatalf("Revoke() error = %v", err)
	}
	session.Close()
	if err := registry.WaitOffline(context.Background(), testAgentID, 7); err != nil {
		t.Fatalf("WaitOffline() error = %v", err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if err := fenceRegistryThrough(context.Background(), registry, testAgentID, 7); err != nil {
			t.Fatalf("FenceThrough() attempt %d error = %v", attempt+1, err)
		}
	}
}

// Rationale: a current-generation session proves every lower generation has
// already lost registry authority; recovery must not revoke or cancel it.
func TestRegistryFenceThroughLeavesNewerRegisteredGenerationCurrent(t *testing.T) {
	registry := NewRegistry()
	prior, err := registry.Open(context.Background(), testAgentID, 7)
	if err != nil {
		t.Fatalf("open prior session: %v", err)
	}
	current, err := registry.Open(context.Background(), testAgentID, 8)
	if err != nil {
		t.Fatalf("open current session: %v", err)
	}
	defer current.Close()
	if err := fenceRegistryThrough(context.Background(), registry, testAgentID, 7); err != nil {
		t.Fatalf("FenceThrough() error = %v", err)
	}
	if !current.AssignmentsAllowed() {
		t.Fatal("prior-generation fence stopped the current session")
	}
	select {
	case <-current.Done():
		t.Fatal("prior-generation fence canceled the current session")
	default:
	}
	prior.Close()
}

// Rationale: stale lifecycle work must never stop, revoke, or wait on a newer
// replacement generation that reused the same Agent id.
func TestRegistryLifecycleOperationsFenceGenerationMismatch(t *testing.T) {
	registry := NewRegistry()
	session, err := registry.Open(context.Background(), testAgentID, 8)
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	defer session.Close()

	assertStateConflict(t, registry.StopAssignments(context.Background(), testAgentID, 7))
	assertStateConflict(t, registry.Revoke(context.Background(), testAgentID, 7))
	assertStateConflict(t, registry.WaitOffline(context.Background(), testAgentID, 7))
	if !session.AssignmentsAllowed() {
		t.Fatal("stale lifecycle operation fenced the replacement")
	}
	select {
	case <-session.Done():
		t.Fatal("stale lifecycle operation canceled the replacement")
	default:
	}
}

// Rationale: deletion retries are allowed after disconnect or after the
// in-memory session disappeared, and each matching operation must be a no-op.
func TestRegistryLifecycleOperationsAreIdempotentWhenAbsentOrOffline(t *testing.T) {
	registry := NewRegistry()
	for _, operation := range []struct {
		name string
		run  func() error
	}{
		{name: "stop absent", run: func() error {
			return registry.StopAssignments(context.Background(), testAgentID, 7)
		}},
		{name: "revoke absent", run: func() error {
			return registry.Revoke(context.Background(), testAgentID, 7)
		}},
		{name: "wait absent", run: func() error {
			return registry.WaitOffline(context.Background(), testAgentID, 7)
		}},
	} {
		if err := operation.run(); err != nil {
			t.Fatalf("%s: %v", operation.name, err)
		}
	}
	_, err := registry.Open(context.Background(), testAgentID, 7)
	assertStateConflict(t, err)

	session, err := registry.Open(context.Background(), testOtherAgentID, 7)
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	session.Close()
	if err := registry.StopAssignments(context.Background(), testOtherAgentID, 7); err != nil {
		t.Fatalf("stop offline: %v", err)
	}
	if err := registry.Revoke(context.Background(), testOtherAgentID, 7); err != nil {
		t.Fatalf("revoke offline: %v", err)
	}
	if err := registry.WaitOffline(context.Background(), testOtherAgentID, 7); err != nil {
		t.Fatalf("wait offline: %v", err)
	}
}

// Rationale: quiescence belongs to an Agent generation, not one connection;
// replacing a live same-generation stream must not silently reopen assignment.
func TestRegistrySameGenerationReconnectRemainsStoppedAfterLiveQuiescence(t *testing.T) {
	registry := NewRegistry()
	first, err := registry.Open(context.Background(), testAgentID, 7)
	if err != nil {
		t.Fatalf("open first session: %v", err)
	}
	if err := registry.StopAssignments(context.Background(), testAgentID, 7); err != nil {
		t.Fatalf("stop assignments: %v", err)
	}
	replacement, err := registry.Open(context.Background(), testAgentID, 7)
	if err != nil {
		t.Fatalf("open replacement: %v", err)
	}
	defer replacement.Close()
	select {
	case <-first.Done():
	default:
		t.Fatal("same-generation replacement did not fence the old stream")
	}
	if replacement.AssignmentsAllowed() {
		t.Fatal("same-generation replacement reopened assignments")
	}
	first.Close()
}

// Rationale: quiescence recorded before connection or after disconnect must
// still fence a later same-generation stream during resumable deletion.
func TestRegistrySameGenerationReconnectRemainsStoppedWhenAbsentOrOffline(t *testing.T) {
	for _, test := range []struct {
		name    string
		prepare func(*testing.T, *Registry)
	}{
		{
			name: "absent",
			prepare: func(t *testing.T, registry *Registry) {
				t.Helper()
				if err := registry.StopAssignments(context.Background(), testAgentID, 7); err != nil {
					t.Fatalf("stop absent assignments: %v", err)
				}
			},
		},
		{
			name: "offline",
			prepare: func(t *testing.T, registry *Registry) {
				t.Helper()
				session, err := registry.Open(context.Background(), testAgentID, 7)
				if err != nil {
					t.Fatalf("open initial session: %v", err)
				}
				session.Close()
				if err := registry.StopAssignments(context.Background(), testAgentID, 7); err != nil {
					t.Fatalf("stop offline assignments: %v", err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := NewRegistry()
			test.prepare(t, registry)
			session, err := registry.Open(context.Background(), testAgentID, 7)
			if err != nil {
				t.Fatalf("open same-generation session: %v", err)
			}
			defer session.Close()
			if session.AssignmentsAllowed() {
				t.Fatal("same-generation session reopened assignments")
			}
		})
	}
}

// Rationale: removal can revoke while authentication is paused before Open;
// the later Open must observe durable-policy fencing despite no live session.
func TestRegistryRevokeWhileAbsentRejectsPausedSameGenerationOpen(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Revoke(context.Background(), testAgentID, 7); err != nil {
		t.Fatalf("revoke absent generation: %v", err)
	}
	_, err := registry.Open(context.Background(), testAgentID, 6)
	assertStateConflict(t, err)
	_, err = registry.Open(context.Background(), testAgentID, 7)
	assertStateConflict(t, err)
	replacement, err := registry.Open(context.Background(), testAgentID, 8)
	if err != nil {
		t.Fatalf("open newer generation: %v", err)
	}
	if !replacement.AssignmentsAllowed() {
		t.Fatal("newer unfenced generation inherited stale lifecycle policy")
	}
	replacement.Close()
}

// Rationale: invalid Open inputs must fail before a newer attempt can cancel
// or otherwise mutate the current authenticated session.
func TestRegistryOpenRejectsInvalidInputsWithoutFencingCurrentSession(t *testing.T) {
	registry := NewRegistry()
	current, err := registry.Open(context.Background(), testAgentID, 7)
	if err != nil {
		t.Fatalf("open current session: %v", err)
	}
	defer current.Close()

	//lint:ignore SA1012 This test verifies rejection without fencing the active session.
	if _, err := registry.Open(nil, testAgentID, 8); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("nil-context Open() error = %v, want internal", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := registry.Open(canceled, testAgentID, 8); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled-context Open() error = %v, want context.Canceled", err)
	}
	if _, err := registry.Open(context.Background(), testAgentID, 0); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("zero-generation Open() error = %v, want validation.failed", err)
	}
	select {
	case <-current.Done():
		t.Fatal("invalid Open input fenced the current session")
	default:
	}
	if err := current.RecordReady(testTime(), 1, "v0.4.2"); err != nil {
		t.Fatalf("current session lost authority: %v", err)
	}
}

// Rationale: a parent canceled while Open waits for the registry lock is
// already canceled at the mutation boundary and must not fence the live session.
func TestRegistryOpenRejectsParentCanceledWhileWaitingForMutation(t *testing.T) {
	registry := NewRegistry()
	current, err := registry.Open(context.Background(), testAgentID, 7)
	if err != nil {
		t.Fatalf("open current session: %v", err)
	}
	defer current.Close()

	parent, cancel := context.WithCancel(context.Background())
	checked := make(chan struct{})
	observed := &observedErrorContext{Context: parent, checked: checked}
	registry.mu.Lock()
	result := make(chan error, 1)
	go func() {
		_, openErr := registry.Open(observed, testAgentID, 8)
		result <- openErr
	}()
	<-checked
	cancel()
	registry.mu.Unlock()

	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Open() error = %v, want context.Canceled", err)
	}
	select {
	case <-current.Done():
		t.Fatal("canceled Open fenced the current session")
	default:
	}
}

// Rationale: revoking one generation must fence its stream idempotently while
// still allowing a separately authenticated newer generation to replace it.
func TestRegistryRevokeFencesOnlyMatchingGeneration(t *testing.T) {
	registry := NewRegistry()
	session, err := registry.Open(context.Background(), testAgentID, 7)
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	if err := registry.StopAssignments(context.Background(), testAgentID, 7); err != nil {
		t.Fatalf("stop assignments: %v", err)
	}
	if err := registry.Revoke(context.Background(), testAgentID, 7); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if err := registry.Revoke(context.Background(), testAgentID, 7); err != nil {
		t.Fatalf("repeat revoke: %v", err)
	}
	select {
	case <-session.Done():
	default:
		t.Fatal("matching revocation did not cancel the session")
	}
	session.Close()

	replacement, err := registry.Open(context.Background(), testAgentID, 8)
	if err != nil {
		t.Fatalf("open replacement: %v", err)
	}
	replacement.Close()
}

func fenceRegistryThrough(
	ctx context.Context,
	registry *Registry,
	agentID string,
	generation uint64,
) error {
	fencer, ok := any(registry).(interface {
		FenceThrough(context.Context, string, uint64) error
	})
	if !ok {
		return errors.New("registry does not expose a prior-generation fence")
	}
	return fencer.FenceThrough(ctx, agentID, generation)
}

func assertStateConflict(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("expected state conflict, got %v", err)
	}
	var domainError *errs.Error
	if !errors.As(err, &domainError) {
		t.Fatalf("expected domain error, got %T", err)
	}
	if got := domainError.HTTPStatus(); got != 409 {
		t.Fatalf("expected status 409, got %d", got)
	}
}

type observedErrorContext struct {
	context.Context
	checked chan struct{}
	once    sync.Once
}

func (ctx *observedErrorContext) Err() error {
	err := ctx.Context.Err()
	ctx.once.Do(func() { close(ctx.checked) })
	return err
}
